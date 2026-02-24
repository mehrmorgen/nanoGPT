"""Helper utilities for testing coverage processing and formatting.

These functions are intentionally stateless and reusable across the
TestingTools facade, keeping logic focused and testable.
"""

from __future__ import annotations

import hashlib
import json
import re
import shlex
from collections.abc import Mapping
from pathlib import Path
from typing import Any, Sequence, cast
from typing_extensions import TypedDict


# Precompiled pytest progress regex used to clean noisy output
_PYTEST_PROGRESS_RE = re.compile(r"^[\.s]+(?:\s+\[\s*\d+%])?$")


def compute_coverage_fingerprint(root_path: Path) -> str:
    """Compute a fingerprint over repo files relevant for coverage.

    The hash includes file path, size, mtime_ns and content digest for
    all Python files under src/ml_playground/{framework,tools,experiments,runtime}
    and tests/, plus pyproject.toml.
    """
    pyproject_path = root_path / "pyproject.toml"
    include_paths = [
        root_path / "src" / "ml_playground" / "framework",
        root_path / "src" / "ml_playground" / "tools",
        root_path / "src" / "ml_playground" / "experiments",
        root_path / "src" / "ml_playground" / "runtime",
        root_path / "tests",
    ]

    parts: list[str] = []
    if pyproject_path.exists():
        try:
            stat_result = pyproject_path.stat()
            hasher = hashlib.sha256()
            with pyproject_path.open("rb") as handle:
                for chunk in iter(lambda: handle.read(8192), b""):
                    hasher.update(chunk)
            parts.append(
                f"pyproject.toml:{stat_result.st_size}:{stat_result.st_mtime_ns}:{hasher.hexdigest()}"
            )
        except OSError:
            pass

    for base_path in include_paths:
        if not base_path.exists():
            continue
        for file_path in sorted(base_path.rglob("*.py")):
            try:
                stat_result = file_path.stat()
                hasher = hashlib.sha256()
                with file_path.open("rb") as handle:
                    for chunk in iter(lambda: handle.read(8192), b""):
                        hasher.update(chunk)
            except OSError:
                continue
            relative = file_path.relative_to(root_path).as_posix()
            parts.append(
                f"{relative}:{stat_result.st_size}:{stat_result.st_mtime_ns}:{hasher.hexdigest()}"
            )

    payload = "\n".join(parts)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def read_coverage_manifest(manifest_path: Path) -> dict[str, str] | None:
    """Load the stored coverage fingerprint manifest if present."""
    if not manifest_path.exists():
        return None
    try:
        with manifest_path.open(encoding="utf-8") as manifest_file:
            raw_mapping: object = cast(object, json.load(manifest_file))
    except (OSError, json.JSONDecodeError):
        return None
    if not isinstance(raw_mapping, Mapping):
        return None
    typed_mapping = cast(Mapping[str, object], raw_mapping)
    fingerprint = typed_mapping.get("fingerprint")
    if isinstance(fingerprint, str):
        return {"fingerprint": fingerprint}
    return None


def write_coverage_manifest(manifest_path: Path, *, fingerprint: str) -> None:
    """Write the coverage fingerprint manifest to disk."""
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    payload = {"fingerprint": fingerprint}
    with manifest_path.open("w", encoding="utf-8") as manifest_file:
        json.dump(payload, manifest_file)


def clean_pytest_output(output: str) -> str:
    """Remove pytest progress lines and xdist status messages from stdout."""
    cleaned_lines: list[str] = []
    for line in output.splitlines():
        stripped = line.strip()
        if not stripped:
            continue
        if stripped.startswith("bringing up nodes"):
            continue
        if _PYTEST_PROGRESS_RE.fullmatch(stripped):
            continue
        cleaned_lines.append(line)
    return "\n".join(cleaned_lines)


class _CoverageSummary(TypedDict, total=False):
    percent_covered: float | None
    percent_covered_display: str | None
    num_branches: int | float | None
    covered_branches: int | float | None


class _CoverageFileInfo(TypedDict, total=False):
    summary: _CoverageSummary


def collect_undercovered_files(
    coverage_data: Mapping[str, object],
) -> list[tuple[str, float, float | None, int]]:
    """Return files with <100% line coverage and optional branch %.

    Returns list of tuples (path, line_pct, branch_pct_or_None, loc), sorted by
    (line_pct asc, path asc).
    """
    files_section = coverage_data.get("files")
    if not isinstance(files_section, Mapping):
        return []

    files = cast(Mapping[str, _CoverageFileInfo], files_section)
    undercovered: list[tuple[str, float, float | None, int]] = []
    for path, info in files.items():
        summary_section = info.get("summary")
        if not isinstance(summary_section, Mapping):
            continue
        summary = cast(Mapping[str, Any], summary_section)
        percent = summary.get("percent_covered")
        if percent is None:
            display = summary.get("percent_covered_display")
            if isinstance(display, str):
                try:
                    percent = float(display)
                except ValueError:
                    continue
        if percent is None:
            continue
        percent_float = float(percent)
        branch_percent: float | None = None
        loc = 0
        statements_value = summary.get("num_statements")
        if isinstance(statements_value, (int, float)):
            loc = int(statements_value)
        else:
            covered_lines = summary.get("covered_lines")
            missing_lines = summary.get("missing_lines")
            if isinstance(covered_lines, (int, float)) and isinstance(
                missing_lines, (int, float)
            ):
                loc = int(float(covered_lines) + float(missing_lines))
        num_branches_value = summary.get("num_branches")
        covered_branches = summary.get("covered_branches")
        missing_branches = summary.get("missing_branches")
        if (
            (
                not isinstance(num_branches_value, (int, float))
                or num_branches_value <= 0
            )
            and isinstance(covered_branches, (int, float))
            and isinstance(missing_branches, (int, float))
        ):
            num_branches_value = float(covered_branches) + float(missing_branches)
        if (
            isinstance(num_branches_value, (int, float))
            and num_branches_value
            and isinstance(covered_branches, (int, float))
        ):
            try:
                branch_percent = (
                    float(covered_branches) / float(num_branches_value) * 100
                )
            except (TypeError, ZeroDivisionError):
                branch_percent = None
        if percent_float < 100.0:
            undercovered.append((path, percent_float, branch_percent, loc))
    undercovered.sort(key=lambda item: (item[1], item[0]))
    return undercovered


def format_undercovered_tree(entries: Sequence[_CoverageEntry]) -> list[str]:
    """Render a tree view for undercovered files."""

    class _TreeNode:
        __slots__ = ("children", "files")

        def __init__(self) -> None:
            self.children: dict[str, "_TreeNode"] = {}
            self.files: list[tuple[str, float, float | None, int]] = []

        def add_file(
            self, parts: Sequence[str], file_info: tuple[str, float, float | None, int]
        ) -> None:
            if not parts:
                self.files.append(file_info)
                return
            first, *rest = parts
            child = self.children.setdefault(first, _TreeNode())
            child.add_file(rest, file_info)

    def _render(node: _TreeNode, prefix: str) -> list[str]:
        lines: list[str] = []
        dir_names = sorted(node.children.keys())
        total_entries = len(dir_names) + len(node.files)
        idx = 0

        for name in dir_names:
            is_last = idx == total_entries - 1 and not node.files
            connector = "└── " if is_last else "├── "
            lines.append(f"{prefix}{connector}{name}/")
            child_prefix = prefix + ("    " if is_last else "│   ")
            lines.extend(_render(node.children[name], child_prefix))
            idx += 1

        sorted_files = sorted(node.files, key=lambda item: item[0])
        for file_name, line_pct, branch_pct, loc in sorted_files:
            is_last = idx == total_entries - 1
            connector = "└── " if is_last else "├── "
            branch_text = (
                f" branch = {branch_pct:.2f}%" if branch_pct is not None else ""
            )
            lines.append(
                f"{prefix}{connector}{file_name}: line = {line_pct:.2f}%{branch_text} loc = {loc}"
            )
            idx += 1

        return lines

    root = _TreeNode()
    for entry in entries:
        path, line_pct, branch_pct = entry[:3]
        loc = entry[3] if len(entry) > 3 else 0
        parts = path.split("/")
        root.add_file(parts[:-1], (parts[-1], line_pct, branch_pct, loc))

    return _render(root, "")


def normalize_coverage_path(path: str, root_path: Path) -> str:
    """Normalize a coverage path to a repo-relative POSIX string when possible."""
    if not path:
        return path
    try:
        resolved = Path(path).resolve()
        relative = resolved.relative_to(root_path.resolve())
        return relative.as_posix()
    except (OSError, ValueError):
        return Path(path).as_posix()


def expected_suite_for_path(path: str) -> str:
    """Return suite hints for a given source path."""
    normalized = path.replace("\\", "/")
    if "src/ml_playground/runtime_cli/" in normalized:
        return "tests/unit/runtime/cli + tests/property/runtime/cli"
    if "src/ml_playground/framework/runtime/" in normalized:
        return "tests/unit/runtime + tests/property/runtime"
    if "src/ml_playground/runtime/" in normalized:
        return "tests/unit/runtime + tests/property/runtime"
    if "src/ml_playground/framework/" in normalized:
        return "tests/unit + tests/property"
    if "src/ml_playground/tools/" in normalized:
        return "tests/unit/tools"
    if "src/ml_playground/experiments/" in normalized:
        return "tests/unit/experiments + tests/integration"
    return "tests/unit"


def format_coverage_map(
    entries: Sequence[_CoverageEntry], root_path: Path
) -> list[str]:
    """Format coverage gaps with suite hints."""
    lines: list[str] = []
    for entry in entries:
        path, line_pct, branch_pct = entry[:3]
        loc = entry[3] if len(entry) > 3 else 0
        display = normalize_coverage_path(path, root_path)
        suite_hint = expected_suite_for_path(display)
        branch_text = f", branch={branch_pct:.2f}%" if branch_pct is not None else ""
        lines.append(
            f"- {display}: line={line_pct:.2f}%{branch_text}, loc={loc} expected={suite_hint}"
        )
    return lines


def format_tool_invocation(
    tool: str, args: Sequence[str], *, prefix: str = "uv run"
) -> str:
    suffix = f" {' '.join(args)}" if args else ""
    if prefix:
        return f"Executed: {prefix} tools test {tool}{suffix}"
    return f"Executed: tools test {tool}{suffix}"


def format_command(command: list[str], *, prefix: str | None = "uv run") -> str:
    """Format a command for display purposes."""
    return " ".join(shlex.quote(arg) for arg in command)


def format_coverage_status(
    *,
    metric: str,
    percentage: float,
    threshold: float,
    passed: bool,
) -> str:
    """Format a consistent coverage status line for reporting."""

    icon = "✅" if passed else "❌"
    label = "SUCCESS" if passed else "FAILURE"
    comparator = ">=" if passed else "<"
    return (
        f"[coverage] {icon} {label}: {metric} coverage "
        f"{percentage:.2f}% {comparator} {threshold:.2f}%."
    )


_CoverageEntry3 = tuple[str, float, float | None]
_CoverageEntry4 = tuple[str, float, float | None, int]
_CoverageEntry = _CoverageEntry3 | _CoverageEntry4
