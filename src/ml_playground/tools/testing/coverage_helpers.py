"""Helper utilities for testing coverage processing and formatting.

These functions are intentionally stateless and reusable across the
TestingTools facade, keeping logic focused and testable.
"""

from __future__ import annotations

import hashlib
import json
import re
from pathlib import Path
from typing import Any, List


# Precompiled pytest progress regex used to clean noisy output
_PYTEST_PROGRESS_RE = re.compile(r"^[\.s]+(?:\s+\[\s*\d+%])?$")


def compute_coverage_fingerprint(root_path: Path) -> str:
    """Compute a fingerprint over repo files relevant for coverage.

    The hash includes file path, size, mtime_ns and content digest for
    all Python files under src/ml_playground/tools and tests/.
    """
    include_paths = [
        root_path / "src" / "ml_playground" / "tools",
        root_path / "tests",
    ]

    parts: list[str] = []
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


def read_coverage_manifest(manifest_path: Path) -> dict[str, Any] | None:
    """Load the stored coverage fingerprint manifest if present."""
    if not manifest_path.exists():
        return None
    try:
        with manifest_path.open(encoding="utf-8") as manifest_file:
            return json.load(manifest_file)
    except (OSError, json.JSONDecodeError):
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


def collect_undercovered_files(
    coverage_data: dict[str, Any],
) -> list[tuple[str, float, float | None]]:
    """Return files with <100% line coverage and optional branch %.

    Returns list of tuples (path, line_pct, branch_pct_or_None), sorted by
    (line_pct asc, path asc).
    """
    files = coverage_data.get("files", {})
    undercovered: list[tuple[str, float, float | None]] = []
    for path, info in files.items():
        summary = info.get("summary", {})
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
        num_branches = summary.get("num_branches")
        covered_branches = summary.get("covered_branches")
        if isinstance(num_branches, (int, float)) and num_branches:
            try:
                branch_percent = float(covered_branches) / float(num_branches) * 100
            except (TypeError, ZeroDivisionError):
                branch_percent = None
        if percent_float < 100.0:
            undercovered.append((path, percent_float, branch_percent))
    undercovered.sort(key=lambda item: (item[1], item[0]))
    return undercovered


def format_undercovered_tree(
    entries: list[tuple[str, float, float | None]],
) -> list[str]:
    """Render a tree view for undercovered files."""
    root: dict[str, Any] = {}

    for path, line_pct, branch_pct in entries:
        # Faster than Path(path).parts for small strings
        parts = path.split("/")
        node = root
        for part in parts[:-1]:
            node = node.setdefault(part, {})  # type: ignore[assignment]
        node.setdefault("__files__", []).append((parts[-1], line_pct, branch_pct))

    def render(node: dict[str, Any], prefix: str) -> list[str]:
        lines: list[str] = []
        dir_names = sorted(name for name in node.keys() if name != "__files__")
        files = sorted(node.get("__files__", []), key=lambda item: item[0])

        total = len(dir_names) + len(files)
        idx = 0

        for name in dir_names:
            is_last = (
                idx == total - 1 if len(files) == 0 and name == dir_names[-1] else False
            )
            connector = "└── " if is_last else "├── "
            lines.append(f"{prefix}{connector}{name}/")
            child_prefix = prefix + ("    " if is_last else "│   ")
            lines.extend(render(node[name], child_prefix))
            idx += 1

        for file_info in files:
            is_last = idx == total - 1
            connector = "└── " if is_last else "├── "
            name, line_pct, branch_pct = file_info
            branch_text = (
                f" branch = {branch_pct:.2f}%" if branch_pct is not None else ""
            )
            lines.append(
                f"{prefix}{connector}{name}: line = {line_pct:.2f}%{branch_text}"
            )
            idx += 1

        return lines

    return render(root, "")


def format_tool_invocation(tool: str, args: List[str]) -> str:
    suffix = f" {' '.join(args)}" if args else ""
    return f"Executed: uv run tools test {tool}{suffix}"


def format_command(command: list[str]) -> str:
    return "Executed: " + " ".join(command)
