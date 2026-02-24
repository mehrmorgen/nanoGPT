from __future__ import annotations

import json
from pathlib import Path
from typing import Mapping, cast, Union, Optional, Literal

from ml_playground.framework.core.di_implementations import DefaultJsonParser
from ml_playground.tools.core.errors import ToolExecutionError


class CoverageService:
    """Service for handling coverage data collection and reporting."""

    def __init__(self, root_path: Path) -> None:
        self.root_path = root_path
        self.json_parser = DefaultJsonParser()

    def collect_metrics(
        self,
        json_path: Path,
    ) -> list[str]:
        """Parse coverage JSON and return formatted metric lines."""
        if not json_path.exists():
            raise ToolExecutionError(
                "Coverage JSON data not found",
                reason=f"Coverage JSON file missing: {json_path}",
                rationale="Coverage metrics require JSON report generation",
            )

        try:
            content = json_path.read_text(encoding="utf-8")
            raw_json_data = self.json_parser.parse_json(content)
            coverage_data: Mapping[str, object] = raw_json_data
            totals = cast(Mapping[str, object], coverage_data["totals"])
        except (json.JSONDecodeError, KeyError) as exc:
            raise ToolExecutionError(
                "Failed to parse coverage JSON for summary",
                reason=str(exc),
                rationale="Coverage JSON must contain totals for reporting metrics",
            ) from exc

        statements_obj = totals.get("num_statements", 0)
        covered_lines_obj = totals.get("covered_lines", 0)
        missing_lines_obj = totals.get("missing_lines", 0)
        num_branches_obj = totals.get("num_branches", 0)
        covered_branches_obj = totals.get("covered_branches", 0)
        missing_branches_obj = totals.get("missing_branches", 0)

        statements = (
            int(statements_obj) if isinstance(statements_obj, (int, float)) else 0
        )
        covered_lines = (
            int(covered_lines_obj) if isinstance(covered_lines_obj, (int, float)) else 0
        )
        missing_lines = (
            int(missing_lines_obj) if isinstance(missing_lines_obj, (int, float)) else 0
        )
        if statements <= 0:
            statements = covered_lines + missing_lines

        num_branches = (
            int(num_branches_obj) if isinstance(num_branches_obj, (int, float)) else 0
        )
        covered_branches = (
            int(covered_branches_obj)
            if isinstance(covered_branches_obj, (int, float))
            else 0
        )
        missing_branches = (
            int(missing_branches_obj)
            if isinstance(missing_branches_obj, (int, float))
            else 0
        )
        if num_branches <= 0:
            num_branches = covered_branches + missing_branches

        line_pct = (covered_lines / statements * 100) if statements else 0.0
        branch_pct = (covered_branches / num_branches * 100) if num_branches else 0.0

        metrics_lines = [
            f"Coverage totals: lines={line_pct:.2f}% ({covered_lines}/{statements}), loc={statements}",
        ]
        if num_branches:
            metrics_lines.append(
                f"Branch totals: branches={branch_pct:.2f}% ({covered_branches}/{num_branches})"
            )
        else:
            metrics_lines.append(
                "Branch totals: not available (no branch data collected)"
            )

        return metrics_lines

    def get_undercovered_files(
        self, coverage_data: Mapping[str, object]
    ) -> list[tuple[str, float, float | None, int]]:
        """Identify files with less than 100% coverage."""
        files = cast(Mapping[str, Mapping[str, object]], coverage_data.get("files", {}))
        undercovered: list[tuple[str, float, float | None, int]] = []
        for path, info in files.items():
            summary = cast(Mapping[str, object], info.get("summary", {}))
            percent = cast(Optional[float], summary.get("percent_covered"))
            if percent is None:
                display = summary.get("percent_covered_display")
                if isinstance(display, str):
                    try:
                        percent = float(display.rstrip("%"))
                    except ValueError:
                        percent = 0.0
                else:
                    percent = 0.0
            percent_float = float(percent)
            loc = 0
            num_statements = summary.get("num_statements")
            if isinstance(num_statements, (int, float)):
                loc = int(num_statements)
            else:
                covered_lines = summary.get("covered_lines")
                missing_lines = summary.get("missing_lines")
                if isinstance(covered_lines, (int, float)) and isinstance(
                    missing_lines, (int, float)
                ):
                    loc = int(float(covered_lines) + float(missing_lines))
            branch_percent: float | None = None
            num_branches = summary.get("num_branches")
            covered_branches = summary.get("covered_branches")
            missing_branches = summary.get("missing_branches")
            if (
                (not isinstance(num_branches, (int, float)) or float(num_branches) <= 0)
                and isinstance(covered_branches, (int, float))
                and isinstance(missing_branches, (int, float))
            ):
                num_branches = float(covered_branches) + float(missing_branches)
            if isinstance(num_branches, (int, float)) and num_branches:
                try:
                    covered_branches_float = float(
                        cast(Union[str, float, int], covered_branches)
                    )
                    branch_percent = covered_branches_float / float(num_branches) * 100
                except (TypeError, ValueError, ZeroDivisionError):
                    branch_percent = None
            if percent_float < 100.0:
                undercovered.append((path, percent_float, branch_percent, loc))
        undercovered.sort(key=lambda item: (item[1], item[0]))
        return undercovered

    def render_undercovered_tree(
        self,
        entries: list[
            tuple[str, float, float | None] | tuple[str, float, float | None, int]
        ],
    ) -> list[str]:
        """Render a tree view of files with coverage gaps."""
        root: dict[str, object] = {}

        for entry in entries:
            path, line_pct, branch_pct = entry[:3]
            loc = entry[3] if len(entry) > 3 else 0
            parts = Path(path).parts
            node = root
            for part in parts[:-1]:
                if part not in node:
                    node[part] = {}
                node = cast(dict[str, object], node[part])

            if "__files__" not in node:
                node["__files__"] = []
            files_list = cast(
                list[tuple[str, float, float | None, int]],
                node["__files__"],
            )
            files_list.append((parts[-1], line_pct, branch_pct, loc))

        def _render_node(node: Mapping[str, object], prefix: str) -> list[str]:
            lines: list[str] = []
            dir_names = sorted(name for name in node.keys() if name != "__files__")

            raw_files = node.get("__files__", [])
            files = sorted(
                cast(list[tuple[str, float, float | None, int]], raw_files),
                key=lambda item: item[0],
            )

            combined: list[tuple[str, Literal["dir", "file"], object]] = [
                (name, "dir", node[name]) for name in dir_names
            ]
            combined.extend(
                (file_info[0], "file", cast(object, file_info)) for file_info in files
            )

            for idx, (name, kind, payload) in enumerate(combined):
                is_last = idx == len(combined) - 1
                connector = "└── " if is_last else "├── "
                if kind == "dir":
                    lines.append(f"{prefix}{connector}{name}/")
                    child_prefix = prefix + ("    " if is_last else "│   ")
                    lines.extend(
                        _render_node(cast(Mapping[str, object], payload), child_prefix)
                    )
                else:
                    file_payload = cast(tuple[str, float, float | None, int], payload)
                    _, line_pct, branch_pct, loc = file_payload
                    branch_text = (
                        f" branch = {branch_pct:.2f}%" if branch_pct is not None else ""
                    )
                    lines.append(
                        f"{prefix}{connector}{name}: line = {line_pct:.2f}%{branch_text} loc = {loc}"
                    )

            return lines

        return _render_node(root, "")
