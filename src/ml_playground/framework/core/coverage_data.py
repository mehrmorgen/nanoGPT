"""Shared coverage-data normalization utilities."""

from __future__ import annotations

from collections.abc import Mapping
from typing import cast


def _to_float(value: object | None) -> float:
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        try:
            return float(value)
        except ValueError:
            return 0.0
    return 0.0


def extract_coverage_totals(coverage_data: Mapping[str, object]) -> dict[str, float]:
    """Extract coverage totals from either top-level totals or per-file summaries."""
    files_section = coverage_data.get("files")
    if isinstance(files_section, Mapping):
        covered_lines_from_files = 0.0
        missing_lines_from_files = 0.0
        covered_branches_from_files = 0.0
        missing_branches_from_files = 0.0
        for raw_info in files_section.values():
            if not isinstance(raw_info, Mapping):
                continue
            info = cast(Mapping[str, object], raw_info)
            summary = info.get("summary")
            if not isinstance(summary, Mapping):
                continue
            summary_map = cast(Mapping[str, object], summary)
            covered_lines_from_files += _to_float(summary_map.get("covered_lines"))
            missing_lines_from_files += _to_float(summary_map.get("missing_lines"))
            covered_branches_from_files += _to_float(
                summary_map.get("covered_branches")
            )
            missing_branches_from_files += _to_float(
                summary_map.get("missing_branches")
            )

        num_statements_from_files = covered_lines_from_files + missing_lines_from_files
        num_branches_from_files = (
            covered_branches_from_files + missing_branches_from_files
        )
        if num_statements_from_files > 0:
            return {
                "num_branches": num_branches_from_files,
                "covered_branches": covered_branches_from_files,
                "missing_branches": missing_branches_from_files,
                "covered_lines": covered_lines_from_files,
                "missing_lines": missing_lines_from_files,
                "num_statements": num_statements_from_files,
            }

    totals_section = coverage_data.get("totals")
    if not isinstance(totals_section, Mapping):
        totals_section = coverage_data.get("summary")
    normalized_totals: Mapping[str, object]
    if isinstance(totals_section, Mapping):
        normalized_totals = cast(Mapping[str, object], totals_section)
    else:
        normalized_totals = {}

    covered_lines = _to_float(normalized_totals.get("covered_lines"))
    missing_lines = _to_float(normalized_totals.get("missing_lines"))
    num_statements = _to_float(normalized_totals.get("num_statements"))
    if num_statements <= 0:
        num_statements = covered_lines + missing_lines

    covered_branches = _to_float(normalized_totals.get("covered_branches"))
    missing_branches = _to_float(normalized_totals.get("missing_branches"))
    num_branches = _to_float(normalized_totals.get("num_branches"))
    if num_branches <= 0:
        num_branches = covered_branches + missing_branches

    totals = {
        "num_branches": num_branches,
        "covered_branches": covered_branches,
        "missing_branches": missing_branches,
        "covered_lines": covered_lines,
        "missing_lines": missing_lines,
        "num_statements": num_statements,
    }
    percent = _to_float(normalized_totals.get("percent_covered"))
    if percent > 0:
        totals["percent_covered"] = percent
    return totals
