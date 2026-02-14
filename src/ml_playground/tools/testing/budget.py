"""Test budget reporting utilities for integration and E2E suites."""

from __future__ import annotations

import json
import re
import tomllib
from pathlib import Path
from typing import List, Mapping, cast

from ..core.config import ToolsConfig
from ..core.interfaces import OperationId, ToolResult
from ..utils.subprocess_utils import SubprocessRunner


_BUDGET_SECTION = ("tool", "ml_playground", "testing", "budgets")


def _read_test_budgets_from_config(root_path: Path) -> dict[str, float]:
    pyproject_path = root_path / "pyproject.toml"
    if not pyproject_path.exists():
        return {}
    try:
        with pyproject_path.open("rb") as handle:
            data: Mapping[str, object] = tomllib.load(handle)
    except (OSError, tomllib.TOMLDecodeError):
        return {}

    section = data
    for key in _BUDGET_SECTION:
        value = section.get(key)
        if not isinstance(value, Mapping):
            return {}
        section = cast(Mapping[str, object], value)

    budgets: dict[str, float] = {}
    for name in ("integration_seconds", "e2e_seconds"):
        value = section.get(name)
        if isinstance(value, (int, float)):
            budgets[name] = float(value)
    return budgets


def _budget_cache_path(root_path: Path) -> Path:
    return root_path / ".cache" / "test_budgets.json"


def _read_budget_cache(root_path: Path) -> dict[str, float]:
    cache_path = _budget_cache_path(root_path)
    if not cache_path.exists():
        return {}
    try:
        payload = cast(object, json.loads(cache_path.read_text(encoding="utf-8")))
    except (OSError, json.JSONDecodeError):
        return {}
    if not isinstance(payload, Mapping):
        return {}
    payload = cast(Mapping[str, object], payload)
    cached: dict[str, float] = {}
    for key in ("integration_seconds", "e2e_seconds"):
        value = payload.get(key)
        if isinstance(value, (int, float)):
            cached[key] = float(value)
    return cached


def _write_budget_cache(root_path: Path, payload: Mapping[str, float]) -> None:
    cache_path = _budget_cache_path(root_path)
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    with cache_path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle)


def _parse_pytest_runtime(output: str) -> float | None:
    for line in reversed(output.splitlines()):
        match = re.search(r" in ([0-9]+\.[0-9]+|[0-9]+)s", line)
        if match:
            try:
                return float(match.group(1))
            except ValueError:
                return None
    return None


def _run_suite(
    *,
    suite_path: str,
    root_path: Path,
    subprocess_runner: SubprocessRunner,
    operation_id: OperationId,
    timeout: int,
) -> tuple[ToolResult, float | None]:
    result = subprocess_runner.run_uv_command(
        [
            "pytest",
            "-q",
            "--durations=0",
            "--durations-min=0",
            suite_path,
        ],
        cwd=root_path,
        timeout=timeout,
        operation_id=operation_id,
    )
    duration = _parse_pytest_runtime(result.stdout or "")
    return result, duration


def run_test_budget_report(
    config: ToolsConfig,
    root_path: Path,
    args: List[str],
    subprocess_runner: SubprocessRunner,
    *,
    refresh: bool = False,
) -> ToolResult:
    operation_id = OperationId(namespace="tools", category="test", command="budget")
    _ = args

    budgets = _read_test_budgets_from_config(root_path)
    cached = _read_budget_cache(root_path)

    if refresh:
        refreshed: dict[str, float] = {}
        for key, suite in (
            ("integration_seconds", "tests/integration"),
            ("e2e_seconds", "tests/e2e"),
        ):
            result, duration = _run_suite(
                suite_path=suite,
                root_path=root_path,
                subprocess_runner=subprocess_runner,
                operation_id=operation_id,
                timeout=config.testing.timeout,
            )
            if not result.success:
                return ToolResult(
                    success=False,
                    exit_code=result.exit_code,
                    stdout=result.stdout,
                    stderr=result.stderr,
                    operation_id=operation_id,
                )
            if duration is not None:
                refreshed[key] = duration
        if refreshed:
            cached.update(refreshed)
            _write_budget_cache(root_path, cached)

    lines: list[str] = ["Test budget report:"]
    for key, label in (
        ("integration_seconds", "integration"),
        ("e2e_seconds", "e2e"),
    ):
        budget_value = budgets.get(key)
        last_value = cached.get(key)
        if budget_value is None:
            status = "budget=unset"
        else:
            status = f"budget={budget_value:.2f}s"
        if last_value is None:
            status = f"{status}, last=unknown"
            if refresh is False:
                status = f"{status} (run with --refresh)"
        else:
            within = budget_value is None or last_value <= budget_value
            verdict = "ok" if within else "over"
            status = f"{status}, last={last_value:.2f}s ({verdict})"
        lines.append(f"- {label}: {status}")

    return ToolResult(
        success=True,
        exit_code=0,
        stdout="\n".join(lines),
        stderr="",
        operation_id=operation_id,
    )
