from __future__ import annotations

from pathlib import Path
from typing import List

import pytest

from ml_playground.tools.ci.ci import CITools
from ml_playground.tools.core.config import ToolsConfig
from ml_playground.tools.core.errors import ToolExecutionError
from ml_playground.tools.core.interfaces import OperationId, ToolResult
from tests.unit.tools.fakes import FakeSubprocessRunner, create_failure_result, create_success_result


@pytest.fixture()
def ci_tools(tmp_path: Path) -> tuple[CITools, FakeSubprocessRunner]:
    runner = FakeSubprocessRunner()
    tools = CITools(ToolsConfig(), tmp_path, subprocess_runner=runner)
    tools.cache_dir = tmp_path / ".cache"
    return tools, runner


def test_coverage_badge_with_existing_json(ci_tools: tuple[CITools, FakeSubprocessRunner]) -> None:
    tools, runner = ci_tools
    coverage_dir = tools.cache_dir / "coverage"
    coverage_dir.mkdir(parents=True)
    (coverage_dir / "coverage.json").write_text('{"totals": {"percent_covered": 85.5}}', encoding="utf-8")

    result = tools.coverage_badge([])

    assert result.success is True
    assert "85.5% coverage" in (result.stdout or "")
    assert runner.calls == []


def test_coverage_badge_generates_json_when_missing(ci_tools: tuple[CITools, FakeSubprocessRunner]) -> None:
    tools, runner = ci_tools
    operation_id = OperationId(namespace="tools", category="ci", command="coverage-badge")
    runner.set_results([create_success_result(operation_id, "Coverage generated")])
    coverage_dir = tools.cache_dir / "coverage"
    coverage_dir.mkdir(parents=True)
    json_path = coverage_dir / "coverage.json"

    class JsonGeneratingRunner(FakeSubprocessRunner):
        def __init__(self, json_target: Path) -> None:
            super().__init__()
            self._json_target = json_target

        def run_uv_command(  # type: ignore[override]
            self,
            args: List[str],
            *,
            cwd: str | Path | None = None,
            env: dict[str, str] | None = None,
            timeout: int | None = None,
            operation_id: OperationId,
            python: str | None = None,
            no_project: bool = False,
        ) -> ToolResult:
            if args[:2] == ["coverage", "json"]:
                self._json_target.write_text('{"totals": {"percent_covered": 75.0}}', encoding="utf-8")
            return super().run_uv_command(
                args,
                cwd=cwd,
                env=env,
                timeout=timeout,
                operation_id=operation_id,
                python=python,
                no_project=no_project,
            )

    json_runner = JsonGeneratingRunner(json_path)
    json_runner.set_results([create_success_result(operation_id, "Coverage generated")])
    json_tools = CITools(ToolsConfig(), tools.root_path, subprocess_runner=json_runner)
    json_tools.cache_dir = tools.cache_dir

    result = json_tools.coverage_badge([])

    assert result.success is True
    assert "75.0% coverage" in (result.stdout or "")


def test_coverage_badge_generation_failure(ci_tools: tuple[CITools, FakeSubprocessRunner]) -> None:
    tools, _ = ci_tools
    coverage_dir = tools.cache_dir / "coverage"
    coverage_dir.mkdir(parents=True)
    (coverage_dir / "coverage.json").write_text("{not-json}", encoding="utf-8")

    result = tools.coverage_badge([])

    assert result.success is False
    assert "Failed to generate coverage badge" in (result.stderr or "")


def test_coverage_badge_raises_when_coverage_json_fails(ci_tools: tuple[CITools, FakeSubprocessRunner]) -> None:
    tools, runner = ci_tools
    operation_id = OperationId(namespace="tools", category="ci", command="coverage-badge")
    runner.set_results([create_failure_result(operation_id, 1, stderr="coverage json failed")])

    with pytest.raises(ToolExecutionError) as exc_info:
        tools.coverage_badge([])

    assert "Failed to generate coverage JSON" in str(exc_info.value)
    assert runner.calls  # ensures the command executed


def test_coverage_badge_respects_output_dir(tmp_path: Path) -> None:
    config = ToolsConfig()
    config.ci.badge_output_dir = Path("artifacts/badges")
    runner = FakeSubprocessRunner()
    tools = CITools(config, tmp_path, subprocess_runner=runner)
    tools.cache_dir = tmp_path / ".cache"

    coverage_dir = tools.cache_dir / "coverage"
    coverage_dir.mkdir(parents=True)
    (coverage_dir / "coverage.json").write_text('{"totals": {"percent_covered": 88.2}}', encoding="utf-8")

    result = tools.coverage_badge([])

    assert result.success is True
    expected_badge = (tmp_path / config.ci.badge_output_dir / "coverage.svg").resolve()
    assert expected_badge.exists()
    assert "88.2% coverage" in (result.stdout or "")
