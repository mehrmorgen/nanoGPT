from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path
from typing import Iterator, List

import pytest

from ml_playground.tools.ci.ci import CITools
from ml_playground.tools.core.config import ToolsConfig
from ml_playground.tools.core.errors import ToolExecutionError
from ml_playground.tools.core.interfaces import OperationId, ToolResult
from tests.unit.tools.fakes import (
    FakeSubprocessRunner,
    create_failure_result,
    create_success_result,
)


@contextmanager
def _override_attr(obj: object, name: str, value: object) -> Iterator[None]:
    original = getattr(obj, name)
    object.__setattr__(obj, name, value)
    try:
        yield
    finally:
        object.__setattr__(obj, name, original)


@pytest.fixture()
def ci_tools(tmp_path: Path) -> tuple[CITools, FakeSubprocessRunner]:
    runner = FakeSubprocessRunner()
    tools = CITools(ToolsConfig(), tmp_path, subprocess_runner=runner)
    tools.cache_dir = tmp_path / ".cache"
    return tools, runner


def test_coverage_badge_with_existing_json(
    ci_tools: tuple[CITools, FakeSubprocessRunner],
) -> None:
    tools, runner = ci_tools
    coverage_dir = tools.cache_dir / "coverage"
    coverage_dir.mkdir(parents=True)
    (coverage_dir / "coverage.json").write_text(
        '{"totals": {"percent_covered": 85.5}}', encoding="utf-8"
    )

    result = tools.coverage_badge([])

    assert result.success is True
    assert "85.5% coverage" in (result.stdout or "")
    assert runner.calls == []


def test_coverage_badge_generates_json_when_missing(
    ci_tools: tuple[CITools, FakeSubprocessRunner],
) -> None:
    tools, runner = ci_tools
    operation_id = OperationId(
        namespace="tools", category="ci", command="coverage-badge"
    )
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
                self._json_target.write_text(
                    '{"totals": {"percent_covered": 75.0}}', encoding="utf-8"
                )
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


def test_coverage_badge_generation_failure(
    ci_tools: tuple[CITools, FakeSubprocessRunner],
) -> None:
    tools, _ = ci_tools
    coverage_dir = tools.cache_dir / "coverage"
    coverage_dir.mkdir(parents=True)
    (coverage_dir / "coverage.json").write_text("{not-json}", encoding="utf-8")

    result = tools.coverage_badge([])

    assert result.success is False
    assert "Failed to generate coverage badge" in (result.stderr or "")


def test_coverage_badge_raises_when_coverage_json_fails(
    ci_tools: tuple[CITools, FakeSubprocessRunner],
) -> None:
    tools, runner = ci_tools
    operation_id = OperationId(
        namespace="tools", category="ci", command="coverage-badge"
    )
    runner.set_results(
        [create_failure_result(operation_id, 1, stderr="coverage json failed")]
    )

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
    (coverage_dir / "coverage.json").write_text(
        '{"totals": {"percent_covered": 88.2}}', encoding="utf-8"
    )

    result = tools.coverage_badge([])

    assert result.success is True
    expected_badge = (tmp_path / config.ci.badge_output_dir / "coverage.svg").resolve()
    assert expected_badge.exists()
    assert "88.2% coverage" in (result.stdout or "")


def test_coverage_badge_color_threshold_yellow(tmp_path: Path) -> None:
    tools = CITools(ToolsConfig(), tmp_path, subprocess_runner=FakeSubprocessRunner())
    tools.cache_dir = tmp_path / ".cache"

    coverage_dir = tools.cache_dir / "coverage"
    coverage_dir.mkdir(parents=True)
    (coverage_dir / "coverage.json").write_text(
        '{"totals": {"percent_covered": 65.0}}', encoding="utf-8"
    )

    result = tools.coverage_badge([])

    assert result.success is True
    badge = (tmp_path / tools.config.ci.badge_output_dir / "coverage.svg").resolve()
    assert "yellow" in badge.read_text(encoding="utf-8")


def test_coverage_badge_color_threshold_red(tmp_path: Path) -> None:
    tools = CITools(ToolsConfig(), tmp_path, subprocess_runner=FakeSubprocessRunner())
    tools.cache_dir = tmp_path / ".cache"

    coverage_dir = tools.cache_dir / "coverage"
    coverage_dir.mkdir(parents=True)
    (coverage_dir / "coverage.json").write_text(
        '{"totals": {"percent_covered": 55.0}}', encoding="utf-8"
    )

    result = tools.coverage_badge([])

    assert result.success is True
    badge = (tmp_path / tools.config.ci.badge_output_dir / "coverage.svg").resolve()
    assert "red" in badge.read_text(encoding="utf-8")


def test_quality_fast_aggregates_stderr_warnings(
    ci_tools: tuple[CITools, FakeSubprocessRunner],
) -> None:
    tools, runner = ci_tools
    op_id = OperationId(namespace="tools", category="ci", command="quality-fast")
    runner.set_results(
        [
            create_success_result(op_id, stdout="", stderr="warn"),
            create_success_result(op_id, stdout="formatted", stderr=""),
            create_success_result(op_id, stdout="", stderr=""),
        ]
    )

    result = tools.quality_fast([])

    assert result.success is True
    assert "ruff warnings:\nwarn" in (result.stderr or "")
    assert "ruff-format:\nformatted" in (result.stdout or "")


def test_quality_ci_local_without_cache_binds(tmp_path: Path) -> None:
    runner = FakeSubprocessRunner()
    tools = CITools(ToolsConfig(), tmp_path, subprocess_runner=runner)

    result = tools.quality_ci_local([], bind_caches=False)
    assert runner.calls
    assert "--bind" not in runner.calls[0]["command"]

    assert result.success is True
