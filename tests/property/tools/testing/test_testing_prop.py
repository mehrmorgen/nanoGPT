from __future__ import annotations

from pathlib import Path
from typing import Any, Callable, ContextManager

import hypothesis.strategies as st
from hypothesis import given, settings, HealthCheck

import ml_playground.tools.testing.testing as testing_module
import ml_playground.tools.testing.coverage as coverage_module
import ml_playground.tools.testing.mutation as mutation_module
from ml_playground.tools.core.config import ToolsConfig
from ml_playground.tools.core.interfaces import OperationId, ToolResult

from tests.property.tools._helpers import DeterministicRunner


def _success(command: str, stdout: str = "ok") -> ToolResult:
    return ToolResult(
        success=True,
        exit_code=0,
        stdout=stdout,
        stderr="",
        operation_id=OperationId(namespace="tools", category="test", command=command),
    )


@settings(
    max_examples=20,
    deadline=None,
    derandomize=True,
    suppress_health_check=[HealthCheck.function_scoped_fixture],
)
@given(  # type: ignore[reportAny]
    args=st.lists(st.text(min_size=0, max_size=6), max_size=3)
)
def test_unit_delegates(
    args: list[str],
    tmp_path: Path,
    override_attr: Callable[[object, str, object], ContextManager[None]],
) -> None:
    seen: list[tuple[Any, ...]] = []

    def fake_run_unit(**kwargs: Any) -> ToolResult:
        seen.append((kwargs,))
        return _success("unit")

    tools = testing_module.TestingTools(ToolsConfig(), tmp_path, DeterministicRunner())
    with override_attr(testing_module, "run_unit", fake_run_unit):
        result = tools.unit(args)

    assert result.success is True
    assert seen
    kwargs = seen[0][0]
    assert kwargs["args"] == args


@settings(
    max_examples=20,
    deadline=None,
    derandomize=True,
    suppress_health_check=[HealthCheck.function_scoped_fixture],
)
@given(  # type: ignore[reportAny]
    args=st.lists(st.text(min_size=0, max_size=6), max_size=3)
)
def test_all_tests_runs_pytest(args: list[str], tmp_path: Path) -> None:
    runner = DeterministicRunner()
    tools = testing_module.TestingTools(ToolsConfig(), tmp_path, runner)

    result = tools.all_tests(args)

    assert runner.calls
    call = runner.calls[0]
    assert call.kind == "pytest"
    assert "tests/unit" in call.args
    assert result.operation_id.command == "all"


@settings(
    max_examples=20,
    deadline=None,
    derandomize=True,
    suppress_health_check=[HealthCheck.function_scoped_fixture],
)
@given(  # type: ignore[reportAny]
    args=st.lists(st.text(min_size=0, max_size=4), max_size=2)
)
def test_coverage_report_delegates(
    args: list[str],
    tmp_path: Path,
    override_attr: Callable[[object, str, object], ContextManager[None]],
) -> None:
    seen: list[tuple[Any, ...]] = []

    def fake_ensure_coverage_data(
        **kwargs: Any,
    ) -> tuple[list[str], list[str], dict[str, str]]:
        return [], [], {}

    def fake_run_report(**kwargs: Any) -> ToolResult:
        seen.append((kwargs,))
        return _success("coverage-report")

    tools = testing_module.TestingTools(ToolsConfig(), tmp_path, DeterministicRunner())
    with override_attr(
        coverage_module, "_ensure_coverage_data", fake_ensure_coverage_data
    ):
        with override_attr(testing_module, "run_coverage_report", fake_run_report):
            result = tools.coverage_report(args)

    assert result.success is True
    kwargs = seen[0][0]
    assert kwargs["args"] == args


@settings(
    max_examples=20,
    deadline=None,
    derandomize=True,
    suppress_health_check=[HealthCheck.function_scoped_fixture],
)
@given(  # type: ignore[reportAny]
    args=st.lists(st.text(min_size=0, max_size=4), max_size=2)
)
def test_coverage_threshold_delegates(
    args: list[str],
    tmp_path: Path,
    override_attr: Callable[[object, str, object], ContextManager[None]],
) -> None:
    seen: list[tuple[Any, ...]] = []

    def fake_run_threshold(**kwargs: Any) -> ToolResult:
        seen.append((kwargs,))
        return _success("coverage-threshold")

    tools = testing_module.TestingTools(ToolsConfig(), tmp_path, DeterministicRunner())
    with override_attr(coverage_module, "run_coverage_threshold", fake_run_threshold):
        result = tools.coverage_threshold(
            args, line_threshold=90.0, branch_threshold=80.0
        )

    assert result.success is True
    kwargs = seen[0][0]
    assert kwargs["line_threshold"] == 90.0
    assert kwargs["branch_threshold"] == 80.0


def test_clean_removes_artifacts(tmp_path: Path) -> None:
    cfg = ToolsConfig()
    tools = testing_module.TestingTools(cfg, tmp_path, DeterministicRunner())

    artifacts = [
        tmp_path / ".pytest_cache",
        tmp_path / "htmlcov",
        tmp_path / ".cache" / "coverage",
        tmp_path / ".cache" / "hypothesis",
    ]
    for path in artifacts:
        path.mkdir(parents=True, exist_ok=True)
        marker = path / "marker"
        marker.write_text("data")

    result = tools.clean([])

    assert result.success is True
    assert all(not path.exists() for path in artifacts)


def test_mutation_run_executes_steps(
    tmp_path: Path,
    override_attr: Callable[[object, str, object], ContextManager[None]],
) -> None:
    tools = testing_module.TestingTools(ToolsConfig(), tmp_path, DeterministicRunner())
    sequence: list[str] = []

    def recorder(name: str) -> Callable[..., ToolResult]:
        return lambda *args, **kwargs: sequence.append(name) or _success(name)

    def reset_stub(*_args: Any, **_kwargs: Any) -> ToolResult:
        return _success("reset")

    def summary_stub(*_args: Any, **_kwargs: Any) -> ToolResult:
        return _success("summary")

    with override_attr(mutation_module, "mutation_reset", reset_stub):
        with override_attr(mutation_module, "mutation_summary", summary_stub):
            with override_attr(mutation_module, "mutation_init", recorder("init")):
                with override_attr(mutation_module, "mutation_exec", recorder("exec")):
                    with override_attr(
                        mutation_module, "mutation_report", recorder("report")
                    ):
                        result = tools.mutation_run([])

    assert result.success is True
    assert sequence == ["init", "exec", "report"]
