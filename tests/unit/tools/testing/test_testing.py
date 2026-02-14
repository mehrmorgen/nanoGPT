"""Misc unit tests for TestingTools facade to bump coverage on edge paths."""

from __future__ import annotations

import os
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator

import pytest

import ml_playground.tools.core.config as config_module
from ml_playground.tools.core.config import ToolsConfig
from ml_playground.tools.core.errors import ToolExecutionError
from ml_playground.tools.testing.coverage_helpers import format_coverage_status
from ml_playground.tools.testing.testing import TestingTools as _TestingTools
from ml_playground.tools.core.interfaces import OperationId, ToolResult
from tests.unit.tools.fakes import FakeSubprocessRunner


_MISSING = object()


@contextmanager
def override_attr(obj: Any, name: str, value: Any) -> Iterator[None]:
    original = getattr(obj, name, _MISSING)
    object.__setattr__(obj, name, value)
    try:
        yield
    finally:
        if original is _MISSING:
            delattr(obj, name)
        else:
            object.__setattr__(obj, name, original)


@contextmanager
def override_env(name: str, value: str | None) -> Iterator[None]:
    original = os.environ.get(name)
    if value is None:
        os.environ.pop(name, None)
    else:
        os.environ[name] = value
    try:
        yield
    finally:
        if original is None:
            os.environ.pop(name, None)
        else:
            os.environ[name] = original


@pytest.fixture
def cfg() -> ToolsConfig:
    return ToolsConfig(
        testing=config_module.TestToolsConfig(
            timeout=60, coverage_threshold=80.0, parallel_workers=2
        )
    )


def test_format_coverage_status_variants() -> None:
    ok = format_coverage_status(
        metric="Line", percentage=91.23, threshold=90.00, passed=True
    )
    fail = format_coverage_status(
        metric="Branch", percentage=65.0, threshold=70.0, passed=False
    )
    assert "SUCCESS" in ok and "Line coverage 91.23% >= 90.00%" in ok
    assert "FAILURE" in fail and "Branch coverage 65.00% < 70.00%" in fail


def test_coverage_report_raises_on_ci_empty_file(
    cfg: ToolsConfig, tmp_path: Path
) -> None:
    tools = _TestingTools(cfg, tmp_path, FakeSubprocessRunner())
    # Create empty coverage file
    cov = tmp_path / ".cache" / "coverage" / "coverage.sqlite"
    cov.parent.mkdir(parents=True, exist_ok=True)
    cov.write_bytes(b"")

    # Stub _ensure_coverage_data to no-op so we hit the CI empty-file guard
    def _noop_ensure(
        *,
        args: list[str],
        learning_mode: bool,
        verbosity_level: int,
        verbose: bool,
        operation_id: Any,
        executed_commands: list[str],
        force_regen: bool = False,
    ) -> tuple[list[str], list[str], dict[str, str]]:
        return [], [], {"COVERAGE_FILE": str(cov)}

    with override_attr(tools, "_ensure_coverage_data", _noop_ensure):
        with override_env("CI", "true"):
            with pytest.raises(ToolExecutionError):
                tools.coverage_report([], verbose=False)


def test_unit_learning_mode_attaches_info(cfg: ToolsConfig, tmp_path: Path) -> None:
    tools = _TestingTools(cfg, tmp_path, FakeSubprocessRunner())
    result = tools.unit(["-q"], learning_mode=True, verbosity_level=1)
    assert result.success is True
    assert result.learning_info is not None
    assert result.learning_info.commands_executed or result.learning_info.explanations


def test_integration_learning_mode_attaches_info(
    cfg: ToolsConfig, tmp_path: Path
) -> None:
    tools = _TestingTools(cfg, tmp_path, FakeSubprocessRunner())
    result = tools.integration(["-q"], learning_mode=True, verbosity_level=1)
    assert result.success is True
    assert result.learning_info is not None
    assert result.learning_info.commands_executed or result.learning_info.explanations


def test_acceptance_learning_mode_attaches_info(
    cfg: ToolsConfig, tmp_path: Path
) -> None:
    tools = _TestingTools(cfg, tmp_path, FakeSubprocessRunner())
    result = tools.acceptance(["-q"], learning_mode=True, verbosity_level=1)
    assert result.success is True
    assert result.learning_info is not None
    assert result.learning_info.commands_executed or result.learning_info.explanations


def test_e2e_learning_mode_attaches_info(cfg: ToolsConfig, tmp_path: Path) -> None:
    tools = _TestingTools(cfg, tmp_path, FakeSubprocessRunner())
    result = tools.e2e(["-q"], learning_mode=True, verbosity_level=1)
    assert result.success is True
    assert result.learning_info is not None
    assert result.learning_info.commands_executed or result.learning_info.explanations


def test_property_learning_mode_attaches_info(cfg: ToolsConfig, tmp_path: Path) -> None:
    tools = _TestingTools(cfg, tmp_path, FakeSubprocessRunner())
    result = tools.property_tests(["-q"], learning_mode=True, verbosity_level=1)
    assert result.success is True
    assert result.learning_info is not None
    assert result.learning_info.commands_executed or result.learning_info.explanations


def test_regression_learning_mode_attaches_info(
    cfg: ToolsConfig, tmp_path: Path
) -> None:
    tools = _TestingTools(cfg, tmp_path, FakeSubprocessRunner())
    result = tools.regression(["-q"], learning_mode=True, verbosity_level=1)
    assert result.success is True
    assert result.learning_info is not None
    assert result.learning_info.commands_executed or result.learning_info.explanations


def test_regression_without_learning_mode_has_no_learning_info(
    cfg: ToolsConfig, tmp_path: Path
) -> None:
    tools = _TestingTools(cfg, tmp_path, FakeSubprocessRunner())
    result = tools.regression(["-q"])
    assert result.success is True
    assert result.learning_info is not None
    assert result.learning_info.commands_executed == []
    assert result.learning_info.explanations == []


def test_all_tests_learning_mode_attaches_info(
    cfg: ToolsConfig, tmp_path: Path
) -> None:
    tools = _TestingTools(cfg, tmp_path, FakeSubprocessRunner())
    result = tools.all_tests(["-q"], learning_mode=True, verbosity_level=1)
    assert result.success is True
    assert result.learning_info is not None
    assert result.learning_info.commands_executed or result.learning_info.explanations


def test_clean_executes_without_error(cfg: ToolsConfig, tmp_path: Path) -> None:
    tools = _TestingTools(cfg, tmp_path, FakeSubprocessRunner())
    result = tools.clean([])
    assert result.success is True
    assert result.stdout.strip() != ""


def test_clean_learning_mode_includes_explanation(
    cfg: ToolsConfig, tmp_path: Path
) -> None:
    tools = _TestingTools(cfg, tmp_path, FakeSubprocessRunner())
    result = tools.clean([], learning_mode=True, verbosity_level=2)

    assert result.learning_info is not None
    assert any("Cleaning" in part for part in result.learning_info.explanations)


def test_clean_removes_file_artifacts(cfg: ToolsConfig, tmp_path: Path) -> None:
    tools = _TestingTools(cfg, tmp_path, FakeSubprocessRunner())

    tools._cache_dir.mkdir(parents=True, exist_ok=True)
    file_target = tools._cache_dir / "coverage"
    file_target.write_text("not a dir", encoding="utf-8")

    result = tools.clean([])

    assert result.success is True
    assert file_target.exists() is False
    assert "coverage" in result.stdout


def test_mutation_run_stops_on_first_failing_step(
    cfg: ToolsConfig, tmp_path: Path
) -> None:
    # Subclass TestingTools to override internal methods without mocks
    class FakeMutationTestingTools(_TestingTools):
        def _mutation_reset(self, args: list[str]) -> ToolResult:
            return ToolResult(
                success=False,
                exit_code=42,
                stdout="reset failed",
                stderr="reset error",
                operation_id=OperationId(
                    namespace="tools", category="test", command="mutation-reset"
                ),
            )

    tools = FakeMutationTestingTools(cfg, tmp_path, FakeSubprocessRunner())
    result = tools.mutation_run([])

    assert result.success is False
    assert result.exit_code == 42
    assert "reset failed" in result.stdout


def test_mutation_run_wraps_unexpected_exceptions(
    cfg: ToolsConfig, tmp_path: Path
) -> None:
    """mutation_run should wrap unexpected exceptions into a failing ToolResult.

    This exercises the defensive catch-all branch in the mutation pipeline.
    """

    class ExplodingMutationTestingTools(_TestingTools):
        def _mutation_reset(self, args: list[str]) -> ToolResult:
            raise RuntimeError("boom")

    tools = ExplodingMutationTestingTools(cfg, tmp_path, FakeSubprocessRunner())
    result = tools.mutation_run([])

    assert result.success is False
    assert result.exit_code == 1
    assert "Mutation reset failed: boom" in (result.stderr or "")


def test_mutation_run_aggregates_stderr_without_stdout(
    cfg: ToolsConfig, tmp_path: Path
) -> None:
    class WarningMutationTestingTools(_TestingTools):
        def _mutation_reset(self, args: list[str]) -> ToolResult:
            return ToolResult(
                success=True,
                exit_code=0,
                stdout="",
                stderr="warn",
                operation_id=OperationId(
                    namespace="tools", category="test", command="mutation-reset"
                ),
            )

        def _mutation_summary(self, args: list[str]) -> ToolResult:
            return ToolResult(
                success=True,
                exit_code=0,
                stdout="ok",
                stderr="",
                operation_id=OperationId(
                    namespace="tools", category="test", command="mutation-step"
                ),
            )

        def _mutation_init(self, args: list[str]) -> ToolResult:
            return ToolResult(
                success=True,
                exit_code=0,
                stdout="",
                stderr="",
                operation_id=OperationId(
                    namespace="tools", category="test", command="mutation-init"
                ),
            )

        def _mutation_exec(self, args: list[str]) -> ToolResult:
            return ToolResult(
                success=True,
                exit_code=0,
                stdout="",
                stderr="",
                operation_id=OperationId(
                    namespace="tools", category="test", command="mutation-exec"
                ),
            )

        def _mutation_report(self, args: list[str]) -> ToolResult:
            return ToolResult(
                success=True,
                exit_code=0,
                stdout="",
                stderr="",
                operation_id=OperationId(
                    namespace="tools", category="test", command="mutation-report"
                ),
            )

    tools = WarningMutationTestingTools(cfg, tmp_path, FakeSubprocessRunner())
    result = tools.mutation_run([])

    assert result.success is True
    assert "Mutation reset warnings:" in (result.stderr or "")
    assert "warn" in (result.stderr or "")
