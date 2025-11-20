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
    setattr(obj, name, value)
    try:
        yield
    finally:
        if original is _MISSING:
            delattr(obj, name)
        else:
            setattr(obj, name, original)


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


def test_coverage_env_creates_directories_and_env(cfg: ToolsConfig, tmp_path: Path) -> None:
    tools = _TestingTools(cfg, tmp_path, FakeSubprocessRunner())

    env = tools._coverage_env()  # type: ignore[attr-defined]

    # Directories for coverage and hypothesis should exist
    assert (tmp_path / ".cache" / "coverage").exists()
    assert (tmp_path / ".cache" / "hypothesis").exists()

    # Environment variables should include coverage and hypothesis settings
    assert env["COVERAGE_FILE"].endswith("coverage.sqlite")
    assert "HYPOTHESIS_DATABASE_DIRECTORY" in env
    assert "HYPOTHESIS_STORAGE_DIRECTORY" in env
    assert env["HYPOTHESIS_SEED"] == "0"
    assert env["PYTHONHASHSEED"] == "0"


def test_read_coverage_thresholds_from_config_reads_values(
    cfg: ToolsConfig, tmp_path: Path
) -> None:
    tools = _TestingTools(cfg, tmp_path, FakeSubprocessRunner())

    pyproject = tmp_path / "pyproject.toml"
    pyproject.write_text(
        """
[tool.ml_playground.coverage.thresholds]
line_threshold = 91.5
branch_threshold = 83.0
""".strip(),
        encoding="utf-8",
    )

    thresholds = tools._read_coverage_thresholds_from_config()  # type: ignore[attr-defined]
    assert thresholds["line_threshold"] == 91.5
    assert thresholds["branch_threshold"] == 83.0


def test_collect_coverage_metrics_uses_existing_json(cfg: ToolsConfig, tmp_path: Path) -> None:
    tools = _TestingTools(cfg, tmp_path, FakeSubprocessRunner())

    # Prepare coverage JSON with totals including branches
    coverage_dir = tmp_path / ".cache" / "coverage"
    coverage_dir.mkdir(parents=True, exist_ok=True)
    json_path = coverage_dir / "coverage.json"
    json_path.write_text(
        """
{"totals": {"num_statements": 10, "covered_lines": 9, "num_branches": 4, "covered_branches": 3}}
""".strip(),
        encoding="utf-8",
    )

    executed: list[str] = []
    result, metrics = tools._collect_coverage_metrics(  # type: ignore[attr-defined]
        env={"COVERAGE_FILE": str(coverage_dir / "coverage.sqlite")},
        operation_id=OperationId(namespace="tools", category="test", command="coverage"),
        executed_commands=executed,
    )

    assert result is None
    assert any("Coverage totals:" in line for line in metrics)
    assert any("Branch totals:" in line for line in metrics)


def test_mutation_run_stops_on_first_failing_step(cfg: ToolsConfig, tmp_path: Path) -> None:
    tools = _TestingTools(cfg, tmp_path, FakeSubprocessRunner())

    # Patch mutation_reset to simulate an early failure while other steps succeed.
    def failing_reset(_args: list[str]) -> ToolResult:
        return ToolResult(
            success=False,
            exit_code=42,
            stdout="reset failed",
            stderr="reset error",
            operation_id=OperationId(namespace="tools", category="test", command="mutation-reset"),
        )

    with override_attr(tools, "mutation_reset", failing_reset):
        result = tools.mutation_run([])

    assert result.success is False
    assert result.exit_code == 42
    assert "reset failed" in result.stdout


def test_mutation_run_wraps_unexpected_exceptions(cfg: ToolsConfig, tmp_path: Path) -> None:
    """mutation_run should wrap unexpected exceptions into a failing ToolResult.

    This exercises the defensive catch-all branch in the mutation pipeline.
    """

    tools = _TestingTools(cfg, tmp_path, FakeSubprocessRunner())

    def exploding_reset(_args: list[str]) -> ToolResult:
        raise RuntimeError("boom")

    with override_attr(tools, "mutation_reset", exploding_reset):
        result = tools.mutation_run([])

    assert result.success is False
    assert result.exit_code == 1
    assert "Mutation reset failed: boom" in result.stderr


def test_ensure_coverage_data_uses_cached_manifest_when_coverage_present(
    cfg: ToolsConfig, tmp_path: Path
) -> None:
    tools = _TestingTools(cfg, tmp_path, FakeSubprocessRunner())

    coverage_file = tmp_path / ".cache" / "coverage" / "coverage.sqlite"
    coverage_file.parent.mkdir(parents=True, exist_ok=True)
    coverage_file.write_bytes(b"data")

    with override_attr(tools, "_compute_coverage_fingerprint", lambda: "fp"), override_attr(
        tools, "_read_coverage_manifest", lambda: {"fingerprint": "fp"}
    ):
        executed: list[str] = []
        result, notes, env = tools._ensure_coverage_data(  # type: ignore[attr-defined]
            args=[],
            learning_mode=False,
            verbosity_level=1,
            verbose=False,
            operation_id=OperationId(
                namespace="tools", category="test", command="coverage"
            ),
            executed_commands=executed,
        )

    assert result is None
    assert notes == []
    assert env["COVERAGE_FILE"].endswith("coverage.sqlite")
    assert executed == []


def test_ensure_coverage_data_combines_existing_fragments_without_regen(
    cfg: ToolsConfig, tmp_path: Path
) -> None:
    tools = _TestingTools(cfg, tmp_path, FakeSubprocessRunner())

    coverage_file = tmp_path / ".cache" / "coverage" / "coverage.sqlite"
    coverage_file.parent.mkdir(parents=True, exist_ok=True)
    # Start with an empty coverage file so we do not take the early cache hit path.
    coverage_file.write_bytes(b"")

    def fake_combine(
        *, env: dict[str, str], operation_id: OperationId, executed_commands: list[str]
    ) -> tuple[ToolResult | None, bool]:
        # Simulate that combine created a non-empty coverage file so the
        # manifest-writing branch is taken.
        coverage_file.write_bytes(b"combined")
        return None, True

    def fail_run_for_data(**_: object) -> tuple[ToolResult | None, list[str]]:
        raise AssertionError("_run_coverage_test_for_data should not be called")

    with override_attr(tools, "_compute_coverage_fingerprint", lambda: "fp"), (
        override_attr(tools, "_read_coverage_manifest", lambda: {"fingerprint": "fp"})
    ), override_attr(tools, "_combine_coverage_fragments", fake_combine), override_attr(
        tools, "_run_coverage_test_for_data", fail_run_for_data
    ):
        executed: list[str] = []
        result, notes, _ = tools._ensure_coverage_data(  # type: ignore[attr-defined]
            args=[],
            learning_mode=False,
            verbosity_level=1,
            verbose=True,
            operation_id=OperationId(
                namespace="tools", category="test", command="coverage"
            ),
            executed_commands=executed,
        )

    assert result is None
    assert any("Combined existing coverage fragments" in note for note in notes)


def test_run_coverage_test_for_data_propagates_failure(
    cfg: ToolsConfig, tmp_path: Path
) -> None:
    tools = _TestingTools(cfg, tmp_path, FakeSubprocessRunner())

    def failing_coverage_test(
        args: list[str], *, learning_mode: bool, verbosity_level: int
    ) -> ToolResult:
        return ToolResult(
            success=False,
            exit_code=2,
            stdout="boom",
            stderr="err",
            operation_id=OperationId(
                namespace="tools", category="test", command="coverage-test"
            ),
        )

    with override_attr(tools, "coverage_test", failing_coverage_test):
        executed: list[str] = []
        result, notes = tools._run_coverage_test_for_data(  # type: ignore[attr-defined]
            args=["-q"],
            verbosity_level=1,
            verbose=False,
            operation_id=OperationId(
                namespace="tools", category="test", command="coverage"
            ),
            executed_commands=executed,
        )

    assert isinstance(result, ToolResult)
    assert result.exit_code == 2
    assert notes == []
    assert any("coverage" in cmd for cmd in executed)


class _FailingPytestRunner(FakeSubprocessRunner):
    def run_pytest_command(self, *args: Any, **kwargs: Any) -> ToolResult:  # type: ignore[override]
        return ToolResult(
            success=False,
            exit_code=3,
            stdout="pytest boom",
            stderr="pytest err",
            operation_id=kwargs.get(
                "operation_id",
                OperationId(namespace="tools", category="test", command="pytest"),
            ),
        )


def test_generate_coverage_via_pytest_failure_returns_result(
    cfg: ToolsConfig, tmp_path: Path
) -> None:
    tools = _TestingTools(cfg, tmp_path, _FailingPytestRunner())

    executed: list[str] = []
    result, notes = tools._generate_coverage_via_pytest(  # type: ignore[attr-defined]
        args=["-q"],
        verbose=False,
        operation_id=OperationId(
            namespace="tools", category="test", command="coverage"
        ),
        executed_commands=executed,
    )

    assert isinstance(result, ToolResult)
    assert result.exit_code == 3
    assert notes == []
    assert any("pytest" in cmd for cmd in executed)


def test_generate_coverage_via_pytest_success_adds_verbose_output(
    cfg: ToolsConfig, tmp_path: Path
) -> None:
    tools = _TestingTools(cfg, tmp_path, FakeSubprocessRunner())

    def fake_clean(result: ToolResult) -> ToolResult:
        result.stdout = "ok-out"
        result.stderr = "ok-err"
        return result

    with override_attr(tools, "_clean_pytest_result", fake_clean):
        executed: list[str] = []
        result, notes = tools._generate_coverage_via_pytest(  # type: ignore[attr-defined]
            args=["-q"],
            verbose=True,
            operation_id=OperationId(
                namespace="tools", category="test", command="coverage"
            ),
            executed_commands=executed,
        )

    assert result is None
    assert any("Coverage pipeline generated no data" in note for note in notes)
    assert any("ok-out" in note or "ok-err" in note for note in notes)
    assert any("pytest" in cmd for cmd in executed)
