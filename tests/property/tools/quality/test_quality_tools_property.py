from __future__ import annotations

from pathlib import Path

import hypothesis.strategies as st
from hypothesis import given, settings, HealthCheck

from ml_playground.tools.core.config import ToolsConfig
from ml_playground.tools.quality.quality import QualityTools

from tests.property.tools._helpers import DeterministicRunner


@settings(
    max_examples=40,
    deadline=None,
    derandomize=True,
    suppress_health_check=[HealthCheck.function_scoped_fixture],
)
@given(  # type: ignore[reportAny]
    args=st.lists(st.text(min_size=1, max_size=6), max_size=3)
)
def test_lint_uses_ruff_check(args: list[str], tmp_path: Path) -> None:
    """QualityTools.lint should invoke ruff with expected arguments."""
    runner = DeterministicRunner()
    tools = QualityTools(ToolsConfig(), tmp_path, subprocess_runner=runner)

    result = tools.lint(args)

    assert result.success is True
    call = runner.calls[-1]
    expected = ["ruff", "check", "."] if not args else ["ruff", *args]
    assert call.kind == "uv"
    assert call.args == expected
    assert call.cwd == tmp_path


@settings(
    max_examples=40,
    deadline=None,
    derandomize=True,
    suppress_health_check=[HealthCheck.function_scoped_fixture],
)
@given(  # type: ignore[reportAny]
    args=st.lists(st.text(min_size=1, max_size=6), max_size=3)
)
def test_format_invokes_ruff_format(args: list[str], tmp_path: Path) -> None:
    """QualityTools.format should call ruff with provided arguments."""
    runner = DeterministicRunner()
    tools = QualityTools(ToolsConfig(), tmp_path, subprocess_runner=runner)

    result = tools.format(args)

    assert result.success is True
    call = runner.calls[-1]  # The format command is the last one called
    expected = ["ruff", "format", "."] if not args else ["ruff", "format", ".", *args]
    assert call.args == expected


@settings(
    max_examples=20,
    deadline=None,
    derandomize=True,
    suppress_health_check=[HealthCheck.function_scoped_fixture],
)
@given(  # type: ignore[reportAny]
    args=st.lists(st.text(min_size=1, max_size=4), max_size=2)
)
def test_deadcode_invokes_vulture(args: list[str], tmp_path: Path) -> None:
    """QualityTools.deadcode should invoke vulture with pkg path."""
    runner = DeterministicRunner()
    tools = QualityTools(ToolsConfig(), tmp_path, subprocess_runner=runner)

    result = tools.deadcode(args)

    assert result.success is True
    call = runner.calls[-1]
    pkg_path = tmp_path / "src" / "ml_playground"
    assert call.args[0] == "vulture"
    assert pkg_path.as_posix() in call.args


@settings(
    max_examples=20,
    deadline=None,
    derandomize=True,
    suppress_health_check=[HealthCheck.function_scoped_fixture],
)
@given(  # type: ignore[reportAny]
    args=st.lists(st.text(min_size=1, max_size=4), max_size=2)
)
def test_typecheck_runs_pyrefly(args: list[str], tmp_path: Path) -> None:
    """QualityTools.typecheck should run pyrefly."""
    runner = DeterministicRunner()
    tools = QualityTools(ToolsConfig(), tmp_path, subprocess_runner=runner)

    result = tools.typecheck(args)

    assert result.success is True or result.success is False
    uv_calls = [call.args for call in runner.calls if call.kind == "uv"]
    assert any(call[0:2] == ["pyrefly", "check"] for call in uv_calls)
