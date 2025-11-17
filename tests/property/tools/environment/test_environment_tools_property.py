from __future__ import annotations

from pathlib import Path

import hypothesis.strategies as st
from hypothesis import given, settings, HealthCheck

from ml_playground.tools.core.config import ToolsConfig
from ml_playground.tools.environment.environment import EnvironmentTools

from tests.property.tools._helpers import DeterministicRunner


@settings(
    max_examples=25,
    deadline=None,
    derandomize=True,
    suppress_health_check=[HealthCheck.function_scoped_fixture],
)
@given(
    args=st.lists(st.text(min_size=1, max_size=8), max_size=3),
    groups=st.one_of(st.none(), st.lists(st.text(min_size=1, max_size=6), max_size=2)),
    all_groups=st.booleans(),
    frozen=st.booleans(),
)
def test_sync_constructs_uv_arguments(
    args: list[str],
    groups: list[str] | None,
    all_groups: bool,
    frozen: bool,
    tmp_path: Path,
) -> None:
    """sync should translate flags into the underlying uv command."""
    runner = DeterministicRunner()
    tools = EnvironmentTools(ToolsConfig(), tmp_path, subprocess_runner=runner)

    result = tools.sync(args, groups=groups, all_groups=all_groups, frozen=frozen)

    assert result.success is True
    assert len(runner.calls) == 1
    call = runner.calls[0]
    expected: list[str] = ["sync"]
    if frozen:
        expected.append("--frozen")
    if all_groups:
        expected.append("--all-groups")
    elif groups:
        for group in groups:
            expected.extend(["--group", group])
    expected.extend(args)
    assert call.kind == "uv"
    assert call.args == expected
    assert call.cwd == tmp_path


@settings(
    max_examples=10,
    deadline=None,
    derandomize=True,
    suppress_health_check=[HealthCheck.function_scoped_fixture],
)
@given(clear=st.booleans())
def test_setup_creates_venv_and_syncs(clear: bool, tmp_path: Path) -> None:
    """setup should invoke uv venv and uv sync in sequence."""
    runner = DeterministicRunner()
    tools = EnvironmentTools(ToolsConfig(), tmp_path, subprocess_runner=runner)

    result = tools.setup([], clear=clear)

    assert result.success is True
    uv_calls = [call for call in runner.calls if call.kind == "uv"]
    assert len(uv_calls) >= 2
    venv_call = uv_calls[0]
    sync_call = uv_calls[1]
    expected_venv = ["venv"] + (["--clear"] if clear else [])
    assert venv_call.args == expected_venv
    assert sync_call.args == ["sync", "--all-groups"]


@settings(
    max_examples=10,
    deadline=None,
    derandomize=True,
    suppress_health_check=[HealthCheck.function_scoped_fixture],
)
@given(st.just(()))
def test_verify_uses_python_import_command(_: tuple[()]) -> None:
    """verify should attempt to import the configured package via uv."""
    tmp_root = Path.cwd() / ".tmp_env_verify"
    tmp_root.mkdir(exist_ok=True)
    runner = DeterministicRunner()
    tools = EnvironmentTools(ToolsConfig(), tmp_root, subprocess_runner=runner)

    result = tools.verify([])

    assert result.success is True
    call = runner.calls[-1]
    assert call.kind == "uv"
    assert call.args == [
        "python",
        "-c",
        "import ml_playground; print('✓ ml_playground import OK')",
    ]
