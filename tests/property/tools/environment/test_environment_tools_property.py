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
@given(  # type: ignore[reportAny]
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
    expected: list[str] = ["uv", "sync"]
    if frozen:
        expected.append("--frozen")
    if all_groups:
        expected.extend(["--group", "all"])
    elif groups:
        for group in groups:
            expected.extend(["--group", group])
    expected.extend(args)
    assert call.kind == "subprocess"
    assert call.args == expected
    assert call.cwd == tmp_path


@settings(
    max_examples=10,
    deadline=None,
    derandomize=True,
    suppress_health_check=[HealthCheck.function_scoped_fixture],
)
@given(  # type: ignore[reportAny]
    clear=st.booleans()
)
def test_setup_creates_venv_and_syncs(clear: bool, tmp_path: Path) -> None:
    """setup should invoke uv venv and uv sync in sequence."""
    runner = DeterministicRunner()
    tools = EnvironmentTools(ToolsConfig(), tmp_path, subprocess_runner=runner)

    result = tools.setup([], clear=clear)

    assert result.success is True
    subprocess_calls = [call for call in runner.calls if call.kind == "subprocess"]
    assert len(subprocess_calls) >= 2
    venv_call = subprocess_calls[0]
    sync_call = subprocess_calls[1]
    expected_venv = ["uv", "venv"] + (["--clear"] if clear else [])
    assert venv_call.args == expected_venv
    assert sync_call.args == ["uv", "sync", "--group", "all"]


def test_verify_uses_python_import_command(tmp_path: Path) -> None:
    """verify should attempt import + toolchain checks via uv."""
    runner = DeterministicRunner()
    tools = EnvironmentTools(ToolsConfig(), tmp_path, subprocess_runner=runner)

    result = tools.verify([])

    assert result.success is True
    call = runner.calls[-1]
    assert call.kind == "uv"
    assert call.args[0:2] == ["python", "-c"]
    script = call.args[2]
    assert "import ml_playground" in script
    assert (
        "required = ['pre-commit', 'yamlfix', 'basedpyright', 'mypy', 'vulture']"
        in script
    )
    assert "raise SystemExit(0 if not missing else 1)" in script
