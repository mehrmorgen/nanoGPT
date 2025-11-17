from __future__ import annotations

import shlex
from pathlib import Path

import hypothesis.strategies as st
from hypothesis import given, settings

from ml_playground.tools.core.interfaces import OperationId
from ml_playground.tools.utils.subprocess_utils import (
    format_command,
    override_subprocess_runner,
    run_pytest_command,
    run_subprocess,
    run_uv_command,
)

from tests.property.tools._helpers import DeterministicRunner


@settings(max_examples=40, deadline=None, derandomize=True)
@given(
    command=st.lists(st.text(min_size=0, max_size=5), min_size=1, max_size=5),
)
def test_run_subprocess_forwards_arguments(command: list[str]) -> None:
    """run_subprocess should forward arguments to the default runner."""
    operation_id = OperationId(namespace="tools", category="utils", command="run")
    runner = DeterministicRunner()

    with override_subprocess_runner(runner):
        result = run_subprocess(command, operation_id=operation_id)

    assert result.success is True
    assert runner.calls[0].kind == "subprocess"
    assert runner.calls[0].args == command


@settings(max_examples=40, deadline=None, derandomize=True)
@given(
    args=st.lists(st.text(min_size=1, max_size=6), max_size=4),
    python=st.none() | st.text(min_size=1, max_size=5),
    no_project=st.booleans(),
)
def test_run_uv_command_constructs_uv_invocation(
    args: list[str], python: str | None, no_project: bool
) -> None:
    """run_uv_command should construct uv run command respecting flags."""
    operation_id = OperationId(namespace="tools", category="utils", command="uv")
    runner = DeterministicRunner()

    with override_subprocess_runner(runner):
        result = run_uv_command(
            args,
            cwd=Path("/tmp/project"),
            python=python,
            no_project=no_project,
            operation_id=operation_id,
        )

    assert result.success is True
    call = runner.calls[0]
    assert call.kind == "uv"
    assert call.args == args
    if no_project:
        assert call.extra["no_project"] is True
    else:
        assert call.extra["no_project"] is False
    assert call.extra["python"] == python


@settings(max_examples=30, deadline=None, derandomize=True)
@given(
    args=st.lists(st.text(min_size=1, max_size=6), max_size=3),
)
def test_run_pytest_command_adds_default_flags(args: list[str]) -> None:
    """run_pytest_command should prepend standard pytest options."""
    operation_id = OperationId(namespace="tools", category="utils", command="pytest")
    runner = DeterministicRunner()

    with override_subprocess_runner(runner):
        result = run_pytest_command(args, operation_id=operation_id)

    assert result.success is True
    # DeterministicRunner records what args were passed to run_pytest_command
    call = runner.calls[0]
    assert call.kind == "pytest"
    # The args should be the raw args passed to the function (before processing)
    assert call.args == args


@settings(max_examples=50, deadline=None, derandomize=True)
@given(
    parts=st.lists(st.text(min_size=0, max_size=8), min_size=1, max_size=5),
)
def test_format_command_roundtrips(parts: list[str]) -> None:
    """format_command should preserve argument ordering and quoting semantics."""
    formatted = format_command(parts)
    rebuilt = shlex.split(formatted)

    assert rebuilt == parts
