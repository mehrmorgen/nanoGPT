from __future__ import annotations

import shlex
from subprocess import CompletedProcess
from typing import Callable, Dict, Sequence, cast

import pytest
from pytest_bdd import given, scenarios, then, when, parsers


scenarios("../features/tools_cli.feature")
scenarios("../features/runtime_cli.feature")


@pytest.fixture()
def cli_context() -> Dict[str, object]:
    return {}


@given("the project root")
def given_project_root(project_root: object) -> object:  # type: ignore[no-untyped-def]
    return project_root


@when(parsers.parse('I invoke the "{command}" CLI with arguments "{arguments}"'))
def invoke_cli(
    run_cli: object, cli_context: Dict[str, object], command: str, arguments: str
) -> None:
    runner = cast(
        Callable[..., CompletedProcess[str]],
        run_cli,
    )
    cli_args = shlex.split(arguments) if arguments else []
    result = runner(command, *cli_args)
    cli_context["result"] = result


@then(parsers.parse("the command exits with code {expected:d}"))
def assert_exit_code(cli_context: Dict[str, object], expected: int) -> None:
    result = cast(CompletedProcess[str] | None, cli_context.get("result"))
    assert result is not None, "CLI result missing from context"
    assert result.returncode == expected, result.stderr


@then("the output contains:")
def assert_output_contains(
    cli_context: Dict[str, object], datatable: Sequence[Sequence[str]]
) -> None:
    result = cast(CompletedProcess[str] | None, cli_context.get("result"))
    assert result is not None, "CLI result missing from context"
    combined_output: str = (result.stdout or "") + (result.stderr or "")
    for row in datatable[1:]:
        expected: str = row[0]
        assert expected in combined_output, f"Missing expected text: {expected}"
