from __future__ import annotations

import re
import shlex
import subprocess
from pathlib import Path
from typing import Callable, cast

import pytest
from pytest_bdd import given, parsers, scenarios, then, when

scenarios("../features/tools_cli.feature")
scenarios("../features/runtime_cli.feature")
scenarios("../features/tools_features.feature")
scenarios("../features/runtime_features.feature")


ANSI_ESCAPE_RE = re.compile(r"\x1b\[[0-9;]*m")


def _strip_ansi(text: str) -> str:
    return ANSI_ESCAPE_RE.sub("", text)


# Type aliases
CLIContext = dict[str, subprocess.CompletedProcess[str]]
RunCli = Callable[..., subprocess.CompletedProcess[str]]


@pytest.fixture
def cli_context() -> CLIContext:
    return {}


@given("the project root")
def given_project_root(project_root: Path) -> Path:
    return project_root


@when(parsers.parse('I invoke the "{command}" CLI with arguments "{arguments}"'))
def invoke_cli(
    run_cli: RunCli, cli_context: CLIContext, command: str, arguments: str
) -> None:
    cli_args = shlex.split(arguments) if arguments else []
    result = run_cli(command, *cli_args)
    cli_context["result"] = result


@then(parsers.parse("the command exits with code {expected:d}"))
def assert_exit_code(cli_context: CLIContext, expected: int) -> None:
    result = cast(subprocess.CompletedProcess[str], cli_context.get("result"))
    assert result is not None, "CLI result missing from context"
    assert result.returncode == expected, result.stderr


@then("the output contains:")
def assert_output_contains(cli_context: CLIContext, datatable: list[list[str]]) -> None:
    result = cast(subprocess.CompletedProcess[str], cli_context.get("result"))
    assert result is not None, "CLI result missing from context"
    combined_output = (result.stdout or "") + (result.stderr or "")
    stripped_output = _strip_ansi(combined_output)
    for row in datatable[1:]:
        expected = row[0]
        if expected not in stripped_output:
            preview = stripped_output[:2000]
            raise AssertionError(
                "Missing expected text: "
                f"{expected}\n"
                "----- Stripped output (truncated) -----\n"
                f"{preview}\n"
                "----- End output -----"
            )
