"""Quality command implementations for the tools CLI system."""

from typing import List, Optional

import typer
from typing_extensions import Annotated

# Import shared utilities
from ml_playground.tools.cli.state import state
from ml_playground.tools.cli.helpers import (
    get_quality_tools,
    OrderedGroup,
    run_tool_command,
)

# Create quality app
quality_app = typer.Typer(
    help="Code quality tools (lint, format, typecheck)",
    no_args_is_help=True,
    cls=OrderedGroup,
)


@quality_app.command("all")
def quality_all(
    args: Annotated[
        Optional[List[str]],
        typer.Argument(help="Additional arguments (applied to all tools)"),
    ] = None,
) -> None:
    """Run all quality checks (lint, typecheck, deadcode)."""
    tools = get_quality_tools()
    run_tool_command(
        tools.all_checks,
        args or [],
        learning_mode=state.learning_mode,
        verbosity_level=state.verbosity,
    )


@quality_app.command("basedpyright")
def quality_basedpyright(
    args: Annotated[
        Optional[List[str]], typer.Argument(help="Additional basedpyright arguments")
    ] = None,
) -> None:
    """Run BasedPyright type checks."""
    tools = get_quality_tools()
    run_tool_command(
        tools.basedpyright,
        args or [],
        learning_mode=state.learning_mode,
        verbosity_level=state.verbosity,
    )


@quality_app.command("deadcode")
def quality_deadcode(
    args: Annotated[
        Optional[List[str]], typer.Argument(help="Additional vulture arguments")
    ] = None,
) -> None:
    """Scan for dead code using vulture."""
    tools = get_quality_tools()
    run_tool_command(
        tools.deadcode,
        args or [],
        learning_mode=state.learning_mode,
        verbosity_level=state.verbosity,
    )


@quality_app.command("format")
def quality_format(
    args: Annotated[
        Optional[List[str]], typer.Argument(help="Additional ruff arguments")
    ] = None,
) -> None:
    """Auto-fix and format code with Ruff."""
    tools = get_quality_tools()
    run_tool_command(
        tools.format,
        args or [],
        learning_mode=state.learning_mode,
        verbosity_level=state.verbosity,
    )


@quality_app.command("lint")
def quality_lint(
    args: Annotated[
        Optional[List[str]], typer.Argument(help="Additional ruff arguments")
    ] = None,
) -> None:
    """Run Ruff lint checks."""
    tools = get_quality_tools()
    run_tool_command(
        tools.lint,
        args or [],
        learning_mode=state.learning_mode,
        verbosity_level=state.verbosity,
    )


@quality_app.command("mypy")
def quality_mypy(
    args: Annotated[
        Optional[List[str]], typer.Argument(help="Additional mypy arguments")
    ] = None,
) -> None:
    """Run Mypy type checks."""
    tools = get_quality_tools()
    run_tool_command(
        tools.mypy,
        args or [],
        learning_mode=state.learning_mode,
        verbosity_level=state.verbosity,
    )


@quality_app.command("typecheck")
def quality_typecheck(
    args: Annotated[
        Optional[List[str]],
        typer.Argument(help="Additional arguments (applied to both tools)"),
    ] = None,
) -> None:
    """Run both BasedPyright and Mypy type checks."""
    tools = get_quality_tools()
    run_tool_command(
        tools.typecheck,
        args or [],
        learning_mode=state.learning_mode,
        verbosity_level=state.verbosity,
    )
