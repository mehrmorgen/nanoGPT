"""CI command implementations for the tools CLI system."""

from typing import List, Optional

import typer
from typing_extensions import Annotated

from ml_playground.tools.cli.helpers import (
    get_ci_tools,
    OrderedGroup,
    run_tool_command,
)

# Create CI app
ci_app = typer.Typer(
    name="ci",
    help="CI/CD operations (quality gates, badges)",
    no_args_is_help=True,
    cls=OrderedGroup,
)


@ci_app.command("coverage-badge")
def ci_coverage_badge(
    args: Annotated[
        Optional[List[str]], typer.Argument(help="Additional arguments (ignored)")
    ] = None,
) -> None:
    """Regenerate the SVG coverage badges."""
    tools = get_ci_tools()
    run_tool_command(
        tools.coverage_badge,
        args or [],
    )


@ci_app.command("quality-ci-local")
def ci_quality_ci_local(
    bind_caches: Annotated[
        bool,
        typer.Option(
            "--bind-caches/--no-bind-caches",
            help="Bind local caches into the act container",
        ),
    ] = True,
    args: Annotated[
        Optional[List[str]], typer.Argument(help="Additional act arguments")
    ] = None,
) -> None:
    """Run GitHub Actions workflow locally using act."""
    tools = get_ci_tools()
    run_tool_command(
        tools.quality_ci_local,
        bind_caches=bind_caches,
        args=args or [],
    )


@ci_app.command("quality-ext")
def ci_quality_ext(
    args: Annotated[
        Optional[List[str]], typer.Argument(help="Additional arguments (ignored)")
    ] = None,
) -> None:
    """Run extended quality gates (mutation testing moved to testing tools)."""
    tools = get_ci_tools()
    run_tool_command(
        tools.quality_ext,
        args or [],
    )


@ci_app.command("quality-fast")
def ci_quality_fast(
    args: Annotated[
        Optional[List[str]], typer.Argument(help="Additional pre-commit arguments")
    ] = None,
) -> None:
    """Run lint/format focused pre-commit hooks."""
    tools = get_ci_tools()
    run_tool_command(
        tools.quality_fast,
        args or [],
    )


@ci_app.command("quality-gate")
def ci_quality_gate(
    args: Annotated[
        Optional[List[str]], typer.Argument(help="Additional pre-commit arguments")
    ] = None,
) -> None:
    """Run the full pre-commit quality gate."""
    tools = get_ci_tools()
    run_tool_command(
        tools.quality_gate,
        args or [],
    )
