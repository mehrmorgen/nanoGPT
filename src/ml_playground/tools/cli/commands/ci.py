"""CI command implementations for the tools CLI system."""

from typing import List, Optional

import typer
from typing_extensions import Annotated

from ml_playground.tools.core.interfaces import ToolResult
from ml_playground.tools.core.errors import (
    ToolExecutionError,
    ToolConfigurationError,
)
from ml_playground.tools.cli.helpers import (
    get_ci_tools,
    handle_tool_result,
    OrderedGroup,
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
    try:
        tools = get_ci_tools()
        result = tools.coverage_badge(args or [])
        handle_tool_result(result)
    except (ToolExecutionError, ToolConfigurationError) as e:
        handle_tool_result(
            ToolResult.create(
                success=False,
                exit_code=1,
                namespace="tools",
                category="ci",
                command="coverage-badge",
                stderr=f"Error generating coverage badge: {e}",
            )
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
    try:
        tools = get_ci_tools()
        result = tools.quality_ci_local(bind_caches=bind_caches, args=args or [])
        handle_tool_result(result)
    except (ToolExecutionError, ToolConfigurationError) as e:
        handle_tool_result(
            ToolResult.create(
                success=False,
                exit_code=1,
                namespace="tools",
                category="ci",
                command="quality-ci-local",
                stderr=f"Error running quality ci local: {e}",
            )
        )


@ci_app.command("quality-ext")
def ci_quality_ext(
    args: Annotated[
        Optional[List[str]], typer.Argument(help="Additional arguments (ignored)")
    ] = None,
) -> None:
    """Run extended quality gates (mutation testing moved to testing tools)."""
    try:
        tools = get_ci_tools()
        result = tools.quality_ext(args or [])
        handle_tool_result(result)
    except (ToolExecutionError, ToolConfigurationError) as e:
        handle_tool_result(
            ToolResult.create(
                success=False,
                exit_code=1,
                namespace="tools",
                category="ci",
                command="quality-ext",
                stderr=f"Error running quality ext: {e}",
            )
        )


@ci_app.command("quality-fast")
def ci_quality_fast(
    args: Annotated[
        Optional[List[str]], typer.Argument(help="Additional pre-commit arguments")
    ] = None,
) -> None:
    """Run lint/format focused pre-commit hooks."""
    try:
        tools = get_ci_tools()
        result = tools.quality_fast(args or [])
        handle_tool_result(result)
    except (ToolExecutionError, ToolConfigurationError) as e:
        handle_tool_result(
            ToolResult.create(
                success=False,
                exit_code=1,
                namespace="tools",
                category="ci",
                command="quality-fast",
                stderr=f"Error running quality fast: {e}",
            )
        )


@ci_app.command("quality-gate")
def ci_quality_gate(
    args: Annotated[
        Optional[List[str]], typer.Argument(help="Additional pre-commit arguments")
    ] = None,
) -> None:
    """Run the full pre-commit quality gate."""
    try:
        tools = get_ci_tools()
        result = tools.quality_gate(args or [])
        handle_tool_result(result)
    except ToolExecutionError as e:
        handle_tool_result(
            ToolResult.create(
                success=False,
                exit_code=1,
                namespace="tools",
                category="ci",
                command="quality-gate",
                stderr=str(e),
            )
        )
