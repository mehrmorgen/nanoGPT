from __future__ import annotations

from pathlib import Path
from typing import Optional

import typer
from typing_extensions import Annotated

from ml_playground.tools.core.errors import ToolExecutionError

from .. import helpers as cli_helpers
from ..state import state

app = typer.Typer(
    name="agentic",
    help="AI-assisted development tools (workflows, batch operations)",
    no_args_is_help=True,
)


@app.command("guidelines-setup")
def agentic_guidelines_setup(
    args: Annotated[
        Optional[list[str]],
        typer.Argument(help="Additional arguments for guideline setup"),
    ] = None,
) -> None:
    """Set up AI development guidelines and configuration."""
    try:
        tools = cli_helpers.get_agentic_tools()
        result = tools.guidelines_setup(
            args or [],
            learning_mode=state.learning_mode,
            verbosity_level=state.verbosity,
        )
        cli_helpers.handle_tool_result(result)
    except ToolExecutionError as exc:
        typer.echo(f"Error: {exc}", err=True)
        raise typer.Exit(1)


@app.command("batch-review")
def agentic_batch_review(
    output_format: Annotated[
        str, typer.Option("--format", help="Output format (json, yaml, text)")
    ] = "json",
    args: Annotated[
        Optional[list[str]],
        typer.Argument(help="Additional arguments for batch operations"),
    ] = None,
) -> None:
    """Perform batch review operations for AI consumption."""
    try:
        tools = cli_helpers.get_agentic_tools()
        result = tools.batch_review(
            args or [],
            output_format=output_format,
            learning_mode=state.learning_mode,
            verbosity_level=state.verbosity,
        )
        cli_helpers.handle_tool_result(result)
    except ToolExecutionError as exc:
        typer.echo(f"Error: {exc}", err=True)
        raise typer.Exit(1)


@app.command("workflow-helper")
def agentic_workflow_helper(
    workflow_type: Annotated[
        str, typer.Option("--type", help="Workflow type (standard, strict, minimal)")
    ] = "standard",
    args: Annotated[
        Optional[list[str]],
        typer.Argument(help="Additional arguments for workflow generation"),
    ] = None,
) -> None:
    """Provide workflow helpers for common AI development patterns."""
    try:
        tools = cli_helpers.get_agentic_tools()
        result = tools.workflow_helper(
            args or [],
            workflow_type=workflow_type,
            learning_mode=state.learning_mode,
            verbosity_level=state.verbosity,
        )
        cli_helpers.handle_tool_result(result)
    except ToolExecutionError as exc:
        typer.echo(f"Error: {exc}", err=True)
        raise typer.Exit(1)


@app.command("batch-quality")
def agentic_batch_quality(
    output_format: Annotated[
        str, typer.Option("--format", help="Output format (json, yaml, text)")
    ] = "json",
    args: Annotated[
        Optional[list[str]],
        typer.Argument(help="Additional arguments for quality checks"),
    ] = None,
) -> None:
    """Run automated quality checks for AI agent consumption."""
    try:
        tools = cli_helpers.get_agentic_tools()
        result = tools.batch_quality(
            args or [],
            output_format=output_format,
            learning_mode=state.learning_mode,
            verbosity_level=state.verbosity,
        )
        cli_helpers.handle_tool_result(result)
    except ToolExecutionError as exc:
        typer.echo(f"Error: {exc}", err=True)
        raise typer.Exit(1)


@app.command("batch-validate")
def agentic_batch_validate(
    validation_level: Annotated[
        str,
        typer.Option("--level", help="Validation level (minimal, standard, strict)"),
    ] = "standard",
    output_format: Annotated[
        str, typer.Option("--format", help="Output format (json, yaml, text)")
    ] = "json",
    args: Annotated[
        Optional[list[str]], typer.Argument(help="Additional arguments for validation")
    ] = None,
) -> None:
    """Run comprehensive validation for AI-assisted development."""
    try:
        tools = cli_helpers.get_agentic_tools()
        result = tools.batch_validate(
            args or [],
            validation_level=validation_level,
            output_format=output_format,
            learning_mode=state.learning_mode,
            verbosity_level=state.verbosity,
        )
        cli_helpers.handle_tool_result(result)
    except ToolExecutionError as exc:
        typer.echo(f"Error: {exc}", err=True)
        raise typer.Exit(1)


@app.command("workflow-status")
def agentic_workflow_status(
    output_format: Annotated[
        str, typer.Option("--format", help="Output format (json, yaml, text)")
    ] = "json",
    args: Annotated[
        Optional[list[str]],
        typer.Argument(help="Additional arguments for status checking"),
    ] = None,
) -> None:
    """Get current workflow status for AI decision-making."""
    try:
        tools = cli_helpers.get_agentic_tools()
        result = tools.workflow_status(
            args or [],
            output_format=output_format,
            learning_mode=state.learning_mode,
            verbosity_level=state.verbosity,
        )
        cli_helpers.handle_tool_result(result)
    except ToolExecutionError as exc:
        typer.echo(f"Error: {exc}", err=True)
        raise typer.Exit(1)


@app.command("scrape-chat-share")
def agentic_scrape_chat_share(
    url: Annotated[str, typer.Argument(help="Shared ChatGPT conversation URL")],
    output: Annotated[
        Optional[Path],
        typer.Option(
            "--output",
            "-o",
            help="Write markdown output to file instead of stdout",
        ),
    ] = None,
    timeout: Annotated[
        float,
        typer.Option("--timeout", help="HTTP timeout in seconds"),
    ] = 15.0,
) -> None:
    """Scrape a shared ChatGPT conversation and emit Markdown."""
    try:
        tools = cli_helpers.get_agentic_tools()
        result = tools.scrape_chat_share(
            url,
            output_path=output,
            timeout=timeout,
            learning_mode=state.learning_mode,
            verbosity_level=state.verbosity,
        )
        cli_helpers.handle_tool_result(result)
    except ToolExecutionError as exc:
        typer.echo(f"Error: {exc}", err=True)
        raise typer.Exit(1)


@app.command("website-to-markdown")
def agentic_website_to_markdown(
    url: Annotated[str, typer.Argument(help="Website URL to render and convert")],
    output: Annotated[
        Optional[Path],
        typer.Option(
            "--output",
            "-o",
            help="Write markdown output to file instead of stdout",
        ),
    ] = None,
    wait_until: Annotated[
        str,
        typer.Option(
            "--wait-until",
            help="Playwright wait condition (load, domcontentloaded, networkidle, commit)",
        ),
    ] = "networkidle",
    timeout_ms: Annotated[
        int,
        typer.Option(
            "--timeout-ms",
            help="Navigation timeout in milliseconds",
        ),
    ] = 30_000,
    selector: Annotated[
        Optional[str],
        typer.Option(
            "--selector",
            help="Optional CSS selector to wait for before capture",
        ),
    ] = None,
) -> None:
    """Render a dynamic website via Playwright and emit Markdown."""
    try:
        tools = cli_helpers.get_agentic_tools()
        result = tools.website_to_markdown(
            url,
            output_path=output,
            wait_until=wait_until,
            timeout_ms=timeout_ms,
            selector=selector,
            learning_mode=state.learning_mode,
            verbosity_level=state.verbosity,
        )
        cli_helpers.handle_tool_result(result)
    except ToolExecutionError as exc:
        typer.echo(f"Error: {exc}", err=True)
        raise typer.Exit(1)


def build_app() -> typer.Typer:
    return app
