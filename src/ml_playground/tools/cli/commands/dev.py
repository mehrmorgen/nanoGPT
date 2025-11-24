"""Dev command implementations for the tools CLI system."""

from pathlib import Path
from typing import List, Optional

import typer
from typing_extensions import Annotated

from ml_playground.tools.core.errors import (
    ToolExecutionError,
    ToolConfigurationError,
)
from ml_playground.tools.core.interfaces import ToolResult

# Import shared utilities
from ml_playground.tools.cli.helpers import (
    get_dev_tools,
    handle_tool_result,
    OrderedGroup,
)

# Create dev app
dev_app = typer.Typer(
    name="dev",
    help="Development workflow tools (PR management, cleanup utilities)",
    no_args_is_help=True,
    cls=OrderedGroup,
)


@dev_app.command("batch-review")
def dev_batch_review(
    output_format: Annotated[
        str, typer.Option("--format", help="Output format (json, yaml, text)")
    ] = "json",
    args: Annotated[
        Optional[List[str]],
        typer.Argument(help="Additional arguments for batch operations"),
    ] = None,
) -> None:
    """Perform batch review operations for AI consumption."""
    try:
        tools = get_dev_tools()
        result = tools.batch_review(output_format=output_format)
        handle_tool_result(result)
    except (ToolExecutionError, ToolConfigurationError) as e:
        handle_tool_result(
            ToolResult.create(
                success=False,
                exit_code=1,
                namespace="tools",
                category="dev",
                command="batch-review",
                stderr=f"Error performing batch review: {e}",
            )
        )


@dev_app.command("cleanup-ignored-tracked")
def dev_cleanup_ignored_tracked() -> None:
    """Clean up Git-ignored files that are still tracked."""
    try:
        tools = get_dev_tools()
        result = tools.cleanup_ignored_tracked()
        handle_tool_result(result)
    except ToolExecutionError as e:
        typer.echo(f"Error: {e}", err=True)
        raise typer.Exit(1)


@dev_app.command("kill-port")
def dev_kill_port(
    port: Annotated[int, typer.Argument(help="Port number to kill processes on")],
) -> None:
    """Kill processes running on a specific port."""
    try:
        tools = get_dev_tools()
        result = tools.kill_port(port=port)
        handle_tool_result(result)
    except ToolExecutionError as e:
        typer.echo(f"Error: {e}", err=True)
        raise typer.Exit(1)


@dev_app.command("review-bulk-reply")
def dev_review_bulk_reply(
    pr_number: Annotated[int, typer.Argument(help="Pull request number")],
    replies_file: Annotated[
        Path,
        typer.Option(
            "--replies", help="JSON file mapping comment URLs/IDs to reply text"
        ),
    ],
    remote: Annotated[
        str, typer.Option("--remote", help="Git remote name for owner/repo inference")
    ] = "origin",
) -> None:
    """Bulk reply to review threads from a JSON file."""
    try:
        tools = get_dev_tools()
        result = tools.review_bulk_reply(
            pr_number=pr_number,
            replies_file=replies_file,
            remote=remote,
        )
        handle_tool_result(result)
    except (ToolExecutionError, ToolConfigurationError) as e:
        handle_tool_result(
            ToolResult.create(
                success=False,
                exit_code=1,
                namespace="tools",
                category="dev",
                command="review-bulk-reply",
                stderr=f"Error bulk replying to reviews: {e}",
            )
        )


@dev_app.command("review-delete")
def dev_review_delete(
    pr_number: Annotated[int, typer.Argument(help="Pull request number")],
    comments_file: Annotated[
        Path,
        typer.Option(
            "--comments", help="JSON file with list of comment IDs/URLs to delete"
        ),
    ],
    remote: Annotated[
        str, typer.Option("--remote", help="Git remote name for owner/repo inference")
    ] = "origin",
) -> None:
    """Delete review comments from a JSON file."""
    try:
        tools = get_dev_tools()
        result = tools.review_delete(
            pr_number=pr_number,
            comments_file=comments_file,
            remote=remote,
        )
        handle_tool_result(result)
    except (ToolExecutionError, ToolConfigurationError) as e:
        handle_tool_result(
            ToolResult.create(
                success=False,
                exit_code=1,
                namespace="tools",
                category="dev",
                command="review-delete",
                stderr=f"Error deleting review comments: {e}",
            )
        )


@dev_app.command("review-list")
def dev_review_list(
    pr_number: Annotated[int, typer.Argument(help="Pull request number")],
    unreplied: Annotated[
        bool, typer.Option("--unreplied", help="Only show threads without viewer reply")
    ] = False,
    unresolved: Annotated[
        bool, typer.Option("--unresolved", help="Only show unresolved threads")
    ] = False,
    remote: Annotated[
        str, typer.Option("--remote", help="Git remote name for owner/repo inference")
    ] = "origin",
) -> None:
    """List review threads for a pull request."""
    try:
        tools = get_dev_tools()
        result = tools.review_list(
            pr_number=pr_number,
            unreplied=unreplied,
            unresolved=unresolved,
            remote=remote,
        )
        handle_tool_result(result)
    except (ToolExecutionError, ToolConfigurationError) as e:
        handle_tool_result(
            ToolResult.create(
                success=False,
                exit_code=1,
                namespace="tools",
                category="dev",
                command="review-list",
                stderr=f"Error listing review threads: {e}",
            )
        )


@dev_app.command("setup-ai-guidelines")
def dev_setup_ai_guidelines(
    tool: Annotated[
        str, typer.Argument(help="Target tool name (e.g., kiro, cursor, copilot)")
    ],
    dry_run: Annotated[
        bool, typer.Option("--dry-run", help="Preview actions without executing")
    ] = False,
) -> None:
    """Set up AI development guidelines for a specific tool."""
    try:
        tools = get_dev_tools()
        result = tools.setup_ai_guidelines(tool=tool, dry_run=dry_run)
        handle_tool_result(result)
    except (ToolExecutionError, ToolConfigurationError) as e:
        handle_tool_result(
            ToolResult.create(
                success=False,
                exit_code=1,
                namespace="tools",
                category="dev",
                command="setup-ai-guidelines",
                stderr=f"Error setting up AI guidelines: {e}",
            )
        )


@dev_app.command("workflow-status")
def dev_workflow_status(
    output_format: Annotated[
        str, typer.Option("--format", help="Output format (json, yaml, text)")
    ] = "json",
    args: Annotated[
        Optional[List[str]],
        typer.Argument(help="Additional arguments for status checking"),
    ] = None,
) -> None:
    """Get current workflow status for AI decision-making."""
    try:
        tools = get_dev_tools()
        result = tools.workflow_status(output_format=output_format)
        handle_tool_result(result)
    except (ToolExecutionError, ToolConfigurationError) as e:
        handle_tool_result(
            ToolResult.create(
                success=False,
                exit_code=1,
                namespace="tools",
                category="dev",
                command="workflow-status",
                stderr=f"Error getting workflow status: {e}",
            )
        )
