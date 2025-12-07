"""Analysis command implementations for the tools CLI system."""

from pathlib import Path

import typer
from typing_extensions import Annotated

from ml_playground.tools.core.interfaces import ToolResult
from ml_playground.tools.cli.helpers import (
    OrderedGroup,
    run_tool_command,
)

# Create analysis app
analysis_app = typer.Typer(
    name="analysis",
    help="Analysis tools (LIT, sample quality)",
    no_args_is_help=True,
    cls=OrderedGroup,
)


@analysis_app.command("lit")
def run_lit(
    host: Annotated[str, typer.Option(help="Host to bind")] = "127.0.0.1",
    port: Annotated[int, typer.Option(help="Port to bind (0 for auto)")] = 5432,
    open_browser: Annotated[bool, typer.Option(help="Open browser on start")] = False,
) -> None:
    """Run LIT server for bundestag_char experiment."""
    from ml_playground.tools.analysis.lit_integration import run_server_bundestag_char
    import logging

    def _run_lit_command():
        logger = logging.getLogger("ml_playground.tools.analysis.lit")
        run_server_bundestag_char(
            host=host,
            port=port,
            open_browser=open_browser,
            logger=logger,
        )
        return ToolResult.create(
            success=True,
            exit_code=0,
            namespace="tools",
            category="analysis",
            command="lit",
            stdout="LIT server stopped",
        )

    run_tool_command(_run_lit_command)


@analysis_app.command("sample-quality")
def sample_quality(
    file_path: Annotated[Path, typer.Argument(help="Path to sample file to analyze")],
) -> None:
    """Analyze quality of a generated sample file."""
    from ml_playground.tools.analysis.sample_quality import (
        analyze_sample_file,
        format_analysis,
    )

    def _analyze_sample_command():
        analysis = analyze_sample_file(file_path)
        output = format_analysis(analysis)
        return ToolResult.create(
            success=True,
            exit_code=0,
            namespace="tools",
            category="analysis",
            command="sample-quality",
            stdout=output,
        )

    run_tool_command(_analyze_sample_command)
