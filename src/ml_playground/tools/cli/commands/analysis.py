from __future__ import annotations

import logging
from pathlib import Path

import typer
from typing_extensions import Annotated

from ml_playground.tools.analysis.lit_integration import (
    run_server_bundestag_char as run_lit_server,
)
from ml_playground.tools.analysis.sample_quality_public import (
    analyze_sample_file,
    format_analysis,
)

app = typer.Typer(
    name="analysis",
    help="Dataset and sample analysis tools (LIT, quality metrics)",
    no_args_is_help=True,
)

logger = logging.getLogger(__name__)


@app.command("sample-quality")
def analysis_sample_quality(
    file_path: Annotated[
        Path, typer.Argument(help="Path to the sample file to analyze")
    ],
) -> None:
    """Analyze the quality of a generated sample file."""
    try:
        analysis = analyze_sample_file(file_path)
        report = format_analysis(analysis)
        typer.echo(report)
    except Exception as exc:
        typer.echo(f"Error: {exc}", err=True)
        raise typer.Exit(1)


@app.command("lit")
def analysis_lit(
    host: Annotated[str, typer.Option(help="Host to bind")] = "127.0.0.1",
    port: Annotated[int, typer.Option(help="Port to bind (0 for auto)")] = 5432,
    open_browser: Annotated[
        bool, typer.Option("--open-browser", help="Open browser on start")
    ] = False,
) -> None:
    """Launch a LIT server for interactive dataset/model analysis."""
    try:
        from ml_playground.core.logging_protocol import LoggerLike
        from typing import cast

        run_lit_server(
            host=host,
            port=port,
            open_browser=open_browser,
            logger=cast(LoggerLike, logger),
        )
    except Exception as exc:
        typer.echo(f"Error: {exc}", err=True)
        raise typer.Exit(1)


def build_app() -> typer.Typer:
    return app
