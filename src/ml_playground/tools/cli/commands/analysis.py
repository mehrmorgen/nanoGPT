from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, Callable

import typer
from typing_extensions import Annotated

from ml_playground.tools.analysis.lit_integration import (
    run_server_experiment as run_lit_server,
)
from ml_playground.tools.cli.helpers import handle_tool_result
from ml_playground.framework.analysis.sample_quality_public import (
    analyze_sample_file,
    format_analysis,
)
from ml_playground.tools.core.interfaces import ToolResult
from ml_playground.framework.core.project_config import get_default_host

LitFn = Callable[..., Any]


def _get_default_host() -> str:
    try:
        return get_default_host()
    except (ValueError, TypeError):
        return "127.0.0.1"


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
    if not file_path.exists():
        typer.echo(f"Error: sample file not found: {file_path}", err=True)
        raise typer.Exit(2)
    if not file_path.is_file():
        typer.echo(f"Error: sample path is not a file: {file_path}", err=True)
        raise typer.Exit(2)

    try:
        analysis = analyze_sample_file(file_path)
        report = format_analysis(analysis)
        handle_tool_result(
            ToolResult.create(
                success=True,
                exit_code=0,
                namespace="tools",
                category="analysis",
                command="sample-quality",
                stdout=report,
            )
        )
    except Exception as exc:
        typer.echo(f"Error: {exc}", err=True)
        typer.echo(
            "Rationale: Failed to analyze sample file quality. Ensure the file exists and is in a supported format.",
            err=True,
        )
        raise typer.Exit(1)


@app.command("lit")
def analysis_lit(
    host: Annotated[
        str, typer.Option(default_factory=_get_default_host, help="Host to bind")
    ],
    port: Annotated[int, typer.Option(help="Port to bind (0 for auto)")] = 5432,
    open_browser: Annotated[
        bool, typer.Option("--open-browser", help="Open browser on start")
    ] = False,
    experiment: Annotated[
        str, typer.Option(help="Experiment name owning the LIT integration")
    ] = "bundestag_char",
) -> None:
    """Launch a LIT server for interactive dataset/model analysis."""
    try:
        import sys
        import importlib
        from ml_playground.framework.core.logging_protocol import LoggerLike
        from typing import cast

        # Allow tests to monkeypatch at the package root (ml_playground) or this module.
        analysis_mod = importlib.import_module(
            "ml_playground.tools.cli.commands.analysis"
        )
        fallback_pkg = sys.modules.get("ml_playground")
        lit_fn_candidate = (
            getattr(fallback_pkg, "run_lit_server", None) if fallback_pkg else None
        )
        lit_fn: LitFn = cast(
            LitFn,
            lit_fn_candidate or getattr(analysis_mod, "run_lit_server", run_lit_server),
        )

        try:
            lit_fn(
                experiment=experiment,
                host=host,
                port=port,
                open_browser=open_browser,
                logger=cast(LoggerLike, logger),
            )
        except TypeError:
            # Keep compatibility with legacy fakes that omit logger and/or experiment.
            try:
                lit_fn(
                    experiment=experiment,
                    host=host,
                    port=port,
                    open_browser=open_browser,
                )
            except TypeError:
                try:
                    lit_fn(
                        host=host,
                        port=port,
                        open_browser=open_browser,
                        logger=cast(LoggerLike, logger),
                    )
                except TypeError:
                    lit_fn(host=host, port=port, open_browser=open_browser)

        handle_tool_result(
            ToolResult.create(
                success=True,
                exit_code=0,
                namespace="tools",
                category="analysis",
                command="lit",
                stdout="LIT server stopped",
            )
        )
    except Exception as exc:
        typer.echo(f"Error: {exc}", err=True)
        typer.echo(
            "Rationale: Failed to launch LIT server on {host}:{port}. Check if the port is already in use or if LIT dependencies are correctly installed.",
            err=True,
        )
        raise typer.Exit(1)


def build_app() -> typer.Typer:
    return app
