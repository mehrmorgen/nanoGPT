"""Main CLI entry point for the ML Playground tools system.

This module provides the unified CLI accessible via `uv run tools`, organizing
all development tools under logical subcommands with learning mode support.
"""

import logging
import os
from pathlib import Path
from typing import Optional

import typer
from typing_extensions import Annotated

from ml_playground.tools.core.errors import (
    ToolExecutionError,
    ToolConfigurationError,
)


# Import state and dependencies from separate modules
from ml_playground.tools.cli.state import state
from ml_playground.tools.cli.dependencies import (
    get_tools_dependencies,
)
from ml_playground.tools.cli.config_loader import (
    load_config_with_error_handling,
    ensure_config_loaded,
)

# Import command modules
from ml_playground.tools.cli.commands.quality import quality_app
from ml_playground.tools.cli.commands.testing import test_app
from ml_playground.tools.cli.commands.environment import env_app
from ml_playground.tools.cli.commands.ci import ci_app
from ml_playground.tools.cli.commands.dev import dev_app

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Main Typer app
app = typer.Typer(
    name="tools",
    help="ML Playground unified development tools",
    no_args_is_help=False,
    rich_markup_mode="rich",
)


learn_app = typer.Typer(
    name="learn",
    help="Learning mode utilities and educational content",
    no_args_is_help=True,
)

# Add subcommands to main app
app.add_typer(quality_app, name="quality")
app.add_typer(test_app, name="test")
app.add_typer(env_app, name="env")
app.add_typer(ci_app, name="ci")
app.add_typer(dev_app, name="dev")
app.add_typer(learn_app, name="learn")


@app.callback()
def main(
    learning_mode: Annotated[
        Optional[bool],
        typer.Option(
            "--learning-mode/--no-learning-mode",
            help="Enable learning mode to show underlying commands and explanations",
        ),
    ] = None,
    verbosity: Annotated[
        Optional[int],
        typer.Option(
            "--verbosity",
            "-v",
            min=0,
            max=2,
            help="Learning mode verbosity: 0=minimal, 1=standard, 2=comprehensive",
        ),
    ] = None,
    dry_run: Annotated[
        bool,
        typer.Option(
            "--dry-run", help="Show what would be done without executing commands"
        ),
    ] = False,
    project_root: Annotated[
        Optional[Path],
        typer.Option(
            "--project-root",
            help="Path to project root (auto-detected if not specified)",
        ),
    ] = None,
) -> None:
    """ML Playground unified development tools.

    Provides a single entry point for all development tooling including
    quality checks, testing, environment management, CI operations, and
    AI-assisted development workflows.

    Use --learning-mode to see underlying commands and educational explanations.
    """
    # Load configuration first (unit tests expect this to happen at entry)
    deps = get_tools_dependencies()
    load_config_with_error_handling(project_root, deps=deps)

    # Set global options, preferring explicit CLI args over config defaults
    if learning_mode is not None:
        state.learning_mode = learning_mode
        state.mark_learning_mode_explicit(True)

    if verbosity is not None:
        state.verbosity = verbosity

    state.dry_run = dry_run
    if state.dry_run:
        os.environ["ML_PLAYGROUND_TOOLS_DRY_RUN"] = "1"
    else:
        os.environ.pop("ML_PLAYGROUND_TOOLS_DRY_RUN", None)


@app.command("version")
def version() -> None:
    """Show version information."""
    typer.echo("ML Playground Tools v0.1.0")
    typer.echo("Unified development tooling for ML Playground")


@app.command("config")
def show_config() -> None:
    """Show current configuration."""
    ensure_config_loaded()
    assert state.config is not None, (
        "Config should be loaded after _ensure_config_loaded"
    )

    typer.echo("Current tools configuration:")
    typer.echo(f"  Learning mode default: {state.config.learning_mode_default}")
    typer.echo(f"  Default verbosity: {state.config.default_verbosity}")
    typer.echo(f"  Project root: {state.project_root or 'auto-detected'}")
    typer.echo("")
    typer.echo("Tool categories:")
    typer.echo(
        f"  Quality tools: {'enabled' if state.config.quality.enabled else 'disabled'}"
    )
    typer.echo(
        f"  Testing tools: {'enabled' if state.config.testing.enabled else 'disabled'}"
    )
    typer.echo(
        f"  Environment tools: {'enabled' if state.config.environment.enabled else 'disabled'}"
    )
    typer.echo(f"  CI tools: {'enabled' if state.config.ci.enabled else 'disabled'}")


def main_entry() -> None:
    """Main entry point for the tools CLI."""
    try:
        app()
    except KeyboardInterrupt:
        typer.echo("\nOperation cancelled by user", err=True)
        raise typer.Exit(1)
    except typer.Exit:
        # Let Typer exit codes propagate properly
        raise
    except (ToolExecutionError, ToolConfigurationError) as e:
        typer.echo(f"Tool error: {e}", err=True)
        raise typer.Exit(1)
    except Exception as e:
        # Fallback for truly unexpected errors
        typer.echo(f"Unexpected error: {e}", err=True)
        raise typer.Exit(1)


if __name__ == "__main__":
    main_entry()
