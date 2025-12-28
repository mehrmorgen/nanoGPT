"""Main CLI entry point for the ML Playground tools system."""

from __future__ import annotations

import logging
import sys
from pathlib import Path
from typing import Optional, Protocol

import typer
from typing_extensions import Annotated

from ml_playground.tools.cli.commands import (
    agentic,
    analysis,
    ci,
    dev,
    env,
    learn,
    quality,
    test,
)
from ml_playground.tools.cli.state import (
    GlobalState,
    apply_cli_options,
    load_config_with_error_handling,
    state,
)

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Main Typer app and sub-apps
app = typer.Typer(
    name="tools",
    help="ML Playground unified development tools",
    no_args_is_help=False,
    rich_markup_mode="rich",
)

quality_app = quality.app
test_app = test.app
env_app = env.app
ci_app = ci.app
agentic_app = agentic.app
analysis_app = analysis.app
dev_app = dev.app
learn_app = learn.app

app.add_typer(quality_app, name="quality")
app.add_typer(test_app, name="test")
app.add_typer(env_app, name="env")
app.add_typer(ci_app, name="ci")
app.add_typer(agentic_app, name="agentic")
app.add_typer(analysis_app, name="analysis")
app.add_typer(dev_app, name="dev")
app.add_typer(learn_app, name="learn")


class LoaderHook(Protocol):
    def __call__(self, project_root: Optional[Path] = None) -> None: ...


class RunnerHook(Protocol):
    def __call__(self) -> None: ...


# Injectables for tests to avoid monkeypatching
_config_loader: LoaderHook = load_config_with_error_handling
_app_runner: RunnerHook = app


def set_cli_hooks(
    *,
    config_loader: LoaderHook | None = None,
    app_runner: RunnerHook | None = None,
) -> None:
    """Configure CLI hook overrides for testing.

    Falls back to defaults when overrides are not provided.
    """

    global _config_loader, _app_runner
    _config_loader = config_loader or load_config_with_error_handling
    _app_runner = app_runner or app


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
    """ML Playground unified development tools entry."""
    _config_loader(project_root)
    apply_cli_options(learning_mode, verbosity, dry_run)


@app.command("version")
def version() -> None:
    """Show version information."""
    typer.echo("ML Playground Tools v0.1.0")
    typer.echo("Unified development tooling for ML Playground")


@app.command("config")
def show_config() -> None:
    """Show current configuration."""
    if state.config is None:
        _config_loader()

    if state.config is None:
        typer.echo("Configuration not loaded", err=True)
        raise typer.Exit(1)

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
    typer.echo(
        f"  Agentic tools: {'enabled' if state.config.agentic.enabled else 'disabled'}"
    )
    typer.echo("  Analysis tools: enabled")


def main_entry() -> None:
    """Main entry point for the tools CLI."""
    try:
        _app_runner()
    except KeyboardInterrupt:
        typer.echo("\nOperation cancelled by user", err=True)
        sys.exit(1)
    except Exception as exc:
        typer.echo(f"Unexpected error: {exc}", err=True)
        sys.exit(1)


__all__ = [
    "app",
    "main",
    "main_entry",
    "set_cli_hooks",
    "state",
    "GlobalState",
    "quality_app",
    "test_app",
    "env_app",
    "ci_app",
    "analysis_app",
    "agentic_app",
    "dev_app",
    "learn_app",
]


if __name__ == "__main__":
    main_entry()
