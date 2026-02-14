"""Main CLI entry point for the ML Playground tools system."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Callable, Optional, Protocol, cast
import sys

import click

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
from ml_playground.tools.cli.config_loader import load_config_with_error_handling
from ml_playground.tools.cli.dependencies import (
    ToolsDependencies,
    get_tools_dependencies,
)
from ml_playground.tools.cli.state import GlobalState, apply_cli_options, state
from ml_playground.tools.core.config import ToolsConfig
from ml_playground.tools.environment.environment import EnvironmentTools
from ml_playground.tools.utils.subprocess_utils import RealSubprocessRunner

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Main Typer app and sub-apps
app = typer.Typer(
    name="tools",
    help="ML Playground unified development tools",
    no_args_is_help=False,
    invoke_without_command=True,
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
    def __call__(
        self,
        project_root: Optional[Path] = None,
        *,
        deps: ToolsDependencies | None = None,
    ) -> None: ...


class RunnerHook(Protocol):
    def __call__(self) -> None: ...


# Injectables for tests to avoid monkeypatching. _app_runner defaults to None so
# main_entry uses the current module-level app, respecting test overrides.
_DEFAULT_CONFIG_LOADER: LoaderHook = load_config_with_error_handling
_config_loader: LoaderHook = load_config_with_error_handling
_app_runner: RunnerHook | None = None


def set_cli_hooks(
    *,
    config_loader: LoaderHook | None = None,
    app_runner: RunnerHook | None = None,
) -> None:
    """Configure CLI hook overrides for testing.

    Falls back to defaults when overrides are not provided.
    """

    global _config_loader, _app_runner, load_config_with_error_handling
    load_config_with_error_handling = config_loader or _DEFAULT_CONFIG_LOADER
    _config_loader = load_config_with_error_handling
    # Keep `_app_runner` as None unless explicitly overridden so `main_entry()`
    # uses the current `app` attribute (allowing normal patching and CLI behavior).
    _app_runner = app_runner


@app.callback(invoke_without_command=True)
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
    ctx: click.Context | None = click.get_current_context(silent=True)

    # Early guidance when no subcommand provided.
    if ctx is not None and ctx.invoked_subcommand is None:
        typer.echo("Welcome to ML Playground Tools CLI")
        typer.echo("No tools command was provided. Try `uv run tools test`.")
        help_text = ctx.get_help() if hasattr(ctx, "get_help") else ""
        if help_text:
            typer.echo(help_text)
        raise typer.Exit(code=2)

    # Validate project_root eagerly to provide stable CLI error messages.
    if project_root is not None:
        if not project_root.exists():
            typer.echo(f"Project root not found: {project_root}", err=True)
            raise typer.Exit(2)
        if not project_root.is_dir():
            typer.echo(f"Project root is not a directory: {project_root}", err=True)
            raise typer.Exit(2)

    deps = get_tools_dependencies()
    load_config_with_error_handling(project_root, deps=deps)
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
    _run_main_entry()


def _run_main_entry(*, _repair_attempted: bool = False) -> None:
    try:
        module = sys.modules.get(__name__)
        module_app: object | None = None
        if module is not None:
            module_app = getattr(module, "app", None)

        package_module = sys.modules.get("ml_playground.tools.cli")
        package_app = None
        if package_module is not None:
            package_app = getattr(package_module, "app", None)

        def _is_typer_app(obj: object) -> bool:
            return isinstance(obj, typer.Typer)

        if _app_runner is not None:
            runner = _app_runner
        elif callable(module_app) and not _is_typer_app(module_app):
            # Unit tests patch `ml_playground.tools.cli.main.app`.
            runner = cast(RunnerHook, module_app)
        elif package_app is not None:
            runner = cast(RunnerHook, package_app)
        else:
            runner = cast(RunnerHook, module_app)
        cast(Callable[[], object], runner)()
    except ModuleNotFoundError as exc:
        if not _repair_attempted and exc.name == "ml_playground":
            if _repair_environment():
                _run_main_entry(_repair_attempted=True)
                return
        typer.echo(f"Unexpected error: {exc}", err=True)
        raise typer.Exit(1)
    except KeyboardInterrupt:
        typer.echo("\nOperation cancelled by user", err=True)
        raise typer.Exit(1)
    except typer.Exit:
        raise
    except Exception as exc:
        typer.echo(f"Unexpected error: {exc}", err=True)
        raise typer.Exit(1)


def _repair_environment() -> bool:
    env_tools = EnvironmentTools(
        ToolsConfig(),
        Path.cwd(),
        subprocess_runner=RealSubprocessRunner(),
    )
    setup_result = env_tools.setup([], clear=True)
    if not setup_result.success:
        return False
    verify_result = env_tools.verify([])
    return verify_result.success


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
