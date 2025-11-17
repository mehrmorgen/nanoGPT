"""Main CLI entry point for the ML Playground tools system.

This module provides the unified CLI accessible via `uv run tools`, organizing
all development tools under logical subcommands with learning mode support.
"""

import logging
import os
import sys
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

import typer
from typing_extensions import Annotated

from ml_playground.tools.ci.ci import CITools
from ml_playground.tools.dev.dev import DevTools
from ml_playground.tools.environment.environment import EnvironmentTools
from ml_playground.tools.quality.quality import QualityTools
from ml_playground.tools.testing.testing import TestingTools
from ml_playground.tools.core.config import load_tools_config, ToolsConfig
from ml_playground.tools.core.errors import ToolConfigurationError, ToolExecutionError
from ml_playground.tools.core.interfaces import ToolResult
from ml_playground.tools.core.learning_mode import VerbosityLevel

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

# Subcommand groups (will be populated in later phases)
quality_app = typer.Typer(
    help="Code quality tools (lint, format, typecheck)",
    no_args_is_help=True,
)

test_app = typer.Typer(
    name="test",
    help="Testing tools (unit, integration, e2e, coverage)",
    no_args_is_help=True,
)

env_app = typer.Typer(
    name="env",
    help="Environment management tools (setup, sync, clean)",
    no_args_is_help=True,
)

ci_app = typer.Typer(
    name="ci",
    help="CI/CD operations (quality gates, badges)",
    no_args_is_help=True,
)

dev_app = typer.Typer(
    name="dev",
    help="Development workflow tools (PR management, cleanup utilities)",
    no_args_is_help=True,
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


# Global options and state
class GlobalState:
    """Global state for CLI options."""

    def __init__(self):
        self.learning_mode: bool = False
        self.verbosity: int = 1
        self.dry_run: bool = False
        self.project_root: Optional[Path] = None
        self.config: Optional[ToolsConfig] = None
        self.learning_mode_set: bool = False

    def mark_learning_mode_explicit(self, value: bool = True) -> None:
        """Record that learning mode was explicitly configured."""
        self.learning_mode_set = value

    def mark_learning_mode_default(self, value: bool = True) -> None:
        """Record that learning mode default was applied from configuration."""
        self.learning_mode_set = value


# Global state instance
state = GlobalState()


def default_tool_result_handler(result: ToolResult) -> None:
    if result.stdout:
        typer.echo(result.stdout)
    if result.stderr:
        typer.echo(result.stderr, err=True)
    if not result.success:
        raise typer.Exit(result.exit_code)


@dataclass(slots=True)
class ToolsDependencies:
    load_config: Callable[[Path | None], ToolsConfig]
    quality_factory: Callable[[ToolsConfig, Path], QualityTools]
    testing_factory: Callable[[ToolsConfig, Path], TestingTools]
    environment_factory: Callable[[ToolsConfig, Path], EnvironmentTools]
    ci_factory: Callable[[ToolsConfig, Path], CITools]
    dev_factory: Callable[[ToolsConfig], DevTools]
    result_handler: Callable[[ToolResult], None]


def default_tools_dependencies() -> ToolsDependencies:
    def _load_config(project_root: Path | None = None) -> ToolsConfig:
        return load_tools_config(project_root)

    def _quality_factory(config: ToolsConfig, project_root: Path) -> QualityTools:
        return QualityTools(config, project_root)

    def _testing_factory(config: ToolsConfig, project_root: Path) -> TestingTools:
        return TestingTools(config, project_root)

    def _environment_factory(
        config: ToolsConfig, project_root: Path
    ) -> EnvironmentTools:
        return EnvironmentTools(config, project_root)

    def _ci_factory(config: ToolsConfig, project_root: Path) -> CITools:
        return CITools(config, project_root)

    def _dev_factory(config: ToolsConfig) -> DevTools:
        return DevTools(config=config)

    return ToolsDependencies(
        load_config=_load_config,
        quality_factory=_quality_factory,
        testing_factory=_testing_factory,
        environment_factory=_environment_factory,
        ci_factory=_ci_factory,
        dev_factory=_dev_factory,
        result_handler=default_tool_result_handler,
    )


_dependency_factory: Callable[[], ToolsDependencies] = default_tools_dependencies
_cached_dependencies: Optional[ToolsDependencies] = None


def configure_tools_dependencies(factory: Callable[[], ToolsDependencies]) -> None:
    global _dependency_factory, _cached_dependencies
    _dependency_factory = factory
    _cached_dependencies = None


def reset_tools_dependencies() -> None:
    configure_tools_dependencies(default_tools_dependencies)


def get_tools_dependencies() -> ToolsDependencies:
    global _cached_dependencies
    if _cached_dependencies is None:
        _cached_dependencies = _dependency_factory()
    return _cached_dependencies


def _ensure_config_loaded() -> None:
    """Common helper to ensure config is loaded, eliminating repeated None-checks."""
    if state.config is None:
        load_config_with_error_handling(state.project_root)


@contextmanager
def override_tools_dependencies(deps: ToolsDependencies):
    global _cached_dependencies
    previous_factory = _dependency_factory
    previous_cached = _cached_dependencies
    configure_tools_dependencies(lambda: deps)
    try:
        yield
    finally:
        configure_tools_dependencies(previous_factory)
        _cached_dependencies = previous_cached


def load_config_with_error_handling(
    project_root: Path | None = None,
    *,
    deps: ToolsDependencies | None = None,
) -> None:
    """Load configuration with proper error handling."""
    try:
        dependencies = deps or get_tools_dependencies()
        # Reuse the cached configuration when no project root override is provided.
        if project_root is None and state.config is not None:
            return

        target_root = project_root if project_root is not None else state.project_root

        loaded_config = dependencies.load_config(target_root)
        state.config = loaded_config

        if project_root is not None:
            state.project_root = project_root
        elif state.project_root is None and target_root is not None:
            state.project_root = target_root

        # Apply default learning mode and verbosity from config on load.
        # Tests assert these defaults are applied regardless of prior flags;
        # CLI options provided later can override.
        if not state.learning_mode_set:
            state.learning_mode = state.config.learning_mode_default
            state.mark_learning_mode_default(True)

        state.verbosity = state.config.default_verbosity

    except ToolConfigurationError as e:
        handler = (deps or get_tools_dependencies()).result_handler
        handler(
            ToolResult.create(
                success=False,
                exit_code=1,
                namespace="tools",
                category="dev",
                command="config-load",
                stderr=f"Configuration error: {e}",
            )
        )
    except Exception as e:
        handler = (deps or get_tools_dependencies()).result_handler
        handler(
            ToolResult.create(
                success=False,
                exit_code=1,
                namespace="tools",
                category="dev",
                command="config-load",
                stderr=f"Unexpected error loading configuration: {e}",
            )
        )


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


# Helper function to get tool instances
def get_quality_tools() -> QualityTools:
    """Get quality tools instance."""
    _ensure_config_loaded()
    assert state.config is not None, "Config should be loaded after _ensure_config_loaded"
    deps = get_tools_dependencies()
    return deps.quality_factory(state.config, state.project_root or Path.cwd())


def get_testing_tools() -> TestingTools:
    """Get testing tools instance."""
    _ensure_config_loaded()
    assert state.config is not None, "Config should be loaded after _ensure_config_loaded"
    deps = get_tools_dependencies()
    return deps.testing_factory(state.config, state.project_root or Path.cwd())


def get_environment_tools() -> EnvironmentTools:
    """Get environment tools instance."""
    _ensure_config_loaded()
    assert state.config is not None, "Config should be loaded after _ensure_config_loaded"
    deps = get_tools_dependencies()
    return deps.environment_factory(state.config, state.project_root or Path.cwd())


def get_ci_tools() -> CITools:
    """Get CI tools instance."""
    _ensure_config_loaded()
    assert state.config is not None, "Config should be loaded after _ensure_config_loaded"
    deps = get_tools_dependencies()
    return deps.ci_factory(state.config, state.project_root or Path.cwd())


def get_dev_tools() -> DevTools:
    """Get dev tools instance."""
    _ensure_config_loaded()
    assert state.config is not None, "Config should be loaded after _ensure_config_loaded"
    deps = get_tools_dependencies()
    return deps.dev_factory(state.config)


def handle_tool_result(result: ToolResult) -> None:
    """Handle tool result using current dependencies."""
    handler = get_tools_dependencies().result_handler
    handler(result)


# Quality commands
@quality_app.command("lint")
def quality_lint(
    args: Annotated[
        Optional[List[str]], typer.Argument(help="Additional ruff arguments")
    ] = None,
) -> None:
    """Run Ruff lint checks."""
    try:
        tools = get_quality_tools()
        result = tools.lint(
            args or [],
            learning_mode=state.learning_mode,
            verbosity_level=state.verbosity,
        )
        handle_tool_result(result)
    except ToolExecutionError as e:
        typer.echo(f"Error: {e}", err=True)
        raise typer.Exit(1)


@test_app.command("coverage")
def test_coverage(
    line_threshold: Annotated[
        float,
        typer.Option("--line-threshold", help="Minimum line coverage (0 = config)"),
    ] = 0.0,
    branch_threshold: Annotated[
        float,
        typer.Option("--branch-threshold", help="Minimum branch coverage (0 = config)"),
    ] = 0.0,
    force_regen: Annotated[
        bool,
        typer.Option("--force-regen", help="Force regenerating coverage data"),
    ] = False,
    verbose: Annotated[
        bool, typer.Option("--verbose", help="Show verbose artifacts")
    ] = False,
    args: Annotated[
        Optional[List[str]], typer.Argument(help="Additional arguments (ignored)")
    ] = None,
) -> None:
    """Run full coverage pipeline (report + threshold) in one command."""

    try:
        tools = get_testing_tools()
        result = tools.coverage(
            args or [],
            line_threshold=line_threshold or 0.0,
            branch_threshold=branch_threshold or 0.0,
            verbose=verbose,
            learning_mode=state.learning_mode,
            verbosity_level=state.verbosity,
            force_regen=force_regen,
        )
        handle_tool_result(result)
    except ToolExecutionError as e:
        typer.echo(f"Error: {e}", err=True)
        raise typer.Exit(1)


@quality_app.command("format")
def quality_format(
    args: Annotated[
        Optional[List[str]], typer.Argument(help="Additional ruff arguments")
    ] = None,
) -> None:
    """Auto-fix and format code with Ruff."""
    try:
        tools = get_quality_tools()
        result = tools.format(
            args or [],
            learning_mode=state.learning_mode,
            verbosity_level=state.verbosity,
        )
        handle_tool_result(result)
    except ToolExecutionError as e:
        typer.echo(f"Error: {e}", err=True)
        raise typer.Exit(1)


@quality_app.command("lint-check")
def quality_lint_check(
    args: Annotated[
        Optional[List[str]], typer.Argument(help="Additional ruff arguments")
    ] = None,
) -> None:
    """Run Ruff in check-only mode (alias for lint)."""
    try:
        tools = get_quality_tools()
        result = tools.lint_check(
            args or [],
            learning_mode=state.learning_mode,
            verbosity_level=state.verbosity,
        )
        handle_tool_result(result)
    except ToolExecutionError as e:
        typer.echo(f"Error: {e}", err=True)
        raise typer.Exit(1)


@quality_app.command("deadcode")
def quality_deadcode(
    args: Annotated[
        Optional[List[str]], typer.Argument(help="Additional vulture arguments")
    ] = None,
) -> None:
    """Scan for dead code using vulture."""
    try:
        tools = get_quality_tools()
        result = tools.deadcode(
            args or [],
            learning_mode=state.learning_mode,
            verbosity_level=state.verbosity,
        )
        handle_tool_result(result)
    except ToolExecutionError as e:
        typer.echo(f"Error: {e}", err=True)
        raise typer.Exit(1)


@quality_app.command("basedpyright")
def quality_basedpyright(
    args: Annotated[
        Optional[List[str]], typer.Argument(help="Additional basedpyright arguments")
    ] = None,
) -> None:
    """Run BasedPyright type checks."""
    try:
        tools = get_quality_tools()
        result = tools.basedpyright(
            args or [],
            learning_mode=state.learning_mode,
            verbosity_level=state.verbosity,
        )
        handle_tool_result(result)
    except ToolExecutionError as e:
        typer.echo(f"Error: {e}", err=True)
        raise typer.Exit(1)


@quality_app.command("mypy")
def quality_mypy(
    args: Annotated[
        Optional[List[str]], typer.Argument(help="Additional mypy arguments")
    ] = None,
) -> None:
    """Run Mypy type checks."""
    try:
        tools = get_quality_tools()
        result = tools.mypy(
            args or [],
            learning_mode=state.learning_mode,
            verbosity_level=state.verbosity,
        )
        handle_tool_result(result)
    except ToolExecutionError as e:
        typer.echo(f"Error: {e}", err=True)
        raise typer.Exit(1)


@quality_app.command("typecheck")
def quality_typecheck(
    args: Annotated[
        Optional[List[str]],
        typer.Argument(help="Additional arguments (applied to both tools)"),
    ] = None,
) -> None:
    """Run both BasedPyright and Mypy type checks."""
    try:
        tools = get_quality_tools()
        result = tools.typecheck(
            args or [],
            learning_mode=state.learning_mode,
            verbosity_level=state.verbosity,
        )
        handle_tool_result(result)
    except ToolExecutionError as e:
        typer.echo(f"Error: {e}", err=True)
        raise typer.Exit(1)


@quality_app.command("all")
def quality_all(
    args: Annotated[
        Optional[List[str]],
        typer.Argument(help="Additional arguments (applied to all tools)"),
    ] = None,
) -> None:
    """Run all quality checks (lint, typecheck, deadcode)."""
    try:
        tools = get_quality_tools()
        result = tools.all_checks(
            args or [],
            learning_mode=state.learning_mode,
            verbosity_level=state.verbosity,
        )
        handle_tool_result(result)
    except ToolExecutionError as e:
        typer.echo(f"Error: {e}", err=True)
        raise typer.Exit(1)


def _invoke_tests(
    ctx: typer.Context,
    test_dir: str,
    pattern: str | None,
    extra_args: list[str],
) -> None:
    try:
        tools = get_testing_tools()
        args = list(extra_args)
        if pattern:
            args.extend(["-k", pattern])

        learning_mode = state.learning_mode
        verbosity = state.verbosity

        suite_map = {
            "tests/unit": "unit",
            "tests/property": "property_tests",
            "tests/regression": "regression",
        }
        method_name = suite_map.get(test_dir)
        if method_name is None:
            raise ToolExecutionError(
                f"Unsupported test suite: {test_dir}",
                reason="No registered TestingTools handler",
                rationale="Add a dedicated TestingTools method for this suite or update CLI dispatch.",
            )

        suite_fn = getattr(tools, method_name)
        result = suite_fn(args, learning_mode=learning_mode, verbosity_level=verbosity)  # type: ignore[operator]
        handle_tool_result(result)
    except ToolExecutionError as e:
        typer.echo(f"Error: {e}", err=True)
        raise typer.Exit(1)


@test_app.command("unit")
def test_unit(
    ctx: typer.Context,
    pattern: Annotated[str | None, typer.Argument()] = None,
    extra_args: Annotated[list[str] | None, typer.Argument()] = None,
) -> None:
    """Run unit tests."""

    _invoke_tests(ctx, "tests/unit", pattern, list(extra_args or []))


@test_app.command("property")
def test_property(
    ctx: typer.Context,
    pattern: Annotated[str | None, typer.Argument()] = None,
    extra_args: Annotated[list[str] | None, typer.Argument()] = None,
) -> None:
    """Run property-based tests using Hypothesis."""

    _invoke_tests(ctx, "tests/property", pattern, list(extra_args or []))


@test_app.command("regression")
def test_regression(
    ctx: typer.Context,
    pattern: Annotated[str | None, typer.Argument()] = None,
    extra_args: Annotated[Optional[List[str]], typer.Argument()] = None,
) -> None:
    """Run regression suites (policy guards, slow checks)."""

    _invoke_tests(ctx, "tests/regression", pattern, list(extra_args or []))


@test_app.command("all")
def test_all(
    args: Annotated[
        Optional[List[str]], typer.Argument(help="Additional pytest arguments")
    ] = None,
) -> None:
    """Run all tests."""
    try:
        tools = get_testing_tools()
        result = tools.all_tests(
            args or [],
            learning_mode=state.learning_mode,
            verbosity_level=state.verbosity,
        )
        handle_tool_result(result)
    except ToolExecutionError as e:
        typer.echo(f"Error: {e}", err=True)
        raise typer.Exit(1)


@test_app.command("clean")
def test_clean(
    args: Annotated[
        Optional[List[str]], typer.Argument(help="Additional arguments (ignored)")
    ] = None,
) -> None:
    """Clean test artifacts and caches."""
    try:
        tools = get_testing_tools()
        result = tools.clean(
            args or [],
            learning_mode=state.learning_mode,
            verbosity_level=state.verbosity,
        )
        handle_tool_result(result)
    except ToolExecutionError as e:
        typer.echo(f"Error: {e}", err=True)
        raise typer.Exit(1)


# Mutation testing subcommands
mutation_app = typer.Typer(
    name="mutation",
    help="Mutation testing operations",
    no_args_is_help=True,
)
test_app.add_typer(mutation_app, name="mutation")


@mutation_app.command("reset")
def test_mutation_reset(
    args: Annotated[
        Optional[List[str]], typer.Argument(help="Additional arguments (ignored)")
    ] = None,
) -> None:
    """Remove the cached Cosmic Ray session."""
    try:
        tools = get_testing_tools()
        result = tools.mutation_reset(args or [])
        handle_tool_result(result)
    except ToolExecutionError as e:
        typer.echo(f"Error: {e}", err=True)
        raise typer.Exit(1)


@mutation_app.command("summary")
def test_mutation_summary(
    args: Annotated[
        Optional[List[str]], typer.Argument(help="Additional arguments (ignored)")
    ] = None,
) -> None:
    """Show a summary of the current Cosmic Ray configuration."""
    try:
        tools = get_testing_tools()
        result = tools.mutation_summary(args or [])
        handle_tool_result(result)
    except ToolExecutionError as e:
        typer.echo(f"Error: {e}", err=True)
        raise typer.Exit(1)


@mutation_app.command("init")
def test_mutation_init(
    args: Annotated[
        Optional[List[str]], typer.Argument(help="Additional arguments (ignored)")
    ] = None,
) -> None:
    """Initialize the Cosmic Ray session database if needed."""
    try:
        tools = get_testing_tools()
        result = tools.mutation_init(args or [])
        handle_tool_result(result)
    except ToolExecutionError as e:
        typer.echo(f"Error: {e}", err=True)
        raise typer.Exit(1)


@mutation_app.command("exec")
def test_mutation_exec(
    args: Annotated[
        Optional[List[str]], typer.Argument(help="Additional arguments (ignored)")
    ] = None,
) -> None:
    """Execute mutation tests with Cosmic Ray."""
    try:
        tools = get_testing_tools()
        result = tools.mutation_exec(args or [])
        handle_tool_result(result)
    except ToolExecutionError as e:
        typer.echo(f"Error: {e}", err=True)
        raise typer.Exit(1)


@mutation_app.command("report")
def test_mutation_report(
    args: Annotated[
        Optional[List[str]], typer.Argument(help="Additional arguments (ignored)")
    ] = None,
) -> None:
    """Generate a mutation testing report."""
    try:
        tools = get_testing_tools()
        result = tools.mutation_report(args or [])
        handle_tool_result(result)
    except ToolExecutionError as e:
        typer.echo(f"Error: {e}", err=True)
        raise typer.Exit(1)


@mutation_app.command("run")
def test_mutation_run(
    args: Annotated[
        Optional[List[str]], typer.Argument(help="Additional arguments (ignored)")
    ] = None,
) -> None:
    """Run the full mutation testing pipeline."""
    try:
        tools = get_testing_tools()
        result = tools.mutation_run(args or [])
        handle_tool_result(result)
    except ToolExecutionError as e:
        typer.echo(f"Error: {e}", err=True)
        raise typer.Exit(1)


# Environment commands
@env_app.command("setup")
def env_setup(
    clear: Annotated[
        bool, typer.Option("--clear", help="Remove existing virtual environment first")
    ] = False,
    args: Annotated[
        Optional[List[str]], typer.Argument(help="Additional arguments (ignored)")
    ] = None,
) -> None:
    """Create a fresh uv-managed virtual environment and install all dependencies."""
    try:
        tools = get_environment_tools()
        result = tools.setup(args or [], clear=clear)
        handle_tool_result(result)
    except ToolExecutionError as e:
        typer.echo(f"Error: {e}", err=True)
        raise typer.Exit(1)


@env_app.command("sync")
def env_sync(
    groups: Annotated[
        Optional[List[str]],
        typer.Option("--group", help="Sync specific dependency groups (repeatable)"),
    ] = None,
    all_groups: Annotated[
        bool,
        typer.Option("--all-groups", help="Install all optional dependency groups"),
    ] = False,
    frozen: Annotated[
        bool,
        typer.Option(
            "--frozen", help="Use existing lockfile without resolving new versions"
        ),
    ] = False,
    args: Annotated[
        Optional[List[str]], typer.Argument(help="Additional uv sync arguments")
    ] = None,
) -> None:
    """Sync project dependencies using uv."""
    try:
        tools = get_environment_tools()
        result = tools.sync(
            args or [], groups=groups, all_groups=all_groups, frozen=frozen
        )
        handle_tool_result(result)
    except ToolExecutionError as e:
        typer.echo(f"Error: {e}", err=True)
        raise typer.Exit(1)


@env_app.command("verify")
def env_verify(
    args: Annotated[
        Optional[List[str]], typer.Argument(help="Additional arguments (ignored)")
    ] = None,
) -> None:
    """Ensure the project package imports correctly."""
    try:
        tools = get_environment_tools()
        result = tools.verify(args or [])
        handle_tool_result(result)
    except ToolExecutionError as e:
        typer.echo(f"Error: {e}", err=True)
        raise typer.Exit(1)


@env_app.command("clean")
def env_clean(
    args: Annotated[
        Optional[List[str]], typer.Argument(help="Additional arguments (ignored)")
    ] = None,
) -> None:
    """Remove caches and temporary build artifacts."""
    try:
        tools = get_environment_tools()
        result = tools.clean(args or [])
        handle_tool_result(result)
    except ToolExecutionError as e:
        typer.echo(f"Error: {e}", err=True)
        raise typer.Exit(1)


@env_app.command("info")
def env_info(
    args: Annotated[
        Optional[List[str]], typer.Argument(help="Additional arguments (ignored)")
    ] = None,
) -> None:
    """Show environment information."""
    try:
        tools = get_environment_tools()
        result = tools.info(args or [])
        handle_tool_result(result)
    except ToolExecutionError as e:
        typer.echo(f"Error: {e}", err=True)
        raise typer.Exit(1)


@env_app.command("ai-guidelines")
def env_ai_guidelines(
    tool: Annotated[str, typer.Argument(help="Target tool name for AI guidelines")],
    dry_run: Annotated[
        bool, typer.Option("--dry-run", help="Preview actions without executing")
    ] = False,
    args: Annotated[
        Optional[List[str]], typer.Argument(help="Additional arguments (ignored)")
    ] = None,
) -> None:
    """Set up AI guideline symlinks for the requested tool."""
    try:
        tools = get_environment_tools()
        result = tools.ai_guidelines(args or [], tool=tool, dry_run=dry_run)
        handle_tool_result(result)
    except ToolExecutionError as e:
        typer.echo(f"Error: {e}", err=True)
        raise typer.Exit(1)


@env_app.command("tensorboard")
def env_tensorboard(
    logdir: Annotated[
        Path,
        typer.Option(
            "--logdir",
            exists=True,
            file_okay=False,
            dir_okay=True,
            readable=True,
            help="TensorBoard log directory",
        ),
    ],
    port: Annotated[
        int, typer.Option("--port", help="Port to bind TensorBoard to")
    ] = 6006,
    host: Annotated[
        str, typer.Option("--host", help="Host interface to bind to")
    ] = "127.0.0.1",
    args: Annotated[
        Optional[List[str]], typer.Argument(help="Additional tensorboard arguments")
    ] = None,
) -> None:
    """Launch TensorBoard for the given log directory."""
    try:
        tools = get_environment_tools()
        result = tools.tensorboard(args or [], logdir=logdir, port=port, host=host)
        handle_tool_result(result)
    except ToolExecutionError as e:
        typer.echo(f"Error: {e}", err=True)
        raise typer.Exit(1)


@env_app.command("gguf-help")
def env_gguf_help(
    args: Annotated[
        Optional[List[str]], typer.Argument(help="Additional arguments (ignored)")
    ] = None,
) -> None:
    """Show llama.cpp GGUF conversion help."""
    try:
        tools = get_environment_tools()
        result = tools.gguf_help(args or [])
        handle_tool_result(result)
    except ToolExecutionError as e:
        typer.echo(f"Error: {e}", err=True)
        raise typer.Exit(1)


# CI commands
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
        typer.echo(f"Error: {e}", err=True)
        raise typer.Exit(1)


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
    except ToolExecutionError as e:
        typer.echo(f"Error: {e}", err=True)
        raise typer.Exit(1)


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
    except ToolExecutionError as e:
        typer.echo(f"Error: {e}", err=True)
        raise typer.Exit(1)


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
    """Run the GitHub quality workflow locally using act."""
    try:
        tools = get_ci_tools()
        result = tools.quality_ci_local(args or [], bind_caches=bind_caches)
        handle_tool_result(result)
    except ToolExecutionError as e:
        typer.echo(f"Error: {e}", err=True)
        raise typer.Exit(1)


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
    except ToolExecutionError as e:
        typer.echo(f"Error: {e}", err=True)
        raise typer.Exit(1)


# Mutation testing moved to testing tools


# Dev commands
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
    """List GitHub PR review comments with optional filtering."""
    try:
        tools = get_dev_tools()
        result = tools.review_list(
            pr_number=pr_number,
            unreplied=unreplied,
            unresolved=unresolved,
            remote=remote,
        )
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
    """Bulk reply to GitHub PR review comments."""
    try:
        tools = get_dev_tools()
        result = tools.review_bulk_reply(
            pr_number=pr_number,
            replies_file=replies_file,
            remote=remote,
        )
        handle_tool_result(result)
    except ToolExecutionError as e:
        typer.echo(f"Error: {e}", err=True)
        raise typer.Exit(1)


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
    """Delete GitHub PR review comments."""
    try:
        tools = get_dev_tools()
        result = tools.review_delete(
            pr_number=pr_number,
            comments_file=comments_file,
            remote=remote,
        )
        handle_tool_result(result)
    except ToolExecutionError as e:
        typer.echo(f"Error: {e}", err=True)
        raise typer.Exit(1)


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
        result = tools.batch_review(
            output_format=output_format,
        )
        handle_tool_result(result)
    except ToolExecutionError as e:
        typer.echo(f"Error: {e}", err=True)
        raise typer.Exit(1)


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
        result = tools.workflow_status(
            output_format=output_format,
        )
        handle_tool_result(result)
    except ToolExecutionError as e:
        typer.echo(f"Error: {e}", err=True)
        raise typer.Exit(1)


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
    except ToolExecutionError as e:
        typer.echo(f"Error: {e}", err=True)
        raise typer.Exit(1)


@learn_app.command("commands")
def learn_commands(
    category: Annotated[
        Optional[str],
        typer.Option("--category", help="Show commands for specific category"),
    ] = None,
    detailed: Annotated[
        bool, typer.Option("--detailed", help="Show detailed command descriptions")
    ] = False,
) -> None:
    """Show available commands with descriptions and usage examples."""

    # Command catalog with descriptions and examples
    command_catalog: Dict[str, Dict[str, Any]] = {
        "quality": {
            "description": "Code quality tools (lint, format, typecheck)",
            "commands": {
                "lint": "Run Ruff lint checks for code style and potential issues",
                "format": "Auto-fix and format code with Ruff",
                "lint-check": "Run Ruff in check-only mode (alias for lint)",
                "deadcode": "Scan for unused code using vulture",
                "basedpyright": "Run BasedPyright type checks",
                "mypy": "Run Mypy type checks",
                "typecheck": "Run both BasedPyright and Mypy type checks",
                "all": "Run all quality checks (lint, typecheck, deadcode)",
            },
            "examples": [
                "uv run tools quality lint",
                "uv run tools quality format",
                "uv run tools quality all --learning-mode",
            ],
        },
        "test": {
            "description": "Testing tools (unit, integration, e2e, coverage)",
            "commands": {
                "unit": "Run unit tests",
                "integration": "Run integration tests",
                "e2e": "Run end-to-end tests",
                "acceptance": "Run acceptance tests",
                "property": "Run property-based tests",
                "all": "Run all tests",
                "coverage-test": "Run tests with coverage collection",
                "coverage-report": "Generate coverage reports",
                "coverage-threshold": "Check coverage thresholds",
                "clean": "Clean test artifacts and caches",
            },
            "examples": [
                "uv run tools test unit",
                "uv run tools test coverage-test",
                "uv run tools test all --learning-mode",
            ],
        },
        "env": {
            "description": "Environment management (setup, sync, clean)",
            "commands": {
                "setup": "Create fresh virtual environment and install dependencies",
                "sync": "Sync project dependencies using uv",
                "verify": "Ensure the project package imports correctly",
                "clean": "Remove caches and temporary build artifacts",
                "info": "Show environment information",
                "ai-guidelines": "Set up AI guideline symlinks",
                "tensorboard": "Launch TensorBoard for log visualization",
                "gguf-help": "Show llama.cpp GGUF conversion help",
            },
            "examples": [
                "uv run tools env setup",
                "uv run tools env sync --all-groups",
                "uv run tools env clean",
            ],
        },
        "ci": {
            "description": "CI/CD operations (quality gates, mutation testing)",
            "commands": {
                "quality-gate": "Run full pre-commit quality gate",
                "quality-fast": "Run fast quality checks (lint/format focused)",
                "quality-ext": "Run extended quality validation with mutation testing",
                "quality-ci-local": "Run GitHub quality workflow locally using act",
                "coverage-badge": "Regenerate SVG coverage badges",
                "mutation reset": "Remove cached Cosmic Ray session",
                "mutation summary": "Show summary of previous mutation testing",
                "mutation init": "Initialize Cosmic Ray session database",
                "mutation exec": "Execute mutation tests",
                "mutation report": "Render mutation testing report",
                "mutation run": "Run full mutation testing pipeline",
            },
            "examples": [
                "uv run tools ci quality-gate",
                "uv run tools ci quality-fast",
                "uv run tools ci mutation run",
            ],
        },
        "learn": {
            "description": "Learning mode utilities and educational content",
            "commands": {
                "commands": "Show available commands with descriptions",
                "explain": "Explain a specific command with educational content",
                "best-practices": "Show comprehensive best practices guide",
            },
            "examples": [
                "uv run tools learn commands --category quality",
                "uv run tools learn explain quality.lint",
                "uv run tools learn best-practices --verbosity 2",
            ],
        },
    }

    if category:
        # Show detailed information for specific category
        if category not in command_catalog:
            typer.echo(
                f"Error: Unknown category '{category}'. Valid categories: {', '.join(command_catalog.keys())}",
                err=True,
            )
            raise typer.Exit(1)

        cat_info = command_catalog[category]
        typer.echo(f"[bold]{category.title()} Tools[/bold]")
        typer.echo(f"{cat_info['description']}")
        typer.echo("")

        typer.echo("[bold]Available Commands:[/bold]")
        for cmd, desc in cat_info["commands"].items():
            typer.echo(f"  [bold]{cmd}[/bold] - {desc}")
        typer.echo("")

        if detailed or len(cat_info["examples"]) <= 3:
            typer.echo("[bold]Usage Examples:[/bold]")
            for example in cat_info["examples"]:
                typer.echo(f"  {example}")
            typer.echo("")

        typer.echo(
            f"Use [bold]uv run tools {category} --help[/bold] for detailed command options."
        )
        typer.echo(
            f"Use [bold]uv run tools learn explain {category}.<command>[/bold] for educational content."
        )

    else:
        # Show overview of all categories
        typer.echo("[bold]ML Playground Tools - Command Discovery[/bold]")
        typer.echo("")
        typer.echo("Available tool categories:")
        typer.echo("")

        for cat, info in command_catalog.items():
            typer.echo(f"  [bold]{cat}[/bold] - {info['description']}")
            if detailed:
                typer.echo(
                    f"    Commands: {', '.join(list(info['commands'].keys())[:5])}{'...' if len(info['commands']) > 5 else ''}"
                )
        typer.echo("")

        typer.echo("[bold]Quick Start:[/bold]")
        typer.echo(
            "  uv run tools env setup              # Set up development environment"
        )
        typer.echo("  uv run tools quality all            # Run all quality checks")
        typer.echo("  uv run tools test unit              # Run unit tests")
        typer.echo("  uv run tools ci quality-gate        # Run full quality gate")
        typer.echo("")

        typer.echo("[bold]Learning and Discovery:[/bold]")
        typer.echo(
            "  uv run tools learn commands --category <name>    # Show commands for category"
        )
        typer.echo(
            "  uv run tools learn explain <category>.<command>  # Get detailed explanations"
        )
        typer.echo(
            "  uv run tools learn best-practices               # Show best practices guide"
        )
        typer.echo(
            "  uv run tools <category> <command> --learning-mode # See explanations while running"
        )
        typer.echo("")

        typer.echo(
            "Use [bold]--detailed[/bold] for more information or [bold]--category <name>[/bold] for specific categories."
        )


@learn_app.command("explain")
def learn_explain(
    command: Annotated[
        str,
        typer.Argument(help="Command to explain (e.g., 'quality.lint', 'test.unit')"),
    ],
    verbosity: Annotated[
        int,
        typer.Option(
            "--verbosity",
            "-v",
            min=0,
            max=2,
            help="Explanation verbosity: 0=minimal, 1=standard, 2=comprehensive",
        ),
    ] = 1,
) -> None:
    """Explain a specific command with educational content."""
    from ml_playground.tools.core.learning_mode import (
        LearningModeEngine,
        VerbosityLevel,
    )

    # Parse command format (category.command)
    if "." not in command:
        typer.echo(
            "Error: Command must be in format 'category.command' (e.g., 'quality.lint')",
            err=True,
        )
        raise typer.Exit(1)

    try:
        category, cmd = command.split(".", 1)
    except ValueError:
        typer.echo(
            f"Error: Invalid command format '{command}'. Use 'category.command'",
            err=True,
        )
        raise typer.Exit(1)

    # Validate category
    valid_categories = {"quality", "test", "env", "ci"}
    if category not in valid_categories:
        typer.echo(
            f"Error: Unknown category '{category}'. Valid categories: {', '.join(valid_categories)}",
            err=True,
        )
        raise typer.Exit(1)

    # Create learning engine with specified verbosity
    verbosity_level = VerbosityLevel(verbosity)
    engine = LearningModeEngine(verbosity_level)

    # Generate explanation
    learning_info = engine.explain_command(
        command=cmd, context=f"Explaining {category}.{cmd} command", category=category
    )

    # Display explanation
    typer.echo(f"[bold]Command: {command}[/bold]")
    typer.echo("")

    if learning_info.explanations:
        typer.echo("[bold]📖 Explanation:[/bold]")
        for explanation in learning_info.explanations:
            typer.echo(f"  {explanation}")
        typer.echo("")

    if learning_info.best_practices:
        typer.echo("[bold]✨ Best Practices:[/bold]")
        for practice in learning_info.best_practices:
            typer.echo(f"  • {practice}")
        typer.echo("")

    if learning_info.related_concepts:
        typer.echo("[bold]🔗 Related Concepts:[/bold]")
        for concept in learning_info.related_concepts:
            typer.echo(f"  • {concept}")
        typer.echo("")

    if not any(
        [
            learning_info.explanations,
            learning_info.best_practices,
            learning_info.related_concepts,
        ]
    ):
        typer.echo(f"No educational content available for '{command}'")
        typer.echo(
            "This command may not exist or educational content may not be implemented yet."
        )


@learn_app.command("best-practices")
def learn_best_practices(
    category: Annotated[
        Optional[str],
        typer.Option("--category", help="Show best practices for specific category"),
    ] = None,
    verbosity: Annotated[
        int,
        typer.Option(
            "--verbosity",
            "-v",
            min=0,
            max=2,
            help="Best practices verbosity: 0=minimal, 1=standard, 2=comprehensive",
        ),
    ] = 1,
) -> None:
    """Show comprehensive best practices guide for development workflows."""
    from ml_playground.tools.core.learning_mode import (
        LearningModeEngine,
        VerbosityLevel,
    )

    # Create learning engine with specified verbosity
    verbosity_level = VerbosityLevel(verbosity)
    engine = LearningModeEngine(verbosity_level)

    if category:
        # Show category-specific best practices
        valid_categories = {"quality", "test", "env", "ci"}
        if category not in valid_categories:
            typer.echo(
                f"Error: Unknown category '{category}'. Valid categories: {', '.join(valid_categories)}",
                err=True,
            )
            raise typer.Exit(1)

        typer.echo(f"[bold]Best Practices for {category.title()} Tools[/bold]")
        typer.echo("")

        # Use representative command to fetch best practices via public API
        rep_map = {
            "quality": "lint",
            "test": "unit",
            "env": "setup",
            "ci": "quality-gate",
        }
        rep_cmd = rep_map.get(category, "lint")
        info = engine.explain_command(
            command=rep_cmd,
            context="best-practices",
            category=category,
            executed_commands=[],
        )
        for practice in info.best_practices:
            typer.echo(f"  • {practice}")
        typer.echo("")

        # Show learning paths for this category
        _show_learning_paths(category, verbosity_level)
    else:
        # Show comprehensive best practices guide
        typer.echo("[bold]ML Playground Development Best Practices[/bold]")
        typer.echo("")

        # General development workflow
        typer.echo("[bold]🚀 Development Workflow[/bold]")
        workflow_practices = [
            "Start with environment setup: `uv run tools env setup`",
            "Run fast quality checks frequently: `uv run tools ci quality-fast`",
            "Write tests before implementing features (TDD approach)",
            "Use learning mode to understand tools: `--learning-mode`",
            "Run full quality gates before merging: `uv run tools ci quality-gate`",
        ]
        for practice in workflow_practices:
            typer.echo(f"  • {practice}")
        typer.echo("")

        # Show category-specific practices
        categories = ["quality", "test", "env", "ci"]
        for cat in categories:
            typer.echo(f"[bold]{cat.title()} Tools:[/bold]")
            rep_map = {
                "quality": "lint",
                "test": "unit",
                "env": "setup",
                "ci": "quality-gate",
            }
            rep_cmd = rep_map.get(cat, "lint")
            info = engine.explain_command(
                command=rep_cmd,
                context="best-practices",
                category=cat,
                executed_commands=[],
            )
            for practice in info.best_practices[
                :2
            ]:  # Show top 2 practices per category
                typer.echo(f"  • {practice}")
            typer.echo("")

        # Show learning paths
        typer.echo("[bold]📚 Learning Paths[/bold]")
        typer.echo("")
        _show_learning_paths_overview(verbosity_level)


def _show_learning_paths(category: str, verbosity_level: VerbosityLevel) -> None:
    """Show learning paths for a specific category."""
    learning_paths = {
        "quality": {
            "beginner": [
                "Start with `uv run tools quality lint` to check code style",
                "Use `uv run tools quality format` to auto-fix formatting issues",
                "Learn about type checking with `uv run tools quality typecheck`",
                "Run all quality checks with `uv run tools quality all`",
            ],
            "intermediate": [
                "Integrate quality checks into your editor for real-time feedback",
                "Set up pre-commit hooks to run quality checks automatically",
                "Understand different type checkers: BasedPyright vs MyPy",
                "Use dead code detection to maintain clean codebase",
            ],
            "advanced": [
                "Configure custom linting rules for your team's standards",
                "Optimize quality check performance for large codebases",
                "Integrate quality metrics into CI/CD pipelines",
                "Use quality tools for code review automation",
            ],
        },
        "test": {
            "beginner": [
                "Start with unit tests: `uv run tools test unit`",
                "Learn about test coverage: `uv run tools test coverage-test`",
                "Understand different test types: unit, integration, e2e",
                "Use property-based testing for robust validation",
            ],
            "intermediate": [
                "Write effective integration tests for component interactions",
                "Use coverage reports to identify untested code paths",
                "Implement acceptance tests for business requirements",
                "Balance test types in the testing pyramid",
            ],
            "advanced": [
                "Use mutation testing to validate test effectiveness",
                "Optimize test suite performance and parallelization",
                "Implement comprehensive test strategies for ML workflows",
                "Design testable architectures with dependency injection",
            ],
        },
        "env": {
            "beginner": [
                "Set up development environment: `uv run tools env setup`",
                "Sync dependencies regularly: `uv run tools env sync`",
                "Verify environment health: `uv run tools env verify`",
                "Clean caches when issues arise: `uv run tools env clean`",
            ],
            "intermediate": [
                "Understand dependency groups and selective syncing",
                "Use environment info for troubleshooting issues",
                "Set up AI guidelines for consistent workflows",
                "Manage multiple environments for different purposes",
            ],
            "advanced": [
                "Optimize environment setup for team onboarding",
                "Implement environment validation in CI/CD",
                "Use containerized environments for consistency",
                "Automate environment management workflows",
            ],
        },
        "ci": {
            "beginner": [
                "Run quality gates: `uv run tools ci quality-gate`",
                "Use fast quality checks for quick feedback",
                "Generate coverage badges for documentation",
                "Understand the importance of quality gates",
            ],
            "intermediate": [
                "Set up local CI testing with act",
                "Use mutation testing to improve test quality",
                "Implement comprehensive quality validation",
                "Monitor quality metrics over time",
            ],
            "advanced": [
                "Design robust CI/CD pipelines with quality gates",
                "Optimize CI performance with caching and parallelization",
                "Implement progressive quality standards",
                "Use CI metrics for continuous improvement",
            ],
        },
    }

    if category in learning_paths:
        paths = learning_paths[category]

        if verbosity_level.value >= 1:  # Standard or comprehensive
            typer.echo("[bold]🎯 Beginner Path:[/bold]")
            for step in paths["beginner"]:
                typer.echo(f"  1. {step}")
            typer.echo("")

        if verbosity_level.value >= 1:  # Standard or comprehensive
            typer.echo("[bold]🚀 Intermediate Path:[/bold]")
            for step in paths["intermediate"]:
                typer.echo(f"  2. {step}")
            typer.echo("")

        if verbosity_level.value >= 2:  # Comprehensive only
            typer.echo("[bold]🏆 Advanced Path:[/bold]")
            for step in paths["advanced"]:
                typer.echo(f"  3. {step}")
            typer.echo("")


def _show_learning_paths_overview(verbosity_level: VerbosityLevel) -> None:
    """Show overview of learning paths for all categories."""
    if verbosity_level.value == 0:  # Minimal
        typer.echo(
            "Use `uv run tools learn best-practices --category <name>` for specific guidance"
        )
        return

    typer.echo("[bold]🎯 For Beginners:[/bold]")
    typer.echo("  1. Start with environment setup and basic quality checks")
    typer.echo("  2. Learn unit testing and coverage measurement")
    typer.echo("  3. Use learning mode to understand each tool")
    typer.echo("")

    typer.echo("[bold]🚀 For Intermediate Users:[/bold]")
    typer.echo("  1. Integrate tools into your development workflow")
    typer.echo("  2. Set up comprehensive testing strategies")
    typer.echo("  3. Use CI tools for automated quality assurance")
    typer.echo("")

    if verbosity_level.value >= 2:  # Comprehensive
        typer.echo("[bold]🏆 For Advanced Users:[/bold]")
        typer.echo("  1. Design custom workflows and automation")
        typer.echo("  2. Implement AI-assisted development patterns")
        typer.echo("  3. Optimize tools for team and organizational needs")
        typer.echo("")

    typer.echo(
        "Use `--category <name>` to see detailed paths for specific tool categories."
    )


@app.command("version")
def version() -> None:
    """Show version information."""
    typer.echo("ML Playground Tools v0.1.0")
    typer.echo("Unified development tooling for ML Playground")


@app.command("config")
def show_config() -> None:
    """Show current configuration."""
    _ensure_config_loaded()
    assert state.config is not None, "Config should be loaded after _ensure_config_loaded"
    
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
        sys.exit(1)
    except Exception as e:
        typer.echo(f"Unexpected error: {e}", err=True)
        sys.exit(1)


if __name__ == "__main__":
    main_entry()
