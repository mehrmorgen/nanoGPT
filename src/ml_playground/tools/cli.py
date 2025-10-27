"""Main CLI entry point for the ML Playground tools system.

This module provides the unified CLI accessible via `uv run tools`, organizing
all development tools under logical subcommands with learning mode support.
"""

import logging
import sys
from pathlib import Path
from typing import List, Optional

import typer
from typing_extensions import Annotated

from ml_playground.tools.categories.ci import CITools
from ml_playground.tools.categories.environment import EnvironmentTools
from ml_playground.tools.categories.quality import QualityTools
from ml_playground.tools.categories.testing import TestingTools
from ml_playground.tools.core.config import load_tools_config
from ml_playground.tools.core.errors import ToolConfigurationError, ToolExecutionError

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Main Typer app
app = typer.Typer(
    name="tools",
    help="ML Playground unified development tools",
    no_args_is_help=True,
    rich_markup_mode="rich",
)

# Subcommand groups (will be populated in later phases)
quality_app = typer.Typer(
    name="quality",
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
    help="CI/CD operations (quality gates, mutation testing, badges)",
    no_args_is_help=True,
)

agentic_app = typer.Typer(
    name="agentic",
    help="AI-assisted development tools (workflows, batch operations)",
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
app.add_typer(agentic_app, name="agentic")
app.add_typer(learn_app, name="learn")


# Global options and state
class GlobalState:
    """Global state for CLI options."""

    def __init__(self):
        self.learning_mode: bool = False
        self.verbosity: int = 1
        self.dry_run: bool = False
        self.project_root: Optional[Path] = None
        self.config = None


# Global state instance
state = GlobalState()


def load_config_with_error_handling(project_root: Path = None) -> None:
    """Load configuration with proper error handling."""
    try:
        state.config = load_tools_config(project_root)
        state.project_root = project_root

        # Apply default learning mode from config if not explicitly set
        if not hasattr(state, "_learning_mode_set"):
            state.learning_mode = state.config.learning_mode_default
            state.verbosity = state.config.default_verbosity

    except ToolConfigurationError as e:
        typer.echo(f"Configuration error: {e}", err=True)
        raise typer.Exit(1)
    except Exception as e:
        typer.echo(f"Unexpected error loading configuration: {e}", err=True)
        raise typer.Exit(1)


@app.callback()
def main(
    learning_mode: Annotated[
        bool,
        typer.Option(
            "--learning-mode/--no-learning-mode",
            help="Enable learning mode to show underlying commands and explanations",
        ),
    ] = None,
    verbosity: Annotated[
        int,
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
    # Load configuration first
    load_config_with_error_handling(project_root)

    # Set global options, preferring explicit CLI args over config defaults
    if learning_mode is not None:
        state.learning_mode = learning_mode
        state._learning_mode_set = True

    if verbosity is not None:
        state.verbosity = verbosity

    state.dry_run = dry_run


# Helper function to get tool instances
def _get_quality_tools() -> QualityTools:
    """Get quality tools instance."""
    if state.config is None:
        load_config_with_error_handling()
    return QualityTools(state.config, state.project_root or Path.cwd())


def _get_testing_tools() -> TestingTools:
    """Get testing tools instance."""
    if state.config is None:
        load_config_with_error_handling()
    return TestingTools(state.config, state.project_root or Path.cwd())


def _get_environment_tools() -> EnvironmentTools:
    """Get environment tools instance."""
    if state.config is None:
        load_config_with_error_handling()
    return EnvironmentTools(state.config, state.project_root or Path.cwd())


def _get_ci_tools() -> CITools:
    """Get CI tools instance."""
    if state.config is None:
        load_config_with_error_handling()
    return CITools(state.config, state.project_root or Path.cwd())


def _get_agentic_tools():
    """Get agentic tools instance."""
    from ml_playground.tools.categories.agentic import AgenticTools

    if state.config is None:
        load_config_with_error_handling()
    return AgenticTools(state.config, state.project_root or Path.cwd())


def _handle_tool_result(result) -> None:
    """Handle tool result and exit appropriately."""
    if result.stdout:
        typer.echo(result.stdout)
    if result.stderr:
        typer.echo(result.stderr, err=True)

    if not result.success:
        raise typer.Exit(result.exit_code)


# Quality commands
@quality_app.command("lint")
def quality_lint(
    args: Annotated[
        Optional[List[str]], typer.Argument(help="Additional ruff arguments")
    ] = None,
) -> None:
    """Run Ruff lint checks."""
    try:
        tools = _get_quality_tools()
        result = tools.lint(args or [])
        _handle_tool_result(result)
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
        tools = _get_quality_tools()
        result = tools.format(args or [])
        _handle_tool_result(result)
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
        tools = _get_quality_tools()
        result = tools.lint_check(args or [])
        _handle_tool_result(result)
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
        tools = _get_quality_tools()
        result = tools.deadcode(args or [])
        _handle_tool_result(result)
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
        tools = _get_quality_tools()
        result = tools.basedpyright(args or [])
        _handle_tool_result(result)
    except ToolExecutionError as e:
        typer.echo(f"Error: {e}", err=True)
        raise typer.Exit(1)


@quality_app.command("pyright")
def quality_pyright(
    args: Annotated[
        Optional[List[str]], typer.Argument(help="Additional basedpyright arguments")
    ] = None,
) -> None:
    """Run BasedPyright type checks (Pyright CLI alias)."""
    try:
        tools = _get_quality_tools()
        result = tools.pyright(args or [])
        _handle_tool_result(result)
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
        tools = _get_quality_tools()
        result = tools.mypy(args or [])
        _handle_tool_result(result)
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
        tools = _get_quality_tools()
        result = tools.typecheck(args or [])
        _handle_tool_result(result)
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
        tools = _get_quality_tools()
        result = tools.all_checks(args or [])
        _handle_tool_result(result)
    except ToolExecutionError as e:
        typer.echo(f"Error: {e}", err=True)
        raise typer.Exit(1)


# Testing commands
@test_app.command("unit")
def test_unit(
    args: Annotated[
        Optional[List[str]], typer.Argument(help="Additional pytest arguments")
    ] = None,
) -> None:
    """Run unit tests."""
    try:
        tools = _get_testing_tools()
        result = tools.unit(args or [])
        _handle_tool_result(result)
    except ToolExecutionError as e:
        typer.echo(f"Error: {e}", err=True)
        raise typer.Exit(1)


@test_app.command("integration")
def test_integration(
    args: Annotated[
        Optional[List[str]], typer.Argument(help="Additional pytest arguments")
    ] = None,
) -> None:
    """Run integration tests."""
    try:
        tools = _get_testing_tools()
        result = tools.integration(args or [])
        _handle_tool_result(result)
    except ToolExecutionError as e:
        typer.echo(f"Error: {e}", err=True)
        raise typer.Exit(1)


@test_app.command("e2e")
def test_e2e(
    args: Annotated[
        Optional[List[str]], typer.Argument(help="Additional pytest arguments")
    ] = None,
) -> None:
    """Run end-to-end tests."""
    try:
        tools = _get_testing_tools()
        result = tools.e2e(args or [])
        _handle_tool_result(result)
    except ToolExecutionError as e:
        typer.echo(f"Error: {e}", err=True)
        raise typer.Exit(1)


@test_app.command("acceptance")
def test_acceptance(
    args: Annotated[
        Optional[List[str]], typer.Argument(help="Additional pytest arguments")
    ] = None,
) -> None:
    """Run acceptance tests."""
    try:
        tools = _get_testing_tools()
        result = tools.acceptance(args or [])
        _handle_tool_result(result)
    except ToolExecutionError as e:
        typer.echo(f"Error: {e}", err=True)
        raise typer.Exit(1)


@test_app.command("property")
def test_property(
    args: Annotated[
        Optional[List[str]], typer.Argument(help="Additional pytest arguments")
    ] = None,
) -> None:
    """Run property-based tests."""
    try:
        tools = _get_testing_tools()
        result = tools.property_tests(args or [])
        _handle_tool_result(result)
    except ToolExecutionError as e:
        typer.echo(f"Error: {e}", err=True)
        raise typer.Exit(1)


@test_app.command("all")
def test_all(
    args: Annotated[
        Optional[List[str]], typer.Argument(help="Additional pytest arguments")
    ] = None,
) -> None:
    """Run all tests."""
    try:
        tools = _get_testing_tools()
        result = tools.all_tests(args or [])
        _handle_tool_result(result)
    except ToolExecutionError as e:
        typer.echo(f"Error: {e}", err=True)
        raise typer.Exit(1)


@test_app.command("coverage-test")
def test_coverage_test(
    args: Annotated[
        Optional[List[str]], typer.Argument(help="Additional arguments (ignored)")
    ] = None,
) -> None:
    """Run tests with coverage collection."""
    try:
        tools = _get_testing_tools()
        result = tools.coverage_test(args or [])
        _handle_tool_result(result)
    except ToolExecutionError as e:
        typer.echo(f"Error: {e}", err=True)
        raise typer.Exit(1)


@test_app.command("coverage-report")
def test_coverage_report(
    fail_under: Annotated[
        float,
        typer.Option(
            "--fail-under", help="Fail if total coverage is below this threshold"
        ),
    ] = 0.0,
    verbose: Annotated[
        bool, typer.Option("--verbose", help="Print discovered coverage artifacts")
    ] = False,
    args: Annotated[
        Optional[List[str]], typer.Argument(help="Additional arguments (ignored)")
    ] = None,
) -> None:
    """Generate coverage reports."""
    try:
        tools = _get_testing_tools()
        result = tools.coverage_report(
            args or [], fail_under=fail_under, verbose=verbose
        )
        _handle_tool_result(result)
    except ToolExecutionError as e:
        typer.echo(f"Error: {e}", err=True)
        raise typer.Exit(1)


@test_app.command("coverage-threshold")
def test_coverage_threshold(
    line_threshold: Annotated[
        float,
        typer.Option(
            "--line-threshold", help="Fail if line coverage is below this percentage"
        ),
    ] = 0.0,
    branch_threshold: Annotated[
        float,
        typer.Option(
            "--branch-threshold",
            help="Fail if branch coverage is below this percentage",
        ),
    ] = 0.0,
    verbose: Annotated[
        bool, typer.Option("--verbose", help="Print computed coverage totals")
    ] = False,
    args: Annotated[
        Optional[List[str]], typer.Argument(help="Additional arguments (ignored)")
    ] = None,
) -> None:
    """Check coverage thresholds."""
    try:
        tools = _get_testing_tools()
        result = tools.coverage_threshold(
            args or [],
            line_threshold=line_threshold,
            branch_threshold=branch_threshold,
            verbose=verbose,
        )
        _handle_tool_result(result)
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
        tools = _get_testing_tools()
        result = tools.clean(args or [])
        _handle_tool_result(result)
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
        tools = _get_environment_tools()
        result = tools.setup(args or [], clear=clear)
        _handle_tool_result(result)
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
        tools = _get_environment_tools()
        result = tools.sync(
            args or [], groups=groups, all_groups=all_groups, frozen=frozen
        )
        _handle_tool_result(result)
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
        tools = _get_environment_tools()
        result = tools.verify(args or [])
        _handle_tool_result(result)
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
        tools = _get_environment_tools()
        result = tools.clean(args or [])
        _handle_tool_result(result)
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
        tools = _get_environment_tools()
        result = tools.info(args or [])
        _handle_tool_result(result)
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
        tools = _get_environment_tools()
        result = tools.ai_guidelines(args or [], tool=tool, dry_run=dry_run)
        _handle_tool_result(result)
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
        tools = _get_environment_tools()
        result = tools.tensorboard(args or [], logdir=logdir, port=port, host=host)
        _handle_tool_result(result)
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
        tools = _get_environment_tools()
        result = tools.gguf_help(args or [])
        _handle_tool_result(result)
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
        tools = _get_ci_tools()
        result = tools.quality_gate(args or [])
        _handle_tool_result(result)
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
        tools = _get_ci_tools()
        result = tools.quality_fast(args or [])
        _handle_tool_result(result)
    except ToolExecutionError as e:
        typer.echo(f"Error: {e}", err=True)
        raise typer.Exit(1)


@ci_app.command("quality-ext")
def ci_quality_ext(
    args: Annotated[
        Optional[List[str]], typer.Argument(help="Additional arguments (ignored)")
    ] = None,
) -> None:
    """Run quality gates followed by mutation testing."""
    try:
        tools = _get_ci_tools()
        result = tools.quality_ext(args or [])
        _handle_tool_result(result)
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
        tools = _get_ci_tools()
        result = tools.quality_ci_local(args or [], bind_caches=bind_caches)
        _handle_tool_result(result)
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
        tools = _get_ci_tools()
        result = tools.coverage_badge(args or [])
        _handle_tool_result(result)
    except ToolExecutionError as e:
        typer.echo(f"Error: {e}", err=True)
        raise typer.Exit(1)


# Mutation testing subcommands
mutation_app = typer.Typer(
    name="mutation",
    help="Mutation testing operations",
    no_args_is_help=True,
)
ci_app.add_typer(mutation_app, name="mutation")


@mutation_app.command("reset")
def ci_mutation_reset(
    args: Annotated[
        Optional[List[str]], typer.Argument(help="Additional arguments (ignored)")
    ] = None,
) -> None:
    """Remove the cached Cosmic Ray session."""
    try:
        tools = _get_ci_tools()
        result = tools.mutation_reset(args or [])
        _handle_tool_result(result)
    except ToolExecutionError as e:
        typer.echo(f"Error: {e}", err=True)
        raise typer.Exit(1)


@mutation_app.command("summary")
def ci_mutation_summary(
    args: Annotated[
        Optional[List[str]], typer.Argument(help="Additional arguments (ignored)")
    ] = None,
) -> None:
    """Show a summary of the previous Cosmic Ray run."""
    try:
        tools = _get_ci_tools()
        result = tools.mutation_summary(args or [])
        _handle_tool_result(result)
    except ToolExecutionError as e:
        typer.echo(f"Error: {e}", err=True)
        raise typer.Exit(1)


@mutation_app.command("init")
def ci_mutation_init(
    args: Annotated[
        Optional[List[str]], typer.Argument(help="Additional arguments (ignored)")
    ] = None,
) -> None:
    """Initialize the Cosmic Ray session database if needed."""
    try:
        tools = _get_ci_tools()
        result = tools.mutation_init(args or [])
        _handle_tool_result(result)
    except ToolExecutionError as e:
        typer.echo(f"Error: {e}", err=True)
        raise typer.Exit(1)


@mutation_app.command("exec")
def ci_mutation_exec(
    args: Annotated[
        Optional[List[str]], typer.Argument(help="Additional arguments (ignored)")
    ] = None,
) -> None:
    """Execute mutation tests with Cosmic Ray."""
    try:
        tools = _get_ci_tools()
        result = tools.mutation_exec(args or [])
        _handle_tool_result(result)
    except ToolExecutionError as e:
        typer.echo(f"Error: {e}", err=True)
        raise typer.Exit(1)


@mutation_app.command("report")
def ci_mutation_report(
    args: Annotated[
        Optional[List[str]], typer.Argument(help="Additional arguments (ignored)")
    ] = None,
) -> None:
    """Render a mutation testing report."""
    try:
        tools = _get_ci_tools()
        result = tools.mutation_report(args or [])
        _handle_tool_result(result)
    except ToolExecutionError as e:
        typer.echo(f"Error: {e}", err=True)
        raise typer.Exit(1)


@mutation_app.command("run")
def ci_mutation_run(
    args: Annotated[
        Optional[List[str]], typer.Argument(help="Additional arguments (ignored)")
    ] = None,
) -> None:
    """Run the full mutation testing pipeline."""
    try:
        tools = _get_ci_tools()
        result = tools.mutation_run(args or [])
        _handle_tool_result(result)
    except ToolExecutionError as e:
        typer.echo(f"Error: {e}", err=True)
        raise typer.Exit(1)


# Agentic commands
@agentic_app.command("guidelines-setup")
def agentic_guidelines_setup(
    args: Annotated[
        Optional[List[str]],
        typer.Argument(help="Additional arguments for guideline setup"),
    ] = None,
) -> None:
    """Set up AI development guidelines and configuration."""
    try:
        tools = _get_agentic_tools()
        result = tools.guidelines_setup(
            args or [],
            learning_mode=state.learning_mode,
            verbosity_level=state.verbosity,
        )
        _handle_tool_result(result)
    except ToolExecutionError as e:
        typer.echo(f"Error: {e}", err=True)
        raise typer.Exit(1)


@agentic_app.command("batch-review")
def agentic_batch_review(
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
        tools = _get_agentic_tools()
        result = tools.batch_review(
            args or [],
            output_format=output_format,
            learning_mode=state.learning_mode,
            verbosity_level=state.verbosity,
        )
        _handle_tool_result(result)
    except ToolExecutionError as e:
        typer.echo(f"Error: {e}", err=True)
        raise typer.Exit(1)


@agentic_app.command("workflow-helper")
def agentic_workflow_helper(
    workflow_type: Annotated[
        str, typer.Option("--type", help="Workflow type (standard, strict, minimal)")
    ] = "standard",
    args: Annotated[
        Optional[List[str]],
        typer.Argument(help="Additional arguments for workflow generation"),
    ] = None,
) -> None:
    """Provide workflow helpers for common AI development patterns."""
    try:
        tools = _get_agentic_tools()
        result = tools.workflow_helper(
            args or [],
            workflow_type=workflow_type,
            learning_mode=state.learning_mode,
            verbosity_level=state.verbosity,
        )
        _handle_tool_result(result)
    except ToolExecutionError as e:
        typer.echo(f"Error: {e}", err=True)
        raise typer.Exit(1)


@agentic_app.command("batch-quality")
def agentic_batch_quality(
    output_format: Annotated[
        str, typer.Option("--format", help="Output format (json, yaml, text)")
    ] = "json",
    args: Annotated[
        Optional[List[str]],
        typer.Argument(help="Additional arguments for quality checks"),
    ] = None,
) -> None:
    """Run automated quality checks for AI agent consumption."""
    try:
        tools = _get_agentic_tools()
        result = tools.batch_quality(
            args or [],
            output_format=output_format,
            learning_mode=state.learning_mode,
            verbosity_level=state.verbosity,
        )
        _handle_tool_result(result)
    except ToolExecutionError as e:
        typer.echo(f"Error: {e}", err=True)
        raise typer.Exit(1)


@agentic_app.command("batch-validate")
def agentic_batch_validate(
    validation_level: Annotated[
        str,
        typer.Option("--level", help="Validation level (minimal, standard, strict)"),
    ] = "standard",
    output_format: Annotated[
        str, typer.Option("--format", help="Output format (json, yaml, text)")
    ] = "json",
    args: Annotated[
        Optional[List[str]], typer.Argument(help="Additional arguments for validation")
    ] = None,
) -> None:
    """Run comprehensive validation for AI-assisted development."""
    try:
        tools = _get_agentic_tools()
        result = tools.batch_validate(
            args or [],
            validation_level=validation_level,
            output_format=output_format,
            learning_mode=state.learning_mode,
            verbosity_level=state.verbosity,
        )
        _handle_tool_result(result)
    except ToolExecutionError as e:
        typer.echo(f"Error: {e}", err=True)
        raise typer.Exit(1)


@agentic_app.command("workflow-status")
def agentic_workflow_status(
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
        tools = _get_agentic_tools()
        result = tools.workflow_status(
            args or [],
            output_format=output_format,
            learning_mode=state.learning_mode,
            verbosity_level=state.verbosity,
        )
        _handle_tool_result(result)
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
    command_catalog = {
        "quality": {
            "description": "Code quality tools (lint, format, typecheck)",
            "commands": {
                "lint": "Run Ruff lint checks for code style and potential issues",
                "format": "Auto-fix and format code with Ruff",
                "lint-check": "Run Ruff in check-only mode (alias for lint)",
                "deadcode": "Scan for unused code using vulture",
                "basedpyright": "Run BasedPyright type checks",
                "pyright": "Run BasedPyright type checks (Pyright CLI alias)",
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
        "agentic": {
            "description": "AI-assisted development tools",
            "commands": {
                "guidelines-setup": "Set up AI development guidelines",
                "batch-review": "Perform batch review operations for AI consumption",
                "workflow-helper": "Provide workflow helpers for AI development patterns",
                "batch-quality": "Run automated quality checks for AI agents",
                "batch-validate": "Run comprehensive validation for AI decision-making",
                "workflow-status": "Get current workflow status for AI analysis",
            },
            "examples": [
                "uv run tools agentic guidelines-setup",
                "uv run tools agentic batch-review --format json",
                "uv run tools agentic workflow-status",
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
    valid_categories = {"quality", "test", "env", "ci", "agentic"}
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
        valid_categories = {"quality", "test", "env", "ci", "agentic"}
        if category not in valid_categories:
            typer.echo(
                f"Error: Unknown category '{category}'. Valid categories: {', '.join(valid_categories)}",
                err=True,
            )
            raise typer.Exit(1)

        typer.echo(f"[bold]Best Practices for {category.title()} Tools[/bold]")
        typer.echo("")

        category_practices = engine._get_category_best_practices(category)
        for practice in category_practices:
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
        categories = ["quality", "test", "env", "ci", "agentic"]
        for cat in categories:
            typer.echo(f"[bold]{cat.title()} Tools:[/bold]")
            cat_practices = engine._get_category_best_practices(cat)
            for practice in cat_practices[:2]:  # Show top 2 practices per category
                typer.echo(f"  • {practice}")
            typer.echo("")

        # Show learning paths
        typer.echo("[bold]📚 Learning Paths[/bold]")
        typer.echo("")
        _show_learning_paths_overview(verbosity_level)


def _show_learning_paths(category: str, verbosity_level) -> None:
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
        "agentic": {
            "beginner": [
                "Set up AI guidelines for consistent workflows",
                "Use batch operations for AI-assisted development",
                "Understand AI workflow helpers and templates",
                "Learn about structured output formats for AI",
            ],
            "intermediate": [
                "Implement AI-driven code review processes",
                "Use workflow status for AI decision-making",
                "Combine AI tools with human oversight",
                "Optimize AI workflows for your team's needs",
            ],
            "advanced": [
                "Design comprehensive AI-assisted development pipelines",
                "Implement custom AI workflow patterns",
                "Use AI tools for automated quality assurance",
                "Build AI-driven development decision systems",
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


def _show_learning_paths_overview(verbosity_level) -> None:
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
    if state.config is None:
        load_config_with_error_handling()

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
