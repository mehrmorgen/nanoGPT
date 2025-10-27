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
        if not hasattr(state, '_learning_mode_set'):
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
            help="Enable learning mode to show underlying commands and explanations"
        )
    ] = None,
    verbosity: Annotated[
        int,
        typer.Option(
            "--verbosity", "-v",
            min=0, max=2,
            help="Learning mode verbosity: 0=minimal, 1=standard, 2=comprehensive"
        )
    ] = None,
    dry_run: Annotated[
        bool,
        typer.Option(
            "--dry-run",
            help="Show what would be done without executing commands"
        )
    ] = False,
    project_root: Annotated[
        Optional[Path],
        typer.Option(
            "--project-root",
            help="Path to project root (auto-detected if not specified)"
        )
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
        Optional[List[str]],
        typer.Argument(help="Additional ruff arguments")
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
        Optional[List[str]],
        typer.Argument(help="Additional ruff arguments")
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
        Optional[List[str]],
        typer.Argument(help="Additional ruff arguments")
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
        Optional[List[str]],
        typer.Argument(help="Additional vulture arguments")
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
        Optional[List[str]],
        typer.Argument(help="Additional basedpyright arguments")
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
        Optional[List[str]],
        typer.Argument(help="Additional basedpyright arguments")
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
        Optional[List[str]],
        typer.Argument(help="Additional mypy arguments")
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
        typer.Argument(help="Additional arguments (applied to both tools)")
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
        typer.Argument(help="Additional arguments (applied to all tools)")
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
        Optional[List[str]],
        typer.Argument(help="Additional pytest arguments")
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
        Optional[List[str]],
        typer.Argument(help="Additional pytest arguments")
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
        Optional[List[str]],
        typer.Argument(help="Additional pytest arguments")
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
        Optional[List[str]],
        typer.Argument(help="Additional pytest arguments")
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
        Optional[List[str]],
        typer.Argument(help="Additional pytest arguments")
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
        Optional[List[str]],
        typer.Argument(help="Additional pytest arguments")
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
        Optional[List[str]],
        typer.Argument(help="Additional arguments (ignored)")
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
        typer.Option("--fail-under", help="Fail if total coverage is below this threshold")
    ] = 0.0,
    verbose: Annotated[
        bool,
        typer.Option("--verbose", help="Print discovered coverage artifacts")
    ] = False,
    args: Annotated[
        Optional[List[str]],
        typer.Argument(help="Additional arguments (ignored)")
    ] = None,
) -> None:
    """Generate coverage reports."""
    try:
        tools = _get_testing_tools()
        result = tools.coverage_report(args or [], fail_under=fail_under, verbose=verbose)
        _handle_tool_result(result)
    except ToolExecutionError as e:
        typer.echo(f"Error: {e}", err=True)
        raise typer.Exit(1)


@test_app.command("coverage-threshold")
def test_coverage_threshold(
    line_threshold: Annotated[
        float,
        typer.Option("--line-threshold", help="Fail if line coverage is below this percentage")
    ] = 0.0,
    branch_threshold: Annotated[
        float,
        typer.Option("--branch-threshold", help="Fail if branch coverage is below this percentage")
    ] = 0.0,
    verbose: Annotated[
        bool,
        typer.Option("--verbose", help="Print computed coverage totals")
    ] = False,
    args: Annotated[
        Optional[List[str]],
        typer.Argument(help="Additional arguments (ignored)")
    ] = None,
) -> None:
    """Check coverage thresholds."""
    try:
        tools = _get_testing_tools()
        result = tools.coverage_threshold(
            args or [], 
            line_threshold=line_threshold, 
            branch_threshold=branch_threshold,
            verbose=verbose
        )
        _handle_tool_result(result)
    except ToolExecutionError as e:
        typer.echo(f"Error: {e}", err=True)
        raise typer.Exit(1)


@test_app.command("clean")
def test_clean(
    args: Annotated[
        Optional[List[str]],
        typer.Argument(help="Additional arguments (ignored)")
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
        bool,
        typer.Option("--clear", help="Remove existing virtual environment first")
    ] = False,
    args: Annotated[
        Optional[List[str]],
        typer.Argument(help="Additional arguments (ignored)")
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
        typer.Option("--group", help="Sync specific dependency groups (repeatable)")
    ] = None,
    all_groups: Annotated[
        bool,
        typer.Option("--all-groups", help="Install all optional dependency groups")
    ] = False,
    frozen: Annotated[
        bool,
        typer.Option("--frozen", help="Use existing lockfile without resolving new versions")
    ] = False,
    args: Annotated[
        Optional[List[str]],
        typer.Argument(help="Additional uv sync arguments")
    ] = None,
) -> None:
    """Sync project dependencies using uv."""
    try:
        tools = _get_environment_tools()
        result = tools.sync(args or [], groups=groups, all_groups=all_groups, frozen=frozen)
        _handle_tool_result(result)
    except ToolExecutionError as e:
        typer.echo(f"Error: {e}", err=True)
        raise typer.Exit(1)


@env_app.command("verify")
def env_verify(
    args: Annotated[
        Optional[List[str]],
        typer.Argument(help="Additional arguments (ignored)")
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
        Optional[List[str]],
        typer.Argument(help="Additional arguments (ignored)")
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
        Optional[List[str]],
        typer.Argument(help="Additional arguments (ignored)")
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
    tool: Annotated[
        str,
        typer.Argument(help="Target tool name for AI guidelines")
    ],
    dry_run: Annotated[
        bool,
        typer.Option("--dry-run", help="Preview actions without executing")
    ] = False,
    args: Annotated[
        Optional[List[str]],
        typer.Argument(help="Additional arguments (ignored)")
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
            help="TensorBoard log directory"
        )
    ],
    port: Annotated[
        int,
        typer.Option("--port", help="Port to bind TensorBoard to")
    ] = 6006,
    host: Annotated[
        str,
        typer.Option("--host", help="Host interface to bind to")
    ] = "127.0.0.1",
    args: Annotated[
        Optional[List[str]],
        typer.Argument(help="Additional tensorboard arguments")
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
        Optional[List[str]],
        typer.Argument(help="Additional arguments (ignored)")
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
        Optional[List[str]],
        typer.Argument(help="Additional pre-commit arguments")
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
        Optional[List[str]],
        typer.Argument(help="Additional pre-commit arguments")
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
        Optional[List[str]],
        typer.Argument(help="Additional arguments (ignored)")
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
            help="Bind local caches into the act container"
        )
    ] = True,
    args: Annotated[
        Optional[List[str]],
        typer.Argument(help="Additional act arguments")
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
        Optional[List[str]],
        typer.Argument(help="Additional arguments (ignored)")
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
        Optional[List[str]],
        typer.Argument(help="Additional arguments (ignored)")
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
        Optional[List[str]],
        typer.Argument(help="Additional arguments (ignored)")
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
        Optional[List[str]],
        typer.Argument(help="Additional arguments (ignored)")
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
        Optional[List[str]],
        typer.Argument(help="Additional arguments (ignored)")
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
        Optional[List[str]],
        typer.Argument(help="Additional arguments (ignored)")
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
        Optional[List[str]],
        typer.Argument(help="Additional arguments (ignored)")
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


@agentic_app.command("guidelines")
def agentic_guidelines() -> None:
    """Set up AI guidelines (placeholder - will be implemented in phase 5)."""
    typer.echo("Agentic guidelines command - not yet implemented")
    typer.echo("This will be implemented in phase 5: Agentic Tools")


@learn_app.command("commands")
def learn_commands() -> None:
    """Show available commands with descriptions."""
    typer.echo("Available tool categories:")
    typer.echo("")
    typer.echo("  [bold]quality[/bold]   - Code quality tools (lint, format, typecheck)")
    typer.echo("  [bold]test[/bold]      - Testing tools (unit, integration, e2e, coverage)")
    typer.echo("  [bold]env[/bold]       - Environment management (setup, sync, clean)")
    typer.echo("  [bold]ci[/bold]        - CI/CD operations (quality gates, mutation testing)")
    typer.echo("  [bold]agentic[/bold]   - AI-assisted development tools")
    typer.echo("  [bold]learn[/bold]     - Learning mode utilities")
    typer.echo("")
    typer.echo("Use [bold]uv run tools <category> --help[/bold] to see commands in each category.")
    typer.echo("Use [bold]--learning-mode[/bold] with any command to see explanations.")


@learn_app.command("explain")
def learn_explain(
    command: Annotated[
        str,
        typer.Argument(help="Command to explain (e.g., 'quality.lint', 'test.unit')")
    ]
) -> None:
    """Explain a specific command (placeholder - will be implemented in phase 4)."""
    typer.echo(f"Explanation for '{command}' - not yet implemented")
    typer.echo("This will be implemented in phase 4: Learning Mode Infrastructure")


@learn_app.command("best-practices")
def learn_best_practices() -> None:
    """Show best practices guide (placeholder - will be implemented in phase 4)."""
    typer.echo("Best practices guide - not yet implemented")
    typer.echo("This will be implemented in phase 4: Learning Mode Infrastructure")


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
    typer.echo(f"  Quality tools: {'enabled' if state.config.quality.enabled else 'disabled'}")
    typer.echo(f"  Testing tools: {'enabled' if state.config.testing.enabled else 'disabled'}")
    typer.echo(f"  Environment tools: {'enabled' if state.config.environment.enabled else 'disabled'}")
    typer.echo(f"  CI tools: {'enabled' if state.config.ci.enabled else 'disabled'}")
    typer.echo(f"  Agentic tools: {'enabled' if state.config.agentic.enabled else 'disabled'}")


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