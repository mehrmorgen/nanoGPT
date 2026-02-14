"""Testing command implementations for the tools CLI system."""

from typing import Callable, List, Optional, cast

import typer
from typing_extensions import Annotated

# Import shared utilities
from ml_playground.tools.cli.state import state
from ml_playground.tools.cli import helpers
from ml_playground.tools.core.interfaces import ToolResult
from ml_playground.tools.testing.testing import TestingTools


def get_testing_tools() -> TestingTools:
    return helpers.get_testing_tools()


def run_tool_command(
    command_func: Callable[..., ToolResult], *args: object, **kwargs: object
) -> None:
    helpers.run_tool_command(command_func, *args, **kwargs)


# Create testing app
test_app = typer.Typer(
    name="test",
    help="Testing tools (unit, integration, e2e, coverage)",
    no_args_is_help=True,
)


def _invoke_tests(
    ctx: typer.Context,
    test_dir: str,
    pattern: str | None,
    extra_args: list[str],
) -> None:
    tools = get_testing_tools()
    args = list(extra_args)
    if pattern:
        args.extend(["-k", pattern])

    # Map test directories to method names
    suite_map = {
        "tests/unit": "unit",
        "tests/property": "property_tests",
        "tests/regression": "regression",
        "tests/integration": "integration",
        "tests/e2e": "e2e",
        "tests/acceptance": "acceptance",
    }
    method_name = suite_map.get(test_dir)
    if method_name is None:
        raise Exception(f"Unsupported test suite: {test_dir}")

    suite_fn = getattr(tools, method_name)
    suite_call = cast(Callable[..., ToolResult], suite_fn)
    run_tool_command(
        suite_call,
        args,
        learning_mode=state.learning_mode,
        verbosity_level=state.verbosity,
    )


@test_app.command("acceptance")
def test_acceptance(
    ctx: typer.Context,
    pattern: Annotated[str | None, typer.Argument()] = None,
    extra_args: Annotated[list[str] | None, typer.Argument()] = None,
) -> None:
    """Run acceptance tests."""
    _invoke_tests(ctx, "tests/acceptance", pattern, list(extra_args or []))


@test_app.command("all")
def test_all(
    args: Annotated[
        Optional[List[str]], typer.Argument(help="Additional pytest arguments")
    ] = None,
) -> None:
    """Run all tests."""
    tools = get_testing_tools()
    run_tool_command(
        tools.all_tests,
        args or [],
        learning_mode=state.learning_mode,
        verbosity_level=state.verbosity,
    )


@test_app.command("clean")
def test_clean(
    args: Annotated[
        Optional[List[str]], typer.Argument(help="Additional arguments (ignored)")
    ] = None,
) -> None:
    """Clean test artifacts and caches."""
    tools = get_testing_tools()
    run_tool_command(
        tools.clean,
        args or [],
        learning_mode=state.learning_mode,
        verbosity_level=state.verbosity,
    )


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
        typer.Option("--force-regen", help="Force regeneration of coverage data"),
    ] = False,
    verbose: Annotated[
        bool, typer.Option("--verbose", help="Show verbose artifacts")
    ] = False,
    args: Annotated[
        Optional[List[str]], typer.Argument(help="Additional arguments (ignored)")
    ] = None,
) -> None:
    """Run coverage analysis."""
    tools = get_testing_tools()
    run_tool_command(
        tools.coverage,
        args or [],
        line_threshold=line_threshold or 0.0,
        branch_threshold=branch_threshold or 0.0,
        verbose=verbose,
        learning_mode=state.learning_mode,
        verbosity_level=state.verbosity,
        force_regen=force_regen,
    )


@test_app.command("e2e")
def test_e2e(
    ctx: typer.Context,
    pattern: Annotated[str | None, typer.Argument()] = None,
    extra_args: Annotated[list[str] | None, typer.Argument()] = None,
) -> None:
    """Run end-to-end tests."""
    _invoke_tests(ctx, "tests/e2e", pattern, list(extra_args or []))


@test_app.command("integration")
def test_integration(
    ctx: typer.Context,
    pattern: Annotated[str | None, typer.Argument()] = None,
    extra_args: Annotated[list[str] | None, typer.Argument()] = None,
) -> None:
    """Run integration tests."""
    _invoke_tests(ctx, "tests/integration", pattern, list(extra_args or []))


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


@test_app.command("unit")
def test_unit(
    ctx: typer.Context,
    pattern: Annotated[str | None, typer.Argument()] = None,
    extra_args: Annotated[list[str] | None, typer.Argument()] = None,
) -> None:
    """Run unit tests."""
    _invoke_tests(ctx, "tests/unit", pattern, list(extra_args or []))
