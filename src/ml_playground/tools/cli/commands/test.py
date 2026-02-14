from __future__ import annotations

from typing import Optional, cast, Callable

import typer
from typing_extensions import Annotated

from ml_playground.tools.core.interfaces import ToolResult
from ml_playground.tools.core.errors import ToolExecutionError

from .. import helpers as cli_helpers
from ..state import state


def _invoke_tests(
    test_dir: str,
    pattern: str | None,
    extra_args: list[str],
) -> None:
    try:
        suite_map = {
            "tests/unit": "unit",
            "tests/property": "property_tests",
            "tests/regression": "regression",
            "tests/integration": "integration",
            "tests/acceptance": "acceptance",
            "tests/e2e": "e2e",
        }
        method_name = suite_map.get(test_dir)
        if method_name is None:
            raise ToolExecutionError(
                f"Unsupported test suite: {test_dir}",
                reason="No registered TestingTools handler",
                rationale=(
                    "Add a dedicated TestingTools method for this suite or update CLI dispatch."
                ),
            )

        tools = cli_helpers.get_testing_tools()
        args = list(extra_args)
        if pattern:
            args.extend(["-k", pattern])

        # Cast tools to object and use getattr to break Any tracking
        tools_obj: object = cast(object, tools)
        suite_fn_raw = cast(object, getattr(tools_obj, method_name))
        if not callable(suite_fn_raw):
            raise ToolExecutionError(
                f"Method {method_name} is not callable",
                reason="Dynamic method lookup failed",
                rationale="The requested test suite handler was found but is not a callable method.",
            )
        suite_fn: Callable[..., object] = suite_fn_raw

        # Capture the result and cast it to ToolResult to satisfy basedpyright
        raw_result: object = suite_fn(
            args,
            learning_mode=state.learning_mode,
            verbosity_level=state.verbosity,
        )
        result = cast(ToolResult, raw_result)
        cli_helpers.handle_tool_result(result)
    except ToolExecutionError as exc:
        typer.echo(f"Error: {exc}", err=True)
        raise typer.Exit(1)


def build_app() -> typer.Typer:
    app = typer.Typer(
        name="test",
        help="Testing tools (unit, property, regression, integration, coverage, mutation)",
        no_args_is_help=True,
    )

    @app.command("coverage")
    def test_coverage(  # pyright: ignore[reportUnusedFunction]
        line_threshold: Annotated[
            float,
            typer.Option("--line-threshold", help="Minimum line coverage (0 = config)"),
        ] = 0.0,
        branch_threshold: Annotated[
            float,
            typer.Option(
                "--branch-threshold", help="Minimum branch coverage (0 = config)"
            ),
        ] = 0.0,
        force_regen: Annotated[
            bool,
            typer.Option("--force-regen", help="Force regenerating coverage data"),
        ] = False,
        verbose: Annotated[
            bool, typer.Option("--verbose", help="Show verbose artifacts")
        ] = False,
        args: Annotated[
            Optional[list[str]], typer.Argument(help="Additional arguments (ignored)")
        ] = None,
    ) -> None:
        """Run full coverage pipeline (report + threshold) in one command."""

        try:
            tools = cli_helpers.get_testing_tools()
            result = tools.coverage(
                args or [],
                line_threshold=line_threshold or 0.0,
                branch_threshold=branch_threshold or 0.0,
                verbose=verbose,
                learning_mode=state.learning_mode,
                verbosity_level=state.verbosity,
                force_regen=force_regen,
            )
            cli_helpers.handle_tool_result(result)
        except ToolExecutionError as exc:
            typer.echo(f"Error: {exc}", err=True)
            raise typer.Exit(1)

    @app.command("coverage-map")
    def test_coverage_map(  # pyright: ignore[reportUnusedFunction]
        force_regen: Annotated[
            bool,
            typer.Option("--force-regen", help="Force regenerating coverage data"),
        ] = False,
        verbose: Annotated[
            bool, typer.Option("--verbose", help="Show verbose artifacts")
        ] = False,
        args: Annotated[
            Optional[list[str]], typer.Argument(help="Additional arguments (ignored)")
        ] = None,
    ) -> None:
        """Report coverage gaps with suite hints."""
        try:
            tools = cli_helpers.get_testing_tools()
            result = tools.coverage_map(
                args or [],
                verbose=verbose,
                learning_mode=state.learning_mode,
                verbosity_level=state.verbosity,
                force_regen=force_regen,
            )
            cli_helpers.handle_tool_result(result)
        except ToolExecutionError as exc:
            typer.echo(f"Error: {exc}", err=True)
            raise typer.Exit(1)

    @app.command("budget")
    def test_budget(  # pyright: ignore[reportUnusedFunction]
        refresh: Annotated[
            bool,
            typer.Option("--refresh", help="Refresh budgets by running suites"),
        ] = False,
        args: Annotated[
            Optional[list[str]], typer.Argument(help="Additional arguments (ignored)")
        ] = None,
    ) -> None:
        """Report integration/e2e runtime budgets."""
        try:
            tools = cli_helpers.get_testing_tools()
            result = tools.budget_report(args or [], refresh=refresh)
            cli_helpers.handle_tool_result(result)
        except ToolExecutionError as exc:
            typer.echo(f"Error: {exc}", err=True)
            raise typer.Exit(1)

    @app.command("unit")
    def test_unit(  # pyright: ignore[reportUnusedFunction]
        pattern: Annotated[str | None, typer.Argument()] = None,
        extra_args: Annotated[list[str] | None, typer.Argument()] = None,
    ) -> None:
        """Run unit tests."""
        _invoke_tests("tests/unit", pattern, list(extra_args or []))

    @app.command("property")
    def test_property(  # pyright: ignore[reportUnusedFunction]
        pattern: Annotated[str | None, typer.Argument()] = None,
        extra_args: Annotated[list[str] | None, typer.Argument()] = None,
    ) -> None:
        """Run property-based tests using Hypothesis."""

        _invoke_tests("tests/property", pattern, list(extra_args or []))

    @app.command("regression")
    def test_regression(  # pyright: ignore[reportUnusedFunction]
        pattern: Annotated[str | None, typer.Argument()] = None,
        extra_args: Annotated[Optional[list[str]], typer.Argument()] = None,
    ) -> None:
        """Run regression suites (policy guards, slow checks)."""

        _invoke_tests("tests/regression", pattern, list(extra_args or []))

    @app.command("integration")
    def test_integration(  # pyright: ignore[reportUnusedFunction]
        pattern: Annotated[str | None, typer.Argument()] = None,
        extra_args: Annotated[list[str] | None, typer.Argument()] = None,
    ) -> None:
        """Run integration tests."""

        _invoke_tests("tests/integration", pattern, list(extra_args or []))

    @app.command("acceptance")
    def test_acceptance(  # pyright: ignore[reportUnusedFunction]
        pattern: Annotated[str | None, typer.Argument()] = None,
        extra_args: Annotated[list[str] | None, typer.Argument()] = None,
    ) -> None:
        """Run acceptance tests."""

        _invoke_tests("tests/acceptance", pattern, list(extra_args or []))

    @app.command("e2e")
    def test_e2e(  # pyright: ignore[reportUnusedFunction]
        pattern: Annotated[str | None, typer.Argument()] = None,
        extra_args: Annotated[list[str] | None, typer.Argument()] = None,
    ) -> None:
        """Run end-to-end tests."""

        _invoke_tests("tests/e2e", pattern, list(extra_args or []))

    @app.command("all")
    def test_all(  # pyright: ignore[reportUnusedFunction]
        args: Annotated[
            Optional[list[str]], typer.Argument(help="Additional pytest arguments")
        ] = None,
    ) -> None:
        """Run all tests."""
        try:
            tools = cli_helpers.get_testing_tools()
            result = tools.all_tests(
                args or [],
                learning_mode=state.learning_mode,
                verbosity_level=state.verbosity,
            )
            cli_helpers.handle_tool_result(result)
        except ToolExecutionError as exc:
            typer.echo(f"Error: {exc}", err=True)
            raise typer.Exit(1)

    @app.command("clean")
    def test_clean(  # pyright: ignore[reportUnusedFunction]
        args: Annotated[
            Optional[list[str]], typer.Argument(help="Additional arguments (ignored)")
        ] = None,
    ) -> None:
        """Clean test artifacts and caches."""
        try:
            tools = cli_helpers.get_testing_tools()
            result = tools.clean(
                args or [],
                learning_mode=state.learning_mode,
                verbosity_level=state.verbosity,
            )
            cli_helpers.handle_tool_result(result)
        except ToolExecutionError as exc:
            typer.echo(f"Error: {exc}", err=True)
            raise typer.Exit(1)

    mutation_app = typer.Typer(
        name="mutation",
        help="Mutation testing operations",
        no_args_is_help=True,
    )
    app.add_typer(mutation_app, name="mutation")

    @mutation_app.command("reset")
    def test_mutation_reset(  # pyright: ignore[reportUnusedFunction]
        args: Annotated[
            Optional[list[str]], typer.Argument(help="Additional arguments (ignored)")
        ] = None,
    ) -> None:
        """Remove the cached Cosmic Ray session."""
        try:
            tools = cli_helpers.get_testing_tools()
            result = tools.mutation_reset(args or [])
            cli_helpers.handle_tool_result(result)
        except ToolExecutionError as exc:
            typer.echo(f"Error: {exc}", err=True)
            raise typer.Exit(1)

    @mutation_app.command("summary")
    def test_mutation_summary(  # pyright: ignore[reportUnusedFunction]
        args: Annotated[
            Optional[list[str]], typer.Argument(help="Additional arguments (ignored)")
        ] = None,
    ) -> None:
        """Show a summary of the current Cosmic Ray configuration."""
        try:
            tools = cli_helpers.get_testing_tools()
            result = tools.mutation_summary(args or [])
            cli_helpers.handle_tool_result(result)
        except ToolExecutionError as exc:
            typer.echo(f"Error: {exc}", err=True)
            raise typer.Exit(1)

    @mutation_app.command("init")
    def test_mutation_init(  # pyright: ignore[reportUnusedFunction]
        args: Annotated[
            Optional[list[str]], typer.Argument(help="Additional arguments (ignored)")
        ] = None,
    ) -> None:
        """Initialize the Cosmic Ray session database if needed."""
        try:
            tools = cli_helpers.get_testing_tools()
            result = tools.mutation_init(args or [])
            cli_helpers.handle_tool_result(result)
        except ToolExecutionError as exc:
            typer.echo(f"Error: {exc}", err=True)
            raise typer.Exit(1)

    @mutation_app.command("exec")
    def test_mutation_exec(  # pyright: ignore[reportUnusedFunction]
        args: Annotated[
            Optional[list[str]], typer.Argument(help="Additional pytest arguments")
        ] = None,
    ) -> None:
        """Run mutation tests using Cosmic Ray."""
        try:
            tools = cli_helpers.get_testing_tools()
            result = tools.mutation_exec(args or [])
            cli_helpers.handle_tool_result(result)
        except ToolExecutionError as exc:
            typer.echo(f"Error: {exc}", err=True)
            raise typer.Exit(1)

    @mutation_app.command("report")
    def test_mutation_report(  # pyright: ignore[reportUnusedFunction]
        args: Annotated[
            Optional[list[str]], typer.Argument(help="Additional arguments (ignored)")
        ] = None,
    ) -> None:
        """Generate a mutation testing report."""
        try:
            tools = cli_helpers.get_testing_tools()
            result = tools.mutation_report(args or [])
            cli_helpers.handle_tool_result(result)
        except ToolExecutionError as exc:
            typer.echo(f"Error: {exc}", err=True)
            raise typer.Exit(1)

    @mutation_app.command("run")
    def test_mutation_run(  # pyright: ignore[reportUnusedFunction]
        args: Annotated[
            Optional[list[str]], typer.Argument(help="Additional arguments (ignored)")
        ] = None,
    ) -> None:
        """Show equivalent shell commands for the selected test suite."""
        try:
            tools = cli_helpers.get_testing_tools()
            result = tools.mutation_run(args or [])
            cli_helpers.handle_tool_result(result)
        except ToolExecutionError as exc:
            typer.echo(f"Error: {exc}", err=True)
            raise typer.Exit(1)

    return app


# Expose a module-level app for static imports in the CLI aggregator.
app = build_app()


__all__ = ["app", "build_app", "_invoke_tests"]
