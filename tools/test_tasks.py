#!/usr/bin/env -S uv run --no-project
# /// script
# dependencies = [
#   "typer>=0.12.3",
# ]
# ///
"""Test orchestration commands for ml_playground."""

from __future__ import annotations

import typer

from tools import task_utils as utils

app = typer.Typer(help="Test runners executed via uv run.", no_args_is_help=True)


@app.command()
def pytest(*args: str) -> None:
    """Invoke pytest with the shared configuration."""
    _ = utils.uv_run(*utils.pytest_command(utils.forwarded_args(args)))


@app.command()
def test(*args: str) -> None:
    """Run the full test suite."""
    _ = utils.uv_run(*utils.pytest_command(["tests", *utils.forwarded_args(args)]))


@app.command()
def unit(*args: str) -> None:
    """Run unit tests."""
    _ = utils.uv_run(*utils.pytest_command(["tests/unit", *utils.forwarded_args(args)]))


@app.command("property")
def property_tests(*args: str) -> None:
    """Run property-based tests."""
    _ = utils.uv_run(
        *utils.pytest_command(["tests/property", *utils.forwarded_args(args)])
    )


@app.command("unit-cov")
def unit_with_coverage(*args: str) -> None:
    """Run unit tests with coverage reporting."""
    _ = utils.uv_run(
        *utils.pytest_command(
            [
                f"--cov={utils.PKG}",
                "--cov-report=term-missing",
                "tests/unit",
                *utils.forwarded_args(args),
            ]
        )
    )


@app.command()
def integration(*args: str) -> None:
    """Run integration tests."""
    _ = utils.uv_run(
        *utils.pytest_command(
            ["-m", "integration", "--no-cov", *utils.forwarded_args(args)]
        )
    )


@app.command()
def e2e(*args: str) -> None:
    """Run end-to-end tests."""
    _ = utils.uv_run(*utils.pytest_command(["tests/e2e", *utils.forwarded_args(args)]))


@app.command()
def acceptance(*args: str) -> None:
    """Run acceptance tests."""
    _ = utils.uv_run(
        *utils.pytest_command(["tests/acceptance", *utils.forwarded_args(args)])
    )


@app.command()
def clean() -> None:
    """Remove pytest caches and HTML coverage artifacts."""
    utils.ensure_cache_dirs("pytest")
    for path in [
        utils.ROOT / ".pytest_cache",
        utils.ROOT / "htmlcov",
    ]:
        utils.remove_path(path)


def main() -> None:  # pragma: no cover
    try:
        app()
    except utils.CommandError as exc:  # pragma: no cover
        raise typer.Exit(1) from exc


if __name__ == "__main__":  # pragma: no cover
    main()
