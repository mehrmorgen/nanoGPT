#!/usr/bin/env -S uv run --no-project
# /// script
# dependencies = [
#   "typer>=0.12.3",
# ]
# ///
"""Continuous integration workflows for ml_playground."""

from __future__ import annotations

import json
import os
import subprocess
import tomllib
from collections.abc import Mapping
from pathlib import Path
from typing import cast

import typer

from tools import task_utils as utils

app = typer.Typer(
    help="CI-oriented commands executed via uv run.", no_args_is_help=True
)
mutation_app = typer.Typer(help="Mutation testing helpers")
app.add_typer(mutation_app, name="mutation")


def _to_int(value: object) -> int:
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    if isinstance(value, str):
        try:
            return int(float(value))
        except ValueError:
            return 0
    return 0


def _coverage_file_env(coverage_file: Path) -> dict[str, str]:
    env = os.environ.copy()
    env["COVERAGE_FILE"] = str(coverage_file)
    return env


def _read_coverage_thresholds_from_config() -> tuple[float, float]:
    """Read coverage thresholds from pyproject.toml configuration.

    Returns:
        Tuple of (line_threshold, branch_threshold)
    """
    pyproject_path = Path("pyproject.toml")
    if not pyproject_path.exists():
        return 0.0, 0.0

    try:
        with open(pyproject_path, "rb") as f:
            config = tomllib.load(f)

        thresholds = (
            config.get("tool", {})
            .get("ml_playground", {})
            .get("coverage", {})
            .get("thresholds", {})
        )
        line_threshold = float(thresholds.get("line_threshold", 0.0))
        branch_threshold = float(thresholds.get("branch_threshold", 0.0))

        return line_threshold, branch_threshold
    except (tomllib.TOMLDecodeError, ValueError, KeyError):
        return 0.0, 0.0


@app.command()
def lint() -> None:
    """Run Ruff lint checks."""
    _ = utils.uv_run("ruff", "check", ".")


@app.command("lint-check")
def lint_check() -> None:
    """Run Ruff in check-only mode (alias)."""
    lint()


def _pytest(targets: list[str]) -> None:
    _ = utils.uv_run(*utils.pytest_command(targets))


@app.command()
def test(*args: str) -> None:
    """Run the full test suite."""
    _pytest(["tests", *utils.forwarded_args(args)])


@app.command()
def unit(*args: str) -> None:
    """Run unit tests."""
    _pytest(["tests/unit", *utils.forwarded_args(args)])


@app.command("property")
def property_tests(*args: str) -> None:
    """Run property-based tests."""
    _pytest(["tests/property", *utils.forwarded_args(args)])


@app.command()
def integration(*args: str) -> None:
    """Run integration tests."""
    _pytest(["-m", "integration", "--no-cov", *utils.forwarded_args(args)])


@app.command()
def e2e(*args: str) -> None:
    """Run end-to-end tests."""
    _pytest(["tests/e2e", *utils.forwarded_args(args)])


@app.command()
def acceptance(*args: str) -> None:
    """Run acceptance tests."""
    _pytest(["tests/acceptance", *utils.forwarded_args(args)])


@app.command("coverage-test")
def coverage_test() -> None:
    """Run targeted tests under coverage to collect data."""
    utils.ensure_cache_dirs("coverage", "hypothesis")
    dest_cov = utils.coverage_file()
    dest_cov.parent.mkdir(parents=True, exist_ok=True)
    utils.remove_path(dest_cov)
    for fragment in utils.coverage_fragments(dest_cov):
        utils.remove_path(fragment)

    env = os.environ.copy()
    env.update(
        {
            "HYPOTHESIS_DATABASE_DIRECTORY": str(utils.CACHE_DIR / "hypothesis"),
            "HYPOTHESIS_STORAGE_DIRECTORY": str(utils.CACHE_DIR / "hypothesis"),
            "HYPOTHESIS_SEED": "0",
            "PYTHONHASHSEED": "0",
            "COVERAGE_FILE": str(dest_cov),
        }
    )
    _ = utils.uv_run(
        "coverage",
        "run",
        f"--data-file={dest_cov}",
        "-m",
        "pytest",
        "-n",
        "0",
        "tests/unit",
        "tests/property",
        env=env,
    )
    # Normalize to a single monolithic file so the report stage can rely on it
    env_combine = _coverage_file_env(dest_cov)
    _ = utils.uv_run("coverage", "combine", env=env_combine, check=False)


@app.command("coverage-report")
def coverage_report(
    fail_under: float = typer.Option(
        0.0,
        "--fail-under",
        help="Fail if total coverage is below this threshold.",
    ),
    verbose: bool = typer.Option(
        False,
        "--verbose",
        help="Print discovered coverage artifacts",
    ),
) -> None:
    """Generate coverage reports under .cache/coverage."""
    dest_cov = utils.coverage_file()
    ci_strict = os.environ.get("CI", "").lower() == "true"
    if not dest_cov.exists():
        typer.echo(
            "[coverage] missing coverage data file. Ensure the prior 'coverage-test' step ran and wrote '"
            + str(dest_cov)
            + "'",
            err=True,
        )
        raise typer.Exit(1)

    if ci_strict and dest_cov.stat().st_size == 0:
        typer.echo("[coverage] coverage data file is empty", err=True)
        raise typer.Exit(1)

    env = _coverage_file_env(dest_cov)
    fail_arg = ["--fail-under", f"{fail_under:.2f}"]
    coverage_dir = dest_cov.parent
    commands: list[tuple[str, list[str]]] = [
        ("report", ["-m", *fail_arg]),
        ("html", ["-d", str(coverage_dir / "htmlcov")]),
        ("json", ["-o", str(coverage_dir / "coverage.json")]),
        ("xml", ["-o", str(coverage_dir / "coverage.xml")]),
    ]

    for subcommand, args in commands:
        _ = utils.uv_run("coverage", subcommand, *args, env=env)

    if verbose:
        typer.echo("[coverage] artifacts:")
        for path in sorted(dest_cov.parent.iterdir()):
            typer.echo(f"  - {path.relative_to(utils.ROOT)}")


@app.command("coverage-threshold")
def coverage_threshold(
    verbose: bool = typer.Option(
        False,
        "--verbose",
        help="Print computed coverage totals.",
    ),
) -> None:
    """Fail when coverage metrics drop below configured thresholds from pyproject.toml."""
    # Always read thresholds from config
    line_threshold, branch_threshold = _read_coverage_thresholds_from_config()
    
    if line_threshold == 0.0 and branch_threshold == 0.0:
        typer.echo("[coverage] No thresholds configured in pyproject.toml [tool.ml_playground.coverage.thresholds]", err=True)
        raise typer.Exit(1)
    dest_cov = utils.coverage_file()
    if not dest_cov.exists():
        typer.echo(
            "[coverage] missing coverage data file. Run 'uv run ci-tasks coverage-test' first.",
            err=True,
        )
        raise typer.Exit(1)

    env = _coverage_file_env(dest_cov)
    json_path = dest_cov.parent / "coverage.json"
    # Generate JSON report, ignoring exit code since we'll do our own threshold checking
    _ = utils.uv_run("coverage", "json", "-o", str(json_path), env=env, check=False)

    try:
        coverage_payload_raw = cast(
            object, json.loads(json_path.read_text(encoding="utf-8"))
        )
    except json.JSONDecodeError as exc:  # pragma: no cover - defensive
        typer.echo(f"[coverage] failed to parse branch coverage data: {exc}", err=True)
        raise typer.Exit(1)

    if not isinstance(coverage_payload_raw, Mapping):
        typer.echo(
            "[coverage] coverage.json must contain a mapping root object", err=True
        )
        raise typer.Exit(1)
    coverage_payload = cast(Mapping[str, object], coverage_payload_raw)

    totals_obj = coverage_payload.get("totals")
    if not isinstance(totals_obj, Mapping):
        typer.echo("[coverage] coverage.json missing 'totals' mapping", err=True)
        raise typer.Exit(1)
    totals = cast(Mapping[str, object], totals_obj)

    num_branches = _to_int(totals.get("num_branches", 0))
    covered_branches = _to_int(totals.get("covered_branches", 0))
    covered_lines = _to_int(totals.get("covered_lines", 0))
    num_statements = _to_int(totals.get("num_statements", 0))

    messages: list[str] = []

    line_pct = (covered_lines / num_statements) * 100 if num_statements else 0.0
    branch_pct = (
        (covered_branches / num_branches) * 100 if num_branches else float("nan")
    )
    if verbose:
        typer.echo(
            f"[coverage] totals: lines={line_pct:.2f}% branches={branch_pct:.2f}%"
        )

    if line_threshold > 0 and num_statements == 0:
        messages.append("line coverage totals missing from coverage.json")
    elif line_threshold > 0 and line_pct < line_threshold:
        messages.append(
            f"Line coverage {line_pct:.2f}% < {line_threshold:.2f}%. Run 'uv run ci-tasks coverage-test'."
        )

    if branch_threshold > 0:
        if num_branches == 0:
            messages.append("Branch coverage data missing from coverage.json")
        else:
            if branch_pct < branch_threshold:
                messages.append(
                    f"Branch coverage {branch_pct:.2f}% < {branch_threshold:.2f}%."
                )

    if messages:
        for message in messages:
            typer.echo(f"[coverage] {message}", err=True)
        raise typer.Exit(1)


@app.command("coverage-badge")
def coverage_badge() -> None:
    """Regenerate the SVG coverage badges."""
    json_path = utils.coverage_file().parent / "coverage.json"
    if not json_path.exists():
        coverage_report()
    _ = utils.uv_run(
        "python", "tools/coverage_badges.py", str(json_path), "docs/assets"
    )


@app.command()
def quality(
    args: list[str] | None = typer.Argument(
        None,
        metavar="[PRE-COMMIT-ARGS]",
        help="Additional arguments forwarded to pre-commit.",
    ),
) -> None:
    """Run the full pre-commit quality gate."""
    _ = utils.uv_run(
        "pre-commit",
        "run",
        "--config",
        str(utils.PRE_COMMIT_CONFIG),
        "--all-files",
        *utils.forwarded_args(args),
    )
    integration()
    acceptance()
    e2e()


@app.command("quality-fast")
def quality_fast(
    args: list[str] | None = typer.Argument(
        None,
        metavar="[PRE-COMMIT-ARGS]",
        help="Additional arguments forwarded to pre-commit.",
    ),
) -> None:
    """Run lint/format focused pre-commit hooks."""
    _ = utils.uv_run(
        "pre-commit",
        "run",
        "--config",
        str(utils.PRE_COMMIT_CONFIG),
        "--all-files",
        "ruff",
        *utils.forwarded_args(args),
    )
    _ = utils.uv_run(
        "pre-commit",
        "run",
        "--config",
        str(utils.PRE_COMMIT_CONFIG),
        "--all-files",
        "ruff-format",
        *utils.forwarded_args(args),
    )
    _ = utils.uv_run(
        "pre-commit",
        "run",
        "--config",
        str(utils.PRE_COMMIT_CONFIG),
        "--all-files",
        "mdformat",
        *utils.forwarded_args(args),
    )


@app.command("quality-ext")
def quality_ext() -> None:
    """Run quality gates followed by mutation testing."""
    quality()
    mutation_run()


@app.command("quality-ci-local")
def quality_ci_local(
    bind_caches: bool = typer.Option(
        True,
        "--bind-caches/--no-bind-caches",
        help="Bind local caches and the project virtualenv into the act container.",
    ),
    args: list[str] | None = typer.Argument(
        None,
        metavar="[ACT-ARGS]",
        help="Additional arguments forwarded to act.",
    ),
) -> None:
    """Run the GitHub quality workflow locally using act."""
    utils.ensure_cache_dirs("uv", "pre-commit", "ruff")
    (utils.ROOT / ".venv").mkdir(parents=True, exist_ok=True)

    command: list[str] = [
        "act",
        "--container-architecture",
        "linux/amd64",
        "-P",
        "ubuntu-latest=catthehacker/ubuntu:act-latest",
        "-W",
        ".github/workflows/quality.yml",
        "--job",
        "quality",
    ]

    if bind_caches:
        binds: list[tuple[Path, str]] = [
            (utils.CACHE_DIR / "uv", "/root/.cache/uv"),
            (utils.CACHE_DIR / "pre-commit", "/root/.cache/pre-commit"),
            (utils.CACHE_DIR / "ruff", "/root/.cache/ruff"),
            (utils.ROOT / ".venv", "/root/project/.venv"),
        ]
        for host_path, container_path in binds:
            host_path.mkdir(parents=True, exist_ok=True)
            command.extend(["--bind", f"{host_path}:{container_path}"])

    command.extend(utils.forwarded_args(args))

    result = subprocess.run(command, cwd=utils.ROOT)
    if result.returncode != 0:
        raise typer.Exit(result.returncode)


@mutation_app.command("reset")
def mutation_reset() -> None:
    """Remove the cached Cosmic Ray session."""
    session = utils.cosmic_ray_session_file()
    if session.exists():
        typer.echo(f"Removing {session}")
        utils.remove_path(session)


@mutation_app.command("summary")
def mutation_summary() -> None:
    """Show a summary of the previous Cosmic Ray run."""
    _ = utils.uv_run(
        "python", "tools/mutation_summary.py", "--config", "pyproject.toml"
    )


@mutation_app.command("init")
def mutation_init() -> None:
    """Initialize the Cosmic Ray session database if needed."""
    session = utils.cosmic_ray_session_file()
    session.parent.mkdir(parents=True, exist_ok=True)
    result = utils.uv_run(
        "cosmic-ray",
        "init",
        "pyproject.toml",
        str(session),
        check=False,
    )
    if result.returncode != 0:
        typer.echo("[mutation] init skipped (reusing existing session)")
    else:
        typer.echo("[mutation] init complete")


@mutation_app.command("exec")
def mutation_exec() -> None:
    """Execute mutation tests with Cosmic Ray."""
    typer.echo("[mutation] starting exec")
    try:
        _ = utils.uv_run(
            "cosmic-ray", "exec", "pyproject.toml", str(utils.cosmic_ray_session_file())
        )
    except utils.CommandError as exc:
        typer.echo(f"[warning] Cosmic Ray returned non-zero status: {exc}", err=True)
        raise typer.Exit(1) from exc


@mutation_app.command("report")
def mutation_report() -> None:
    """Render a mutation testing report."""
    _ = utils.uv_run("python", "tools/mutation_report.py", "--config", "pyproject.toml")


@mutation_app.command("run")
def mutation_run() -> None:
    """Run the full mutation testing pipeline."""
    mutation_reset()
    mutation_summary()
    mutation_init()
    mutation_exec()
    mutation_report()


def main() -> None:  # pragma: no cover
    try:
        app()
    except utils.CommandError as exc:  # pragma: no cover
        raise typer.Exit(1) from exc


if __name__ == "__main__":  # pragma: no cover
    main()
