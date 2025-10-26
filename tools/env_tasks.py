#!/usr/bin/env -S uv run --no-project
# /// script
# dependencies = [
#   "typer>=0.12.3",
# ]
# ///
"""Developer environment management commands for ml_playground."""

from __future__ import annotations

from pathlib import Path
from typing import Annotated

import typer

from tools import task_utils as utils

app = typer.Typer(
    help="Environment and developer tooling commands.", no_args_is_help=True
)


ClearFlag = Annotated[
    bool, typer.Option("--clear", help="Remove existing virtual env first")
]

GroupsOption = Annotated[
    list[str] | None,
    typer.Option(
        None, "--group", help="Sync the specified dependency groups (repeatable)."
    ),
]

AllGroupsFlag = Annotated[
    bool,
    typer.Option("--all-groups", help="Install all optional dependency groups."),
]

FrozenFlag = Annotated[
    bool,
    typer.Option(
        False,
        "--frozen",
        "--no-frozen",
        help="Use the existing lockfile without resolving new versions.",
    ),
]

DryRunFlag = Annotated[bool, typer.Option("--dry-run", help="Preview actions")]

LogdirOption = Annotated[
    Path,
    typer.Option(
        "--logdir",
        exists=True,
        file_okay=False,
        dir_okay=True,
        readable=True,
        help="TensorBoard log directory",
    ),
]

PortOption = Annotated[int, typer.Option("--port", help="Port to bind")]

HostOption = Annotated[str, typer.Option("--host", help="Host interface")]

ToolArgument = Annotated[str, typer.Argument(help="Target tool name")]


@app.command()
def setup(clear: ClearFlag = False) -> None:
    """Create a fresh uv-managed virtual environment and install all deps."""
    if clear:
        _ = utils.uv("venv", "--clear")
    _ = utils.uv("sync", "--all-groups")


@app.command()
def sync(
    groups: GroupsOption = None,
    all_groups: AllGroupsFlag = False,
    frozen: FrozenFlag = False,
) -> None:
    """Sync project dependencies using uv."""
    args = ["sync"]
    if frozen:
        args.append("--frozen")
    if all_groups:
        args.append("--all-groups")
    elif groups:
        for group in groups:
            args.extend(["--group", group])
    _ = utils.uv(*args)


@app.command()
def verify() -> None:
    """Ensure the project package imports correctly."""
    _ = utils.uv_run(
        "python", "-c", f"import {utils.PKG}; print('✓ {utils.PKG} import OK')"
    )


@app.command()
def clean() -> None:
    """Remove caches and temporary build artifacts."""
    if utils.CACHE_DIR.exists():
        before_entries = sorted(utils.CACHE_DIR.iterdir())
        if before_entries:
            typer.echo("[clean] cache contents before cleanup:")
            for entry in before_entries:
                typer.echo(f"  - {entry.relative_to(utils.ROOT)}")
        else:
            typer.echo("[clean] cache directory already empty")
    else:
        typer.echo("[clean] cache directory missing; nothing to clean")

    targets = [
        utils.CACHE_DIR / "pytest",
        utils.CACHE_DIR / "coverage",
        utils.CACHE_DIR / "hypothesis",
        utils.CACHE_DIR / "pre-commit",
        utils.CACHE_DIR / "ruff",
        utils.CACHE_DIR / "uv",
        utils.CACHE_DIR / "mypy",
        utils.ROOT / "htmlcov",
    ]
    for path in targets:
        utils.remove_path(path)

    for pycache in utils.ROOT.rglob("__pycache__"):
        utils.remove_path(pycache)

    if utils.CACHE_DIR.exists():
        after_entries = sorted(utils.CACHE_DIR.iterdir())
        if after_entries:
            typer.echo("[clean] cache contents after cleanup:")
            for entry in after_entries:
                typer.echo(f"  - {entry.relative_to(utils.ROOT)}")
        else:
            typer.echo("[clean] cache directory empty after cleanup")
    else:
        typer.echo("[clean] cache directory removed")


@app.command("ai-guidelines")
def ai_guidelines(
    tool: ToolArgument,
    dry_run: DryRunFlag = False,
) -> None:
    """Set up AI guideline symlinks for the requested tool."""
    if not tool.strip():
        from tools import setup_ai_guidelines  # local import to avoid circulars

        supported = ", ".join(sorted(setup_ai_guidelines.TOOL_MAP))
        typer.echo(f"[error] Missing tool name. Supported: {supported}")
        raise typer.Exit(1)
    command = ["python", "tools/setup_ai_guidelines.py", tool]
    if dry_run:
        command.append("--dry-run")
    _ = utils.uv_run(*command)


@app.command()
def tensorboard(
    logdir: LogdirOption,
    port: int = 6006,
    host: str = "127.0.0.1",
) -> None:
    """Launch TensorBoard for the given log directory."""
    _ = utils.uv_run(
        "tensorboard",
        "--logdir",
        str(logdir),
        "--port",
        str(port),
        "--host",
        host,
    )


@app.command("gguf-help")
def gguf_help() -> None:
    """Show llama.cpp GGUF conversion help."""
    try:
        _ = utils.uv_run("python", "tools/llama_cpp/convert-hf-to-gguf.py", "--help")
    except utils.CommandError:
        typer.echo("[info] GGUF converter exited with a non-zero status", err=True)


def main() -> None:  # pragma: no cover
    try:
        app()
    except utils.CommandError as exc:  # pragma: no cover
        raise typer.Exit(1) from exc


if __name__ == "__main__":  # pragma: no cover
    main()
