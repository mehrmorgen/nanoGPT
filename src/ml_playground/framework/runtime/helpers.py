from __future__ import annotations

import logging
from pathlib import Path
from typing import Callable, cast

import click
import typer

from ml_playground.framework.configuration import loading as config_loading
from ml_playground.framework.core.logging_protocol import LoggerLike
from ml_playground.framework.runtime.core.results import ToolResult
from ml_playground.framework.runtime.protocols import MetadataConfigLike


def handle_tool_result(result: ToolResult, learning_mode: bool = False) -> None:
    """Handle ToolResult output and exit appropriately."""
    if result.stdout:
        typer.echo(result.stdout)
    if result.stderr:
        typer.echo(result.stderr, err=True)

    if learning_mode and result.learning_info:
        if result.learning_info.explanations:
            typer.echo("\n📚 Learning Mode - What this command does:")
            for explanation in result.learning_info.explanations:
                typer.echo(f"  • {explanation}")

        if result.learning_info.best_practices:
            typer.echo("\n💡 Best Practices:")
            for practice in result.learning_info.best_practices:
                typer.echo(f"  • {practice}")

        if result.learning_info.related_concepts:
            typer.echo("\n🔗 Related Concepts:")
            for concept in result.learning_info.related_concepts:
                typer.echo(f"  • {concept}")

    if not result.success:
        raise typer.Exit(result.exit_code)


def complete_experiments(ctx: typer.Context, incomplete: str) -> list[str]:
    """Auto-complete experiment names based on directories with a config.toml."""
    return config_loading.list_experiments_with_config(incomplete)


def extract_exp_config(ctx: typer.Context) -> Path | None:
    """Extract the --exp-config path from the Typer context."""
    obj = getattr(ctx, "obj", None)
    if not isinstance(obj, dict):
        logging.getLogger(__name__).debug(
            "Context object missing or not a dict; no exp_config."
        )
        return None
    mapping = cast(dict[str, object], obj)
    exp_config_obj: object | None = mapping.get("exp_config")
    logger = logging.getLogger(__name__)
    logger.debug("Context exp_config resolved to %s", exp_config_obj)
    if isinstance(exp_config_obj, Path):
        return exp_config_obj
    if exp_config_obj is None:
        return None
    logger.debug("Unexpected exp_config value type %s; ignoring.", type(exp_config_obj))
    return None


def run_or_exit(
    func: Callable[[], None],
    *,
    keyboard_interrupt_msg: str | None = None,
    exception_exit_code: int = 1,
) -> None:
    """Run a function and exit gracefully on exceptions."""
    try:
        func()
    except FileNotFoundError as e:
        logger = logging.getLogger(__name__)
        logger.error(f"{e}")
        raise typer.Exit(exception_exit_code)
    except (ValueError, TypeError) as e:
        logger = logging.getLogger(__name__)
        logger.error(f"{e}")
        raise typer.Exit(exception_exit_code)
    except (typer.Exit, click.exceptions.Exit):
        raise
    except KeyboardInterrupt:
        if keyboard_interrupt_msg:
            for logger_name in (
                "ml_playground.framework.cli",
                "ml_playground.runtime_cli",
                __name__,
            ):
                logging.getLogger(logger_name).info(keyboard_interrupt_msg)
        return
    except (RuntimeError, OSError, ImportError) as e:
        logger = logging.getLogger(__name__)
        logger.error(f"{e}")
        raise typer.Exit(exception_exit_code)


def log_directory(
    tag: str,
    dir_name: str,
    dir_path: object | None,
    logger: LoggerLike,
) -> None:
    """Log information about a directory path."""
    if dir_path is None:
        logger.info(f"[{tag}] {dir_name}: <not set>")
        return

    if not isinstance(dir_path, Path):
        return

    if dir_path.exists():
        try:
            contents = sorted([p.name for p in dir_path.iterdir()])
            logger.info(f"[{tag}] {dir_name} (exists): {dir_path}")
            logger.info(f"[{tag}]   Contents: {contents}")
        except OSError:
            logger.info(f"[{tag}] {dir_name} (exists): {dir_path}")
    else:
        logger.info(f"[{tag}] {dir_name} (missing): {dir_path}")


def log_command_status(
    tag: str,
    metadata: MetadataConfigLike,
    out_dir: Path | None,
    logger: LoggerLike,
) -> None:
    """Log known file-based artifacts for the given config."""
    try:
        dataset_dir = metadata.dataset_dir
    except (OSError, ValueError, TypeError, AttributeError):
        return

    try:
        log_directory(tag, "out_dir", out_dir, logger)
    except (OSError, ValueError, TypeError):
        pass

    try:
        log_directory(tag, "dataset_dir", dataset_dir, logger)
    except (OSError, ValueError, TypeError):
        pass


__all__ = [
    "complete_experiments",
    "extract_exp_config",
    "handle_tool_result",
    "log_command_status",
    "log_directory",
    "run_or_exit",
]
