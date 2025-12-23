from __future__ import annotations

import logging
import click
from pathlib import Path
from typing import Any, Callable, cast

import typer

__all__ = ["extract_exp_config", "run_or_exit"]


def extract_exp_config(ctx: typer.Context) -> Path | None:
    obj = getattr(ctx, "obj", None)
    if not isinstance(obj, dict):
        logging.getLogger(__name__).debug(
            "Context object missing or not a dict; no exp_config."
        )
        return None
    mapping = cast(dict[str, Any], obj)
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
    logger = logging.getLogger("ml_playground.runtime.cli")
    try:
        func()
    except FileNotFoundError as e:
        logger.error(f"{e}")
        raise typer.Exit(exception_exit_code)
    except (ValueError, TypeError) as e:
        logger.error(f"{e}")
        raise typer.Exit(exception_exit_code)
    except (typer.Exit, click.exceptions.Exit):
        raise
    except KeyboardInterrupt:
        if keyboard_interrupt_msg:
            logger.info(keyboard_interrupt_msg)
        return None
    except (RuntimeError, OSError, ImportError) as e:
        logger.error(f"{e}")
        raise typer.Exit(exception_exit_code)
