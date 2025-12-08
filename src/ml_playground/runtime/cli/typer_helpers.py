from __future__ import annotations

from pathlib import Path
from typing import Annotated, Dict, Mapping, cast

import typer

from ml_playground.runtime import helpers as rt_helpers
from ml_playground.runtime.core.results import VerbosityLevel


def complete_experiments(ctx: typer.Context, incomplete: str) -> list[str]:
    """Public completion helper for experiment names."""
    return rt_helpers.complete_experiments(ctx, incomplete)


EXPERIMENT_HELP = "Experiment name (directory in src/ml_playground/experiments)"

ExperimentArg = Annotated[
    str | None,
    typer.Argument(
        help=EXPERIMENT_HELP,
        autocompletion=complete_experiments,
    ),
]


def extract_exp_config(ctx: typer.Context) -> Path | None:
    return rt_helpers.extract_exp_config(ctx)


def prepare_learning_context(
    ctx: typer.Context,
) -> tuple[bool, VerbosityLevel, Dict[str, object]]:
    if isinstance(ctx.obj, Mapping):
        typed_obj = cast(Mapping[str, object], ctx.obj)
        overrides: Dict[str, object] = {key: typed_obj[key] for key in typed_obj.keys()}
    else:
        overrides = {}

    learning_mode = bool(overrides.get("learning_mode", False))
    verbosity_value = overrides.get("verbosity", VerbosityLevel.STANDARD)
    verbosity = (
        verbosity_value
        if isinstance(verbosity_value, VerbosityLevel)
        else VerbosityLevel(verbosity_value)
        if isinstance(verbosity_value, int)
        else VerbosityLevel.STANDARD
    )
    return learning_mode, verbosity, overrides
