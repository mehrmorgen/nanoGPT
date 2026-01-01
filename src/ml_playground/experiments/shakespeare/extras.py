from __future__ import annotations

from pathlib import Path
from typing import Any, Callable

from pydantic import BaseModel, ConfigDict

from ml_playground.experiments.extras_registry import register_extras_model


class ShakespearePrepareExtras(BaseModel):
    model_config = ConfigDict(extra="forbid", arbitrary_types_allowed=True)

    base_dir: Path | str | None = None
    http_get: Callable[..., Any] | None = None
    tokenizer_factory: Callable[[], Any] | None = None
    writer_fn: Callable[..., Any] | None = None


class ShakespeareTrainExtras(BaseModel):
    model_config = ConfigDict(extra="forbid", arbitrary_types_allowed=True)

    ckpt_last_filename: str | None = None
    ckpt_best_filename: str | None = None
    ckpt_metric: str | None = None
    ckpt_greater_is_better: bool | None = None
    ckpt_atomic: bool | None = None
    ckpt_write_metadata: bool | None = None
    ckpt_top_k: int | None = None
    ckpt_time_interval_minutes: int | None = None


class ShakespeareSampleExtras(BaseModel):
    model_config = ConfigDict(extra="forbid", arbitrary_types_allowed=True)

    require_adapters: bool | None = None


register_extras_model("shakespeare", "prepare", ShakespearePrepareExtras)
register_extras_model("shakespeare", "train", ShakespeareTrainExtras)
register_extras_model("shakespeare", "sample", ShakespeareSampleExtras)
