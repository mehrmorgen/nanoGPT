from __future__ import annotations

from pathlib import Path

from pydantic import BaseModel, ConfigDict

from ml_playground.experiments.extras_registry import register_extras_model


class BundestagCharPrepareExtras(BaseModel):
    model_config = ConfigDict(extra="forbid", arbitrary_types_allowed=True)

    dataset_dir_override: Path | str | None = None


class BundestagCharTrainExtras(BaseModel):
    model_config = ConfigDict(extra="forbid", arbitrary_types_allowed=True)

    ckpt_last_filename: str | None = None
    ckpt_best_filename: str | None = None
    ckpt_metric: str | None = None
    ckpt_greater_is_better: bool | None = None
    ckpt_atomic: bool | None = None
    ckpt_write_metadata: bool | None = None
    ckpt_top_k: int | None = None
    ckpt_time_interval_minutes: int | None = None


class BundestagCharSampleExtras(BaseModel):
    model_config = ConfigDict(extra="forbid", arbitrary_types_allowed=True)

    require_adapters: bool | None = None


register_extras_model("bundestag_char", "prepare", BundestagCharPrepareExtras)
register_extras_model("bundestag_char", "train", BundestagCharTrainExtras)
register_extras_model("bundestag_char", "sample", BundestagCharSampleExtras)
