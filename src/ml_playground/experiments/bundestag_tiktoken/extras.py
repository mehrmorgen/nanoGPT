from __future__ import annotations

from pathlib import Path

from pydantic import BaseModel, ConfigDict

from ml_playground.framework.experiment_registry.extras_registry import (
    register_extras_model,
)


class BundestagTiktokenPrepareExtras(BaseModel):
    model_config = ConfigDict(extra="forbid", arbitrary_types_allowed=True)

    dataset: str | None = None
    dataset_dir_override: Path | str | None = None


class BundestagTiktokenTrainExtras(BaseModel):
    model_config = ConfigDict(extra="forbid", arbitrary_types_allowed=True)

    ckpt_last_filename: str | None = None
    ckpt_best_filename: str | None = None
    ckpt_metric: str | None = None
    ckpt_greater_is_better: bool | None = None
    ckpt_atomic: bool | None = None
    ckpt_write_metadata: bool | None = None
    ckpt_top_k: int | None = None
    ckpt_time_interval_minutes: int | None = None


class BundestagTiktokenSampleExtras(BaseModel):
    model_config = ConfigDict(extra="forbid", arbitrary_types_allowed=True)

    require_adapters: bool | None = None


register_extras_model("bundestag_tiktoken", "prepare", BundestagTiktokenPrepareExtras)
register_extras_model("bundestag_tiktoken", "training", BundestagTiktokenTrainExtras)
register_extras_model("bundestag_tiktoken", "sampling", BundestagTiktokenSampleExtras)
