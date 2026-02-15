from __future__ import annotations

from pathlib import Path
from typing import Literal

from pydantic import BaseModel, ConfigDict

from ml_playground.framework.experiment_registry.extras_registry import (
    register_extras_model,
)


class BundestagCharPrepareExtras(BaseModel):
    model_config = ConfigDict(extra="forbid", arbitrary_types_allowed=True)

    dataset_dir_override: Path | str | None = None
    dataset_source: Literal["auto", "seed", "germaparl_tei"] = "auto"
    germaparl_ref: str = "main"
    germaparl_repo: str = "PolMine/GermaParlTEI"
    germaparl_cache_dir: Path | str | None = None
    germaparl_include_stage: bool = True
    germaparl_include_speaker_attrs: bool = True
    germaparl_force_refresh: bool = False
    germaparl_max_files: int | None = None
    split: float | None = None
    seed_policy: str | None = None
    germaparl_tarball_bytes: bytes | None = None
    germaparl_http_get: object | None = None


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
register_extras_model("bundestag_char", "training", BundestagCharTrainExtras)
register_extras_model("bundestag_char", "sampling", BundestagCharSampleExtras)
