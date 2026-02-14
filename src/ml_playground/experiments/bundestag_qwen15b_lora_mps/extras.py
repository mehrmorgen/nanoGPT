from __future__ import annotations

from pathlib import Path

from pydantic import BaseModel, ConfigDict

from ml_playground.framework.experiment_registry.extras_registry import (
    register_extras_model,
)


class BundestagQwenPrepareExtras(BaseModel):
    model_config = ConfigDict(extra="forbid", arbitrary_types_allowed=True)

    dataset: str | None = None
    dataset_dir_override: Path | str | None = None


class BundestagQwenTrainExtras(BaseModel):
    model_config = ConfigDict(extra="forbid", arbitrary_types_allowed=True)

    save_merged_on_best: bool | None = None
    keep_last_n: int | None = None


class BundestagQwenSampleExtras(BaseModel):
    model_config = ConfigDict(extra="forbid", arbitrary_types_allowed=True)

    require_adapters: bool | None = None


register_extras_model(
    "bundestag_qwen15b_lora_mps", "prepare", BundestagQwenPrepareExtras
)
register_extras_model(
    "bundestag_qwen15b_lora_mps", "training", BundestagQwenTrainExtras
)
register_extras_model(
    "bundestag_qwen15b_lora_mps", "sampling", BundestagQwenSampleExtras
)
