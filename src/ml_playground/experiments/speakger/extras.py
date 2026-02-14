from __future__ import annotations

from pathlib import Path
from typing import Any, Callable

from pydantic import BaseModel, ConfigDict

from ml_playground.framework.experiment_registry.extras_registry import (
    register_extras_model,
)


class SpeakGerPrepareExtras(BaseModel):
    model_config = ConfigDict(extra="forbid", arbitrary_types_allowed=True)

    dataset: str | None = None
    dataset_dir_override: Path | str | None = None


class SpeakGerTrainExtras(BaseModel):
    model_config = ConfigDict(extra="forbid", arbitrary_types_allowed=True)

    save_merged_on_best: bool | None = None
    keep_last_n: int | None = None


class SpeakGerSampleExtras(BaseModel):
    model_config = ConfigDict(extra="forbid", arbitrary_types_allowed=True)

    require_adapters: bool | None = None
    hf_model_name: str | None = None
    tokenizer_factory: Callable[..., Any] | None = None
    base_model_factory: Callable[..., Any] | None = None
    peft_model_factory: Callable[..., Any] | None = None


register_extras_model("speakger", "prepare", SpeakGerPrepareExtras)
register_extras_model("speakger", "training", SpeakGerTrainExtras)
register_extras_model("speakger", "sampling", SpeakGerSampleExtras)
