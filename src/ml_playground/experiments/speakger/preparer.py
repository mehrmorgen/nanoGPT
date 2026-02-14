from __future__ import annotations

from pathlib import Path
from typing import Mapping, cast

from ml_playground.framework.configuration.models import PreparerConfig
from ml_playground.framework.experiment_registry.protocol import (
    Preparer as _PreparerProto,
    PrepareReport,
)

__all__ = ["SpeakGerPreparer", "config_path"]


def _config_path() -> Path:
    return Path(__file__).resolve().parent / "config.toml"


def config_path() -> Path:
    return _config_path()


class SpeakGerPreparer(_PreparerProto):
    def prepare(self, cfg: PreparerConfig) -> PrepareReport:  # type: ignore[override]
        # Minimal preparer: this experiment expects pre-tokenized data or external pipeline.
        # We simply ensure the dataset directory exists and report no-op if present.
        extras = cast(Mapping[str, object], getattr(cfg, "extras", {}) or {})
        base_dir_override = extras.get("dataset_dir_override")
        if isinstance(base_dir_override, (str, Path)):
            exp_dir = Path(base_dir_override)
        else:
            exp_dir = Path(__file__).resolve().parent
        ds_dir = exp_dir / "datasets"
        ds_dir.mkdir(parents=True, exist_ok=True)
        msgs = (
            f"[speakger] no-op prepare; expecting external/pre-tokenized dataset at {ds_dir}",
        )
        return PrepareReport(
            created_files=tuple(),
            updated_files=tuple(),
            skipped_files=(ds_dir,),
            messages=msgs,
        )
