from __future__ import annotations

from pathlib import Path
from ml_playground.framework.configuration.models import TrainerConfig, MetadataConfig
from ml_playground.framework.experiment_registry.protocol import (
    Trainer as _TrainerProto,
    TrainReport,
)
from ml_playground.framework.training.loop.runner import Trainer as _CoreTrainer


class ShakespeareTrainer(_TrainerProto):
    def train(self, cfg: TrainerConfig) -> TrainReport:  # type: ignore[override]
        out_dir: Path = cfg.runtime.out_dir
        shared = MetadataConfig(
            experiment="shakespeare",
            config_path=out_dir / "cfg.toml",
            project_home=out_dir.parent,
            dataset_dir=out_dir,
            train_out_dir=out_dir,
            sample_out_dir=out_dir,
        )
        _CoreTrainer(cfg, shared).run()
        return TrainReport(
            created_files=(),
            updated_files=(),
            skipped_files=(),
            messages=("[shakespeare] training finished",),
        )
