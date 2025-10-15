from __future__ import annotations

from pathlib import Path

from ml_playground.configuration.models import SharedConfig, TrainerConfig
from ml_playground.experiments.protocol import (
    TrainReport,
    Trainer as _TrainerProto,
)
from ml_playground.training.loop.runner import Trainer as _CoreTrainer


def _default_shared(cfg: TrainerConfig) -> SharedConfig:
    runtime = cfg.runtime
    if runtime is None:
        raise ValueError("Runtime configuration is required for training")
    exp_dir = Path(__file__).resolve().parent
    out_dir = runtime.out_dir
    dataset_dir = exp_dir / "datasets"
    config_path = exp_dir / "config.toml"
    project_home = exp_dir
    out_dir = out_dir if isinstance(out_dir, Path) else Path(out_dir)
    return SharedConfig(
        experiment="connect_four",
        config_path=config_path,
        project_home=project_home,
        dataset_dir=dataset_dir,
        train_out_dir=out_dir,
        sample_out_dir=out_dir,
    )


class ConnectFourTrainer(_TrainerProto):
    """Run the nanoGPT training loop for the Connect Four dataset."""

    def train(  # type: ignore[override]
        self, cfg: TrainerConfig, shared: SharedConfig | None = None
    ) -> TrainReport:
        shared_cfg = shared or _default_shared(cfg)
        logger = getattr(cfg, "logger", None)
        if logger:
            logger.info(
                "[connect_four] starting training (dataset_dir=%s, out_dir=%s)",
                shared_cfg.dataset_dir,
                shared_cfg.train_out_dir,
            )
        _CoreTrainer(cfg, shared_cfg).run()
        if logger:
            logger.info("[connect_four] training finished")
        return TrainReport(messages=("[connect_four] training finished",))
