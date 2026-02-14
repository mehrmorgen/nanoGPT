from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, Protocol

from ml_playground.framework.configuration.models import (
    MetadataConfig,
    TrainerConfig,
)
from ml_playground.framework.training.loop.runner import (
    Trainer,
    TrainerDependencies,
)


from pathlib import Path


DirectoryCreator = Callable[[Path, bool, bool], None]


def _build_runtime_metadata(
    cfg: TrainerConfig,
    *,
    mkdir_fn: DirectoryCreator | None = None,
) -> MetadataConfig:
    out_dir = cfg.runtime.out_dir
    if mkdir_fn:
        creator = mkdir_fn
    else:

        def _default_mkdir(p: Path, parents: bool, exist_ok: bool) -> None:
            p.mkdir(parents=parents, exist_ok=exist_ok)

        creator = _default_mkdir
    creator(out_dir, True, True)  # parents=True, exist_ok=True
    return MetadataConfig(
        experiment="runtime",
        config_path=out_dir / "cfg.toml",
        project_home=out_dir,
        dataset_dir=out_dir,
        train_out_dir=out_dir,
        sample_out_dir=out_dir,
    )


class TrainerLike(Protocol):
    iter_num: int
    best_val_loss: float

    def run(self) -> tuple[int, float]: ...


TrainerFactory = Callable[
    [TrainerConfig, MetadataConfig, TrainerDependencies | None], TrainerLike
]


@dataclass(frozen=True)
class TrainingPlan:
    """Strict descriptor for running the training loop."""

    config: TrainerConfig
    metadata: MetadataConfig | None = None
    deps: TrainerDependencies | None = None
    trainer_factory: TrainerFactory | None = None
    mkdir_fn: DirectoryCreator | None = None


@dataclass(frozen=True)
class TrainingSummary:
    iterations: int
    best_loss: float
    metadata: MetadataConfig


class TrainingSession:
    """Owning context for the training loop lifecycle."""

    def __init__(self, plan: TrainingPlan) -> None:
        self.plan = plan
        self.metadata: MetadataConfig | None = plan.metadata
        self.deps = plan.deps
        self.trainer: TrainerLike | None = None
        self.summary: TrainingSummary | None = None

    def _ensure_metadata(self) -> MetadataConfig:
        if self.metadata is not None:
            return self.metadata
        runtime_metadata = _build_runtime_metadata(
            self.plan.config,
            mkdir_fn=self.plan.mkdir_fn,
        )
        # Update the mutable reference for later inspection
        self.metadata = runtime_metadata
        return runtime_metadata

    def run(self) -> TrainingSummary:
        metadata_cfg = self._ensure_metadata()
        factory = self.plan.trainer_factory or Trainer
        trainer = factory(self.plan.config, metadata_cfg, self.deps)
        self.trainer = trainer
        iterations, best_loss = trainer.run()
        self.summary = TrainingSummary(iterations, best_loss, metadata_cfg)
        return self.summary


def run_training(plan: TrainingPlan) -> TrainingSummary:
    """Convenience helper for executing the training plan."""

    session = TrainingSession(plan)
    return session.run()


__all__ = [
    "TrainingPlan",
    "TrainingSession",
    "TrainingSummary",
    "run_training",
    "TrainerDependencies",
    "TrainerFactory",
    "DirectoryCreator",
]
