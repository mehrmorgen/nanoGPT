from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast

from ml_playground.framework.configuration.models import (
    DataConfig,
    LRSchedule,
    MetadataConfig,
    ModelConfig,
    OptimConfig,
    RuntimeConfig,
    TrainerConfig,
)
from ml_playground.framework.training.api import (
    TrainingPlan,
    run_training,
)
from ml_playground.framework.training.loop.runner import TrainerDependencies
from ml_playground.framework.training.loop.runner import Trainer
from tests.support.config_builders import create_metadata_config


def _make_config(tmp_path: Path) -> TrainerConfig:
    return TrainerConfig(
        model=ModelConfig(),
        data=DataConfig(),
        optim=OptimConfig(),
        schedule=LRSchedule(),
        runtime=RuntimeConfig(out_dir=tmp_path),
    )


def test_run_training_uses_factory_and_metadata(tmp_path: Path) -> None:
    cfg = _make_config(tmp_path)
    metadata = create_metadata_config(tmp_path, experiment="unit", mkdir=False)
    calls: dict[str, Any] = {}

    def factory(
        config: TrainerConfig,
        metadata_cfg: MetadataConfig,
        trainer_deps: TrainerDependencies | None = None,
    ) -> Trainer:
        calls["config"] = config
        calls["metadata"] = metadata_cfg

        class _FakeTrainer:
            def run(self) -> tuple[int, float]:
                return 3, 0.25

        return cast(Trainer, _FakeTrainer())

    summary = run_training(
        TrainingPlan(config=cfg, metadata=metadata, trainer_factory=factory)
    )

    assert calls["config"] is cfg
    assert calls["metadata"] is metadata
    assert summary.iterations == 3
    assert summary.best_loss == 0.25
    assert summary.metadata is metadata


def test_run_training_passes_dependencies(tmp_path: Path) -> None:
    cfg = _make_config(tmp_path)
    deps = SimpleNamespace()  # test-only sentinel
    calls: dict[str, Any] = {}

    def factory(
        config: TrainerConfig,
        metadata_cfg: MetadataConfig,
        trainer_deps: TrainerDependencies | None,
    ) -> Trainer:
        calls["deps"] = trainer_deps

        class _FakeTrainer:
            def run(self) -> tuple[int, float]:
                return 1, 0.5

        return cast(Trainer, _FakeTrainer())

    summary = run_training(
        TrainingPlan(
            config=cfg,
            deps=deps,  # type: ignore[arg-type]
            trainer_factory=factory,
        )
    )

    assert calls["deps"] is deps
    assert summary.iterations == 1
    assert summary.best_loss == 0.5


def test_run_training_default_mkdir_when_no_metadata(tmp_path: Path) -> None:
    """When metadata is None and mkdir_fn is None, _build_runtime_metadata uses default mkdir."""
    cfg = _make_config(tmp_path)
    calls: dict[str, Any] = {}

    def factory(
        config: TrainerConfig,
        metadata_cfg: MetadataConfig,
        trainer_deps: TrainerDependencies | None,
    ) -> Trainer:
        calls["metadata"] = metadata_cfg

        class _FakeTrainer:
            def run(self) -> tuple[int, float]:
                return 5, 0.1

        return cast(Trainer, _FakeTrainer())

    summary = run_training(
        TrainingPlan(config=cfg, metadata=None, trainer_factory=factory)
    )

    assert summary.iterations == 5
    assert summary.metadata.experiment == "runtime"
    assert summary.metadata.train_out_dir == tmp_path


def test_run_training_custom_mkdir_fn(tmp_path: Path) -> None:
    """When mkdir_fn is provided, _build_runtime_metadata delegates to it."""
    cfg = _make_config(tmp_path)
    mkdir_calls: list[tuple[Path, bool, bool]] = []

    def _fake_mkdir(p: Path, parents: bool, exist_ok: bool) -> None:
        mkdir_calls.append((p, parents, exist_ok))
        p.mkdir(parents=parents, exist_ok=exist_ok)

    def factory(
        config: TrainerConfig,
        metadata_cfg: MetadataConfig,
        trainer_deps: TrainerDependencies | None,
    ) -> Trainer:
        class _FakeTrainer:
            def run(self) -> tuple[int, float]:
                return 1, 0.5

        return cast(Trainer, _FakeTrainer())

    summary = run_training(
        TrainingPlan(
            config=cfg, metadata=None, mkdir_fn=_fake_mkdir, trainer_factory=factory
        )
    )

    assert len(mkdir_calls) == 1
    assert mkdir_calls[0] == (tmp_path, True, True)
    assert summary.metadata.experiment == "runtime"
