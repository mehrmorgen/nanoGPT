from __future__ import annotations

from pathlib import Path
from typing import Any, cast
from tempfile import TemporaryDirectory

from hypothesis import given, settings
from hypothesis import strategies as st

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
    TrainerDependencies,
    TrainerLike,
    TrainingPlan,
    run_training,
)


_SAFE_SUBDIR = st.from_regex(r"[A-Za-z0-9_-]{1,8}", fullmatch=True)


@given(  # type: ignore[reportAny]
    subdir=_SAFE_SUBDIR
)
@settings(max_examples=10, deadline=None, derandomize=True)
def test_run_training_creates_metadata_when_missing(subdir: str) -> None:
    with TemporaryDirectory() as tmpdir:
        out_dir = (Path(tmpdir) / subdir).resolve()
        cfg = TrainerConfig(
            model=ModelConfig(),
            data=DataConfig(),
            optim=OptimConfig(),
            schedule=LRSchedule(),
            runtime=RuntimeConfig(out_dir=out_dir),
        )

        def factory(
            config: TrainerConfig,
            metadata: MetadataConfig,
            trainer_deps: TrainerDependencies | None = None,
        ) -> TrainerLike:
            class _FakeTrainer:
                iter_num = 0
                best_val_loss = 0.0

                def run(self) -> tuple[int, float]:
                    return 0, 0.0

            return cast(Any, _FakeTrainer())

        summary = run_training(TrainingPlan(config=cfg, trainer_factory=factory))

        assert summary.metadata.train_out_dir.resolve() == out_dir
        assert summary.metadata.sample_out_dir.resolve() == out_dir
        assert summary.metadata.dataset_dir.resolve() == out_dir
        assert summary.metadata.config_path.resolve() == out_dir / "cfg.toml"
        assert out_dir.exists()


@given(  # type: ignore[reportAny]
    iterations=st.integers(min_value=0, max_value=5),
    best=st.floats(min_value=0.0, max_value=1.0, allow_nan=False, allow_infinity=False),
)
@settings(max_examples=10, deadline=None, derandomize=True)
def test_run_training_summary_tracks_factory_output(
    iterations: int, best: float
) -> None:
    with TemporaryDirectory() as tmpdir:
        out_dir = Path(tmpdir) / "out"
        cfg = TrainerConfig(
            model=ModelConfig(),
            data=DataConfig(),
            optim=OptimConfig(),
            schedule=LRSchedule(),
            runtime=RuntimeConfig(out_dir=out_dir),
        )
        calls: dict[str, Any] = {}

        def factory(
            config: TrainerConfig,
            metadata: MetadataConfig,
            trainer_deps: TrainerDependencies | None = None,
        ) -> TrainerLike:
            calls["config"] = config
            calls["metadata"] = metadata

            class _FakeTrainer:
                iter_num = iterations
                best_val_loss = best

                def run(self) -> tuple[int, float]:
                    return iterations, best

            return cast(Any, _FakeTrainer())

        summary = run_training(TrainingPlan(config=cfg, trainer_factory=factory))

        assert calls["config"] is cfg
        assert calls["metadata"] is summary.metadata
        assert summary.iterations == iterations
        assert summary.best_loss == best
