"""Property-based tests for Trainer class."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Iterator

import torch
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from ml_playground.configuration.models import (
    TrainerConfig,
    SharedConfig,
    RuntimeConfig,
    DataConfig,
    ModelConfig,
    OptimConfig,
    LRSchedule,
)
from ml_playground.core.logging_protocol import LoggerLike
from ml_playground.training.loop.runner import Trainer, TrainerDependencies
from ml_playground.training.checkpointing.checkpoint_manager import (
    CheckpointManager,
    Checkpoint,
)
from ml_playground.training.types import BatchProvider, OptimizerLike, ScaledLoss

# --- Fakes ---


class FakeLogger(LoggerLike):
    def debug(self, msg: str, *args: object, **kwargs: object) -> None:
        pass

    def info(self, msg: str, *args: object, **kwargs: object) -> None:
        pass

    def warning(self, msg: str, *args: object, **kwargs: object) -> None:
        pass

    def error(self, msg: str, *args: object, **kwargs: object) -> None:
        pass


class FakeModel:
    def __init__(self) -> None:
        self.training = True
        # Minimal parameter to allow optimizer to work
        self.p = torch.nn.Parameter(torch.tensor([0.0]))

    def __call__(self, inputs: Any, targets: Any) -> tuple[Any, torch.Tensor]:
        # Return logits (dummy) and loss
        # inputs and targets are tensors
        loss = torch.tensor(0.1, requires_grad=True)
        return inputs, loss

    def train(self) -> None:
        self.training = True

    def eval(self) -> None:
        self.training = False

    def parameters(self) -> Iterator[torch.nn.Parameter]:
        yield self.p


class FakeOptimizer:
    def __init__(self, params: Any) -> None:
        self.param_groups: list[dict[str, Any]] = [{"lr": 0.0}]

    def zero_grad(self, set_to_none: bool = False) -> None:
        pass

    def step(self) -> None:
        pass

    def state_dict(self) -> dict[str, Any]:
        return {}

    def load_state_dict(self, state_dict: dict[str, Any]) -> None:
        pass


class FakeScaler:
    def scale(self, loss: torch.Tensor) -> ScaledLoss:
        return loss  # type: ignore

    def step(self, optimizer: Any) -> None:
        pass

    def update(self) -> None:
        pass

    def unscale_(self, optimizer: Any) -> None:
        pass


class FakeBatchProvider:
    def get_batch(self, split: str) -> tuple[torch.Tensor, torch.Tensor]:
        # Return dummy tensors
        return torch.zeros(1, 1), torch.zeros(1, 1)


class FakeCheckpointManager:
    pass


# --- Dependency Factories ---


def fake_initialize_batches(cfg: TrainerConfig, shared: SharedConfig) -> BatchProvider:
    return FakeBatchProvider()


def fake_initialize_model(
    cfg: TrainerConfig, logger: LoggerLike
) -> tuple[Any, OptimizerLike]:
    model = FakeModel()
    return model, FakeOptimizer(model.parameters())


def fake_initialize_components(
    model: Any,
    cfg: TrainerConfig,
    runtime: Any,
    *,
    log_dir: str,
) -> tuple[Any, Any, Any, Any]:
    # Returns model, scaler, ema, writer
    return model, FakeScaler(), None, None


def fake_create_manager(cfg: TrainerConfig, shared: SharedConfig) -> CheckpointManager:
    return FakeCheckpointManager()  # type: ignore


def fake_load_checkpoint(*args: Any, **kwargs: Any) -> Checkpoint | None:
    return None


def fake_apply_checkpoint(*args: Any, **kwargs: Any) -> tuple[int, float]:
    return 0, 0.0


def fake_save_checkpoint(*args: Any, **kwargs: Any) -> None:
    pass


def fake_propagate_metadata(*args: Any, **kwargs: Any) -> None:
    pass


def fake_run_evaluation(*args: Any, **kwargs: Any) -> dict[str, float]:
    return {"train": 0.1, "val": 0.1}


def fake_get_lr(iter_num: int, schedule: LRSchedule, optim: OptimConfig) -> float:
    return 0.001


def fake_vectorize() -> Any:
    return None


# --- Strategies ---


@st.composite
def trainer_configs(draw: st.DrawFn) -> TrainerConfig:
    """Generate valid TrainerConfig."""
    # Minimal valid config
    return TrainerConfig(
        runtime=RuntimeConfig(
            device="cpu",
            dtype="float32",
            seed=draw(st.integers(0, 1000)),
            max_iters=draw(st.integers(1, 5)),  # Short run
            eval_interval=10,  # Don't eval often
            log_interval=10,
            eval_iters=1,
            out_dir=Path("."),
            tensorboard_enabled=False,
        ),
        data=DataConfig(
            batch_size=1,
            block_size=1,
            grad_accum_steps=draw(st.integers(1, 3)),
        ),
        model=ModelConfig(n_layer=1, n_head=1, n_embd=1, vocab_size=10, block_size=1),
        optim=OptimConfig(),
        schedule=LRSchedule(),
        logger=FakeLogger(),
    )


# --- Tests ---


@given(cfg=trainer_configs())
@settings(
    max_examples=5,
    deadline=None,
    derandomize=True,
    suppress_health_check=[HealthCheck.function_scoped_fixture],
)
def test_trainer_runs_successfully(cfg: TrainerConfig, tmp_path: Path) -> None:
    """Trainer should run for max_iters without error."""

    # Create a modified copy of runtime config with correct path
    # model_copy bypasses validation but creates a valid object structure
    runtime = cfg.runtime.model_copy(update={"out_dir": tmp_path})

    # Create modified TrainerConfig
    active_cfg = cfg.model_copy(update={"runtime": runtime, "logger": FakeLogger()})

    shared = SharedConfig(
        experiment="test",
        config_path=tmp_path / "config.toml",
        project_home=tmp_path,
        dataset_dir=tmp_path,
        train_out_dir=tmp_path,
        sample_out_dir=tmp_path,
    )

    deps = TrainerDependencies(
        initialize_batches=fake_initialize_batches,
        initialize_model=fake_initialize_model,
        initialize_components=fake_initialize_components,
        create_manager=fake_create_manager,
        load_checkpoint=fake_load_checkpoint,
        apply_checkpoint=fake_apply_checkpoint,
        save_checkpoint=fake_save_checkpoint,
        propagate_metadata=fake_propagate_metadata,
        run_evaluation=fake_run_evaluation,
        get_lr=fake_get_lr,
        vectorize=fake_vectorize,
    )

    trainer = Trainer(active_cfg, shared, deps=deps)

    iter_num, _ = trainer.run()

    assert iter_num > 0
