"""Acceptance tests for MLflow experiment tracking."""

from __future__ import annotations

from typing import Any, Dict, Optional

import pytest
import torch
from ml_playground.configuration.models import (
    DataConfig,
    LRSchedule,
    ModelConfig,
    OptimConfig,
    RuntimeConfig,
    SharedConfig,
    TrainerConfig,
)
from ml_playground.training.loop.runner import Trainer
from ml_playground.training.mlflow_integration import MLflowManager


class FakeLoggerAcceptance:
    def __init__(self):
        self.lr = 0.001
        self.infos = []

    def info(self, msg, *args, **kwargs):
        self.infos.append(msg)

    def warning(self, msg, *args, **kwargs):
        pass

    def debug(self, msg, *args, **kwargs):
        pass

    def error(self, msg, *args, **kwargs):
        pass


class FakeBatches:
    def __init__(self, x=None, y=None):
        self.x = x if x is not None else torch.zeros((1, 2))
        self.y = y if y is not None else torch.zeros((1, 2))

    def get_batch(self, split):
        return self.x, self.y


class FakeModel(torch.nn.Module):
    def __init__(self):
        super().__init__()
        self.param = torch.nn.Parameter(torch.ones(1))

    def forward(self, x, y=None):
        return None, self.param * x.sum()

    def estimate_mfu(self, *args, **kwargs):
        return 0.5


class FakeOptimizer:
    def __init__(self):
        self.param_groups = [{"lr": 0.0}]

    def zero_grad(self, set_to_none=True):
        pass

    def step(self, *args, **kwargs):
        pass


class FakeScaler:
    def __init__(self):
        self.enabled = False

    def scale(self, loss):
        return loss

    def step(self, optimizer):
        pass

    def update(self):
        pass

    def unscale_(self, optimizer):
        pass


class FakeMLflowClientAcceptance:
    def __init__(self):
        self.tracking_uri = None
        self.experiment_name = None
        self.active_run = object()
        self.params = {}
        self.metrics = []
        self.artifacts = []
        self.tags = {}
        self.start_run_called = 0
        self.end_run_called = 0
        self.should_fail_start = False
        self.should_fail_log = False
        self.should_fail_tag = False

    def set_tracking_uri(self, uri: str) -> None:
        self.tracking_uri = uri

    def set_experiment(self, experiment_name: str) -> Any:
        self.experiment_name = experiment_name

    def start_run(self, run_name=None, description=None) -> Any:
        if self.should_fail_start:
            raise RuntimeError("MLflow down")
        self.start_run_called += 1
        return self.active_run

    def end_run(self) -> None:
        self.end_run_called += 1

    def log_params(self, params: Dict[str, Any]) -> None:
        if self.should_fail_log:
            raise Exception("Log failed")
        self.params.update(params)

    def log_metrics(
        self, metrics: Dict[str, float], step: Optional[int] = None
    ) -> None:
        if self.should_fail_log:
            raise Exception("Log failed")
        self.metrics.append((metrics, step))

    def log_artifact(
        self, local_path: str, artifact_path: Optional[str] = None
    ) -> None:
        if self.should_fail_log:
            raise Exception("Log failed")
        self.artifacts.append((local_path, artifact_path))

    def log_artifacts(
        self, local_dir: str, artifact_path: Optional[str] = None
    ) -> None:
        if self.should_fail_log:
            raise Exception("Log failed")
        self.artifacts.append((local_dir, artifact_path))

    def set_tag(self, key: str, value: Any) -> None:
        if self.should_fail_tag:
            raise Exception("Tag failed")
        self.tags[key] = value


@pytest.fixture
def fake_mlflow():
    return FakeMLflowClientAcceptance()


class FakeDepsAcceptance:
    def __init__(self, mlflow_client: FakeMLflowClientAcceptance):
        self.vmap = None
        self.mlflow_client = mlflow_client
        self.save_checkpoint_called = False
        self.train_step = None
        self.lr_result = 0.001

    def initialize_batches(self, cfg, shared):
        return FakeBatches()

    def initialize_model(self, cfg, logger):
        return FakeModel(), FakeOptimizer()

    def initialize_components(self, model, cfg, runtime, log_dir):
        return model, FakeScaler(), None, None

    def create_manager(self, cfg, shared):
        return object()

    def create_mlflow_manager(self, runtime, shared, logger, **kwargs):
        return MLflowManager(runtime, shared, logger, mlflow_client=self.mlflow_client)

    def load_checkpoint(self, mgr, cfg, logger):
        return None

    def apply_checkpoint(self, *args, **kwargs):
        return 0, 1e9

    def save_checkpoint(self, *args, **kwargs):
        self.save_checkpoint_called = True

    def propagate_metadata(self, *args, **kwargs):
        pass

    def run_evaluation(self, *args, **kwargs):
        return {"val": 1.0, "train": 0.5}

    def get_lr(self, *args, **kwargs):
        return self.lr_result


def test_trainer_mlflow_lifecycle_acceptance(fake_mlflow, tmp_path):
    """
    Acceptance test: Verify that the Trainer correctly manages the MLflow lifecycle
    when enabled in the configuration.
    """
    out_dir = tmp_path / "out"
    config_path = tmp_path / "config.toml"
    config_path.touch()

    runtime = RuntimeConfig(
        out_dir=out_dir,
        mlflow_enabled=True,
        max_iters=1,
        eval_interval=10,
        eval_iters=1,
    )

    shared = SharedConfig(
        experiment="acceptance_test",
        config_path=config_path,
        project_home=tmp_path,
        dataset_dir=tmp_path / "data",
        train_out_dir=out_dir,
        sample_out_dir=tmp_path / "sample",
    )

    fake_logger = FakeLoggerAcceptance()
    trainer_cfg = TrainerConfig(
        model=ModelConfig(n_layer=1, n_head=1, n_embd=32, block_size=32),
        data=DataConfig(block_size=32, batch_size=1, grad_accum_steps=1),
        optim=OptimConfig(),
        schedule=LRSchedule(),
        runtime=runtime,
        logger=fake_logger,  # type: ignore
    )

    deps = FakeDepsAcceptance(fake_mlflow)
    trainer = Trainer(trainer_cfg, shared, deps=deps)  # type: ignore

    # Verify setup was called
    assert fake_mlflow.experiment_name == "acceptance_test"
    assert fake_mlflow.start_run_called == 1
    assert len(fake_mlflow.params) > 0
    assert any(str(config_path) in art[0] for art in fake_mlflow.artifacts)

    # Run one step
    trainer.run()

    # Verify metrics were logged
    assert len(fake_mlflow.metrics) > 0

    # Verify finish was called
    assert fake_mlflow.end_run_called == 1


def test_trainer_mlflow_disabled_acceptance(fake_mlflow, tmp_path):
    """
    Acceptance test: Verify that MLflow is NOT called when disabled.
    """
    out_dir = tmp_path / "out"
    config_path = tmp_path / "config.toml"
    config_path.touch()

    runtime = RuntimeConfig(
        out_dir=out_dir,
        mlflow_enabled=False,
        max_iters=1,
    )

    shared = SharedConfig(
        experiment="disabled_test",
        config_path=config_path,
        project_home=tmp_path,
        dataset_dir=tmp_path / "data",
        train_out_dir=out_dir,
        sample_out_dir=tmp_path / "sample",
    )

    trainer_cfg = TrainerConfig(
        model=ModelConfig(n_layer=1, n_head=1, n_embd=32, block_size=32),
        data=DataConfig(block_size=32, batch_size=1, grad_accum_steps=1),
        optim=OptimConfig(),
        schedule=LRSchedule(),
        runtime=runtime,
    )

    deps = FakeDepsAcceptance(fake_mlflow)
    Trainer(trainer_cfg, shared, deps=deps)  # type: ignore

    # Verify no MLflow calls
    assert fake_mlflow.start_run_called == 0
