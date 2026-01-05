"""Unit tests for MLflow integration and coverage hardening."""

from __future__ import annotations

from pathlib import Path
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
from ml_playground.training.mlflow_integration import MLflowManager
from ml_playground.training.loop.runner import Trainer, default_trainer_dependencies


def create_mlflow_manager(
    runtime: RuntimeConfig,
    shared: SharedConfig,
    logger: Any,
    mlflow_client: Any = None,
    os_module: Any = None,
    platform_module: Any = None,
    sys_module: Any = None,
) -> MLflowManager:
    return MLflowManager(
        runtime,
        shared,
        logger,
        mlflow_client=mlflow_client,
        os_module=os_module or default_trainer_dependencies.os,
        platform_module=platform_module or default_trainer_dependencies.platform,
        sys_module=sys_module or default_trainer_dependencies.sys,
    )


class FakeLogger:
    def __init__(self):
        self.infos = []
        self.warnings = []
        self.debugs = []
        self.errors = []

    def info(self, msg, *args, **kwargs):
        self.infos.append(msg)

    def warning(self, msg, *args, **kwargs):
        self.warnings.append(msg)

    def debug(self, msg, *args, **kwargs):
        self.debugs.append(msg)

    def error(self, msg, *args, **kwargs):
        self.errors.append(msg)


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


class FakeMLflowClient:
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
        if self.should_fail_tag and key == "mlflow.note.content":
            raise Exception("Tag failed")
        self.tags[key] = value


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


class FakeWriter:
    def __init__(self):
        self.scalars = {}
        self.closed = False

    def add_scalar(self, name, value, step):
        if name not in self.scalars:
            self.scalars[name] = []
        self.scalars[name].append((value, step))

    def close(self):
        self.closed = True


class FakeCkptMgr:
    def __init__(self):
        self.saved = []
        self.out_dir = Path("/tmp/fake_out")

    def save(self, *args, **kwargs):
        self.saved.append(args)


class FakeDeps:
    def __init__(self):
        self.vmap = None
        self.save_checkpoint_called = False
        self.initialize_batches_called = False
        self.initialize_model_called = False
        self.initialize_components_called = False
        self.load_checkpoint_called = False
        self.create_manager_called = False
        self.create_mlflow_manager_called = False
        self.propagate_metadata_called = False
        self.run_evaluation_called = False
        self.get_lr_called = False
        self.apply_checkpoint_called = False
        self.train_step = None

        self.eval_result = {"val": 0.5, "train": 0.5}
        self.lr_result = 0.001
        self.checkpoint_to_load = None

    def initialize_batches(self, cfg, shared):
        self.initialize_batches_called = True
        return FakeBatches()

    def initialize_model(self, cfg, logger):
        self.initialize_model_called = True
        return FakeModel(), FakeOptimizer()

    def initialize_components(self, model, cfg, runtime, log_dir):
        self.initialize_components_called = True
        return model, FakeScaler(), None, FakeWriter()

    def load_checkpoint(self, mgr, cfg, logger):
        self.load_checkpoint_called = True
        return self.checkpoint_to_load

    def create_manager(self, cfg, shared):
        self.create_manager_called = True
        return FakeCkptMgr()

    def create_mlflow_manager(self, runtime, shared, logger, **kwargs):
        self.create_mlflow_manager_called = True
        return MLflowManager(runtime, shared, logger, mlflow_client=FakeMLflowClient())

    def save_checkpoint(self, *args, **kwargs):
        self.save_checkpoint_called = True

    def propagate_metadata(self, *args, **kwargs):
        self.propagate_metadata_called = True

    def run_evaluation(self, *args, **kwargs):
        self.run_evaluation_called = True
        return self.eval_result

    def get_lr(self, *args, **kwargs):
        self.get_lr_called = True
        return self.lr_result

    def apply_checkpoint(self, *args, **kwargs):
        self.apply_checkpoint_called = True
        if self.checkpoint_to_load:
            return (
                self.checkpoint_to_load.iter_num,
                self.checkpoint_to_load.best_val_loss,
            )
        return 0, 1e9


class FakeOS:
    def __init__(self, cwd="/tmp", login="user"):
        self.cwd = cwd
        self.login = login
        self.should_fail_login = False

    def getcwd(self) -> str:
        return self.cwd

    def getlogin(self) -> str:
        if self.should_fail_login:
            raise AttributeError("no login")
        return self.login


class FakePlatform:
    def __init__(self, plat="linux", proc="x86"):
        self._plat = plat
        self._proc = proc

    def platform(self) -> str:
        return self._plat

    def processor(self) -> str:
        return self._proc


class FakeSys:
    def __init__(self, version="3.13.0", argv=None):
        self.version = version
        self.argv = argv or ["train.py"]


@pytest.fixture
def fake_mlflow():
    return FakeMLflowClient()


@pytest.fixture
def mlflow_manager(tmp_path, fake_mlflow):
    runtime = RuntimeConfig(out_dir=tmp_path / "out", mlflow_enabled=True)
    shared = SharedConfig(
        experiment="test_exp",
        config_path=tmp_path / "config.toml",
        project_home=tmp_path,
        dataset_dir=tmp_path / "data",
        train_out_dir=tmp_path / "out",
        sample_out_dir=tmp_path / "sample",
    )
    shared.config_path.touch()
    return MLflowManager(
        runtime,
        shared,
        FakeLogger(),
        mlflow_client=fake_mlflow,
        os_module=FakeOS(),
        platform_module=FakePlatform(),
        sys_module=FakeSys(),
    )


def test_mlflow_setup_disabled(tmp_path, fake_mlflow):
    """Test setup does nothing if disabled."""
    runtime = RuntimeConfig(out_dir=tmp_path / "out", mlflow_enabled=False)
    shared = SharedConfig(
        experiment="test_exp",
        config_path=tmp_path / "config.toml",
        project_home=tmp_path,
        dataset_dir=tmp_path / "data",
        train_out_dir=tmp_path / "out",
        sample_out_dir=tmp_path / "sample",
    )
    manager = MLflowManager(runtime, shared, FakeLogger(), mlflow_client=fake_mlflow)
    manager.setup()
    assert fake_mlflow.start_run_called == 0


def test_mlflow_setup_error_handling(mlflow_manager, fake_mlflow):
    """Test setup handles exceptions gracefully."""
    fake_mlflow.should_fail_start = True
    mlflow_manager.setup()
    assert mlflow_manager._active_run is None
    assert len(mlflow_manager.logger.warnings) > 0


def test_mlflow_log_config_no_run(mlflow_manager):
    """Test log_config does nothing if no active run."""
    mlflow_manager.log_config(
        TrainerConfig(
            model=ModelConfig(),
            data=DataConfig(),
            optim=OptimConfig(),
            schedule=LRSchedule(),
            runtime=mlflow_manager.cfg,
        )
    )  # Should not raise


def test_mlflow_log_config_error(mlflow_manager, fake_mlflow):
    """Test log_config handles exceptions."""
    mlflow_manager._active_run = object()
    fake_mlflow.should_fail_log = True
    mlflow_manager.log_config(
        TrainerConfig(
            model=ModelConfig(),
            data=DataConfig(),
            optim=OptimConfig(),
            schedule=LRSchedule(),
            runtime=mlflow_manager.cfg,
        )
    )
    assert len(mlflow_manager.logger.warnings) > 0


def test_mlflow_log_metrics_no_run(mlflow_manager):
    """Test log_metrics does nothing if no active run."""
    mlflow_manager.log_metrics({"a": 1}, 1)  # Should not raise


def test_mlflow_log_artifact_no_run(mlflow_manager):
    """Test log_artifact does nothing if no active run."""
    mlflow_manager.log_artifact(Path("test"))  # Should not raise


def test_mlflow_log_artifact_dir(mlflow_manager, tmp_path, fake_mlflow):
    """Test logging a directory artifact."""
    mlflow_manager._active_run = object()
    test_dir = tmp_path / "artifacts"
    test_dir.mkdir()
    mlflow_manager.log_artifact(test_dir)
    assert any(str(test_dir) in art[0] for art in fake_mlflow.artifacts)


def test_mlflow_log_reproducibility_full(mlflow_manager, fake_mlflow):
    """Log environment and seed information coverage hardening."""
    mlflow_manager._active_run = object()
    mlflow_manager._log_reproducibility_info()
    assert len(fake_mlflow.params) > 0
    assert len(fake_mlflow.tags) > 0


def test_mlflow_log_metrics_success(mlflow_manager, fake_mlflow):
    """Log metrics coverage hardening."""
    mlflow_manager._active_run = object()
    mlflow_manager.log_metrics({"acc": 0.9}, step=10)
    assert any(m[0]["acc"] == 0.9 and m[1] == 10 for m in fake_mlflow.metrics)


def test_mlflow_log_metrics_failure(mlflow_manager, fake_mlflow):
    """Log metrics error coverage hardening."""
    mlflow_manager._active_run = object()
    fake_mlflow.should_fail_log = True
    mlflow_manager.log_metrics({"acc": 0.9}, step=10)
    assert len(mlflow_manager.logger.debugs) > 0


def test_mlflow_log_artifact_file(mlflow_manager, tmp_path, fake_mlflow):
    """Log file artifact coverage hardening."""
    mlflow_manager._active_run = object()
    test_file = tmp_path / "test.txt"
    test_file.touch()
    mlflow_manager.log_artifact(test_file)
    assert any(str(test_file) in art[0] for art in fake_mlflow.artifacts)


def test_mlflow_log_artifact_failure(mlflow_manager, tmp_path, fake_mlflow):
    """Log artifact failure coverage hardening."""
    mlflow_manager._active_run = object()
    test_file = tmp_path / "fail.txt"
    test_file.touch()
    fake_mlflow.should_fail_log = True
    mlflow_manager.log_artifact(test_file)
    assert len(mlflow_manager.logger.warnings) > 0


def test_mlflow_setup_full(mlflow_manager, fake_mlflow):
    """Full setup coverage including tracking URI and experiment name."""
    mlflow_manager.cfg = RuntimeConfig(
        out_dir=mlflow_manager.cfg.out_dir,
        mlflow_enabled=True,
        mlflow_tracking_uri="http://localhost:5000",
        mlflow_experiment_name="custom_exp",
        mlflow_run_name="custom_run",
        mlflow_log_system_metrics=True,
    )
    mlflow_manager.setup()
    assert fake_mlflow.tracking_uri == "http://localhost:5000"
    assert fake_mlflow.experiment_name == "custom_exp"
    assert fake_mlflow.start_run_called == 1


def test_mlflow_log_config_success(mlflow_manager, fake_mlflow):
    """Successful config logging coverage."""
    mlflow_manager._active_run = object()
    trainer_cfg = TrainerConfig(
        model=ModelConfig(),
        data=DataConfig(),
        optim=OptimConfig(),
        schedule=LRSchedule(),
        runtime=mlflow_manager.cfg,
    )
    mlflow_manager.log_config(trainer_cfg)
    assert len(fake_mlflow.params) > 0
    assert len(fake_mlflow.artifacts) > 0


def test_mlflow_setup_minimal(mlflow_manager, fake_mlflow):
    """Setup with minimal config (no custom URI or run name)."""
    mlflow_manager.cfg = RuntimeConfig(
        out_dir=mlflow_manager.cfg.out_dir,
        mlflow_enabled=True,
    )
    mlflow_manager.setup()
    assert fake_mlflow.experiment_name == mlflow_manager.shared.experiment
    assert fake_mlflow.start_run_called == 1


def test_mlflow_setup_system_metrics_tag_fail(mlflow_manager, fake_mlflow):
    """Coverage for system metrics tag failure."""
    mlflow_manager.cfg = RuntimeConfig(
        out_dir=mlflow_manager.cfg.out_dir,
        mlflow_enabled=True,
        mlflow_log_system_metrics=True,
    )
    fake_mlflow.should_fail_tag = True
    mlflow_manager.setup()
    assert len(mlflow_manager.logger.debugs) > 0


def test_mlflow_log_reproducibility_no_getlogin(mlflow_manager, fake_mlflow):
    """Coverage for reproducibility info when getlogin fails."""
    mlflow_manager._active_run = object()
    fake_os = FakeOS()
    fake_os.should_fail_login = True
    mlflow_manager._os = fake_os
    mlflow_manager._log_reproducibility_info()
    assert fake_mlflow.tags["mlflow.user"] == "unknown"


def test_mlflow_setup_system_metrics_disabled(mlflow_manager, fake_mlflow):
    """Coverage for setup when system metrics are disabled."""
    mlflow_manager.cfg = RuntimeConfig(
        out_dir=mlflow_manager.cfg.out_dir,
        mlflow_enabled=True,
        mlflow_log_system_metrics=False,
    )
    mlflow_manager.setup()
    assert "mlflow.note.content" not in fake_mlflow.tags


def test_mlflow_log_config_no_config_file(mlflow_manager, tmp_path, fake_mlflow):
    """Coverage for log_config when config file is missing."""
    mlflow_manager._active_run = object()
    mlflow_manager.shared = SharedConfig(
        experiment="test",
        config_path=tmp_path / "non_existent.toml",
        project_home=tmp_path,
        dataset_dir=tmp_path / "data",
        train_out_dir=tmp_path / "out",
        sample_out_dir=tmp_path / "sample",
    )
    trainer_cfg = TrainerConfig(
        model=ModelConfig(),
        data=DataConfig(),
        optim=OptimConfig(),
        schedule=LRSchedule(),
        runtime=mlflow_manager.cfg,
    )
    mlflow_manager.log_config(trainer_cfg)
    assert not any("config" in art[1] for art in fake_mlflow.artifacts if art[1])


def test_mlflow_finish_no_run(mlflow_manager, fake_mlflow):
    """Coverage for finish when no run is active."""
    mlflow_manager._active_run = None
    mlflow_manager.finish()
    assert fake_mlflow.end_run_called == 0


def test_mlflow_finish_success(mlflow_manager, fake_mlflow):
    """Coverage for finish when a run is active."""
    mlflow_manager._active_run = object()
    mlflow_manager.finish()
    assert fake_mlflow.end_run_called == 1
    assert mlflow_manager._active_run is None


def test_trainer_run_keyboard_interrupt(tmp_path):
    """Harden Trainer.run KeyboardInterrupt handling."""
    out_dir = tmp_path / "out"
    config_path = tmp_path / "config.toml"
    config_path.touch()

    cfg = TrainerConfig(
        model=ModelConfig(n_layer=1, n_head=1, n_embd=32, block_size=32),
        data=DataConfig(block_size=32, batch_size=1, grad_accum_steps=1),
        optim=OptimConfig(),
        schedule=LRSchedule(),
        runtime=RuntimeConfig(out_dir=out_dir, max_iters=1),
    )
    shared = SharedConfig(
        experiment="test",
        config_path=config_path,
        project_home=tmp_path,
        dataset_dir=tmp_path / "data",
        train_out_dir=out_dir,
        sample_out_dir=tmp_path / "sample",
    )

    class InterruptDeps(FakeDeps):
        def initialize_batches(self, cfg, shared):
            class ExplodingBatches:
                def get_batch(self, split):
                    raise KeyboardInterrupt()

            return ExplodingBatches()

    deps = InterruptDeps()
    logger = FakeLogger()
    cfg = cfg.model_copy(update={"logger": logger})
    trainer = Trainer(cfg, shared, deps=deps)  # type: ignore

    try:
        trainer.run()
    except KeyboardInterrupt:
        pass
    assert any("Training loop interrupted" in msg for msg in logger.infos)


def test_trainer_run_base_exception(tmp_path):
    """Harden Trainer.run BaseException handling."""
    out_dir = tmp_path / "out"
    config_path = tmp_path / "config.toml"
    config_path.touch()

    cfg = TrainerConfig(
        model=ModelConfig(n_layer=1, n_head=1, n_embd=32, block_size=32),
        data=DataConfig(block_size=32, batch_size=1, grad_accum_steps=1),
        optim=OptimConfig(),
        schedule=LRSchedule(),
        runtime=RuntimeConfig(out_dir=out_dir, max_iters=1),
    )
    shared = SharedConfig(
        experiment="test",
        config_path=config_path,
        project_home=tmp_path,
        dataset_dir=tmp_path / "data",
        train_out_dir=out_dir,
        sample_out_dir=tmp_path / "sample",
    )

    class FatalDeps(FakeDeps):
        def initialize_batches(self, cfg, shared):
            class ExplodingBatches:
                def get_batch(self, split):
                    raise RuntimeError("Fatal")

            return ExplodingBatches()

    deps = FatalDeps()
    trainer = Trainer(cfg, shared, deps=deps)  # type: ignore
    with pytest.raises(RuntimeError, match="Fatal"):
        trainer.run()


def test_trainer_run_full_loop(tmp_path):
    """Harden Trainer.run full loop coverage."""
    out_dir = tmp_path / "out"
    config_path = tmp_path / "config.toml"
    config_path.touch()

    cfg = TrainerConfig(
        model=ModelConfig(n_layer=1, n_head=1, n_embd=32, block_size=32),
        data=DataConfig(block_size=32, batch_size=1, grad_accum_steps=1),
        optim=OptimConfig(),
        schedule=LRSchedule(),
        runtime=RuntimeConfig(
            out_dir=out_dir, max_iters=2, eval_interval=1, eval_iters=1, log_interval=1
        ),
    )
    shared = SharedConfig(
        experiment="test",
        config_path=config_path,
        project_home=tmp_path,
        dataset_dir=tmp_path / "data",
        train_out_dir=out_dir,
        sample_out_dir=tmp_path / "sample",
    )

    mock_model = FakeModel()

    class FakeFullDeps(FakeDeps):
        def __init__(self):
            super().__init__()
            self.eval_result = {"val": 0.4, "train": 0.5}

        def initialize_model(self, cfg, logger):
            return mock_model, FakeOptimizer()

        def initialize_components(self, model, cfg, runtime, log_dir):
            return model, FakeScaler(), None, FakeWriter()

        def train_step(self, trainer_obj, X, Y):
            return torch.tensor(0.5, requires_grad=True)

    deps = FakeFullDeps()
    trainer = Trainer(cfg, shared, deps=deps)  # type: ignore
    trainer.run()

    assert deps.save_checkpoint_called


def test_trainer_run_evaluation_logic(tmp_path):
    """Harden Trainer.run evaluation branch logic."""
    out_dir = tmp_path / "out"
    config_path = tmp_path / "config.toml"
    config_path.touch()
    cfg = TrainerConfig(
        model=ModelConfig(),
        data=DataConfig(),
        optim=OptimConfig(),
        schedule=LRSchedule(),
        runtime=RuntimeConfig(
            out_dir=out_dir, max_iters=1, eval_interval=1, eval_iters=1
        ),
    )
    shared = SharedConfig(
        experiment="test",
        config_path=config_path,
        project_home=tmp_path,
        dataset_dir=tmp_path / "data",
        train_out_dir=out_dir,
        sample_out_dir=tmp_path / "sample",
    )

    mock_model = FakeModel()

    class FakeEvalDeps(FakeDeps):
        def __init__(self):
            super().__init__()
            self.eval_result = {"val": 0.1}

        def initialize_model(self, cfg, logger):
            return mock_model, FakeOptimizer()

        def initialize_components(self, model, cfg, runtime, log_dir):
            return model, FakeScaler(), None, None

        def train_step(self, trainer_obj, X, Y):
            return torch.tensor(0.5, requires_grad=True)

    deps = FakeEvalDeps()
    trainer = Trainer(cfg, shared, deps=deps)  # type: ignore
    trainer.best_val_loss = 0.5
    trainer.run()
    assert trainer.best_val_loss == 0.1
    assert deps.save_checkpoint_called


def test_trainer_step_accum_python(tmp_path):
    """Harden Trainer._train_step_python."""
    out_dir = tmp_path / "out"
    config_path = tmp_path / "config.toml"
    config_path.touch()

    cfg = TrainerConfig(
        model=ModelConfig(),
        data=DataConfig(grad_accum_steps=2),
        optim=OptimConfig(),
        schedule=LRSchedule(),
        runtime=RuntimeConfig(out_dir=out_dir),
    )
    shared = SharedConfig(
        experiment="test",
        config_path=config_path,
        project_home=tmp_path,
        dataset_dir=tmp_path / "data",
        train_out_dir=out_dir,
        sample_out_dir=tmp_path / "sample",
    )

    mock_model = FakeModel()

    class FakeAccumDeps(FakeDeps):
        def initialize_model(self, cfg, logger):
            return mock_model, FakeOptimizer()

    deps = FakeAccumDeps()
    trainer = Trainer(cfg, shared, deps=deps)  # type: ignore
    trainer.batches = FakeBatches()
    trainer.model = mock_model

    X = torch.zeros((2, 32), dtype=torch.long)
    Y = torch.zeros((2, 32), dtype=torch.long)

    loss = trainer._train_step_python(X, Y, 2)
    assert isinstance(loss, torch.Tensor)


def test_trainer_step_vmap_success(tmp_path):
    """Harden Trainer._train_step_vmap."""
    out_dir = tmp_path / "out"
    config_path = tmp_path / "config.toml"
    config_path.touch()

    cfg = TrainerConfig(
        model=ModelConfig(),
        data=DataConfig(grad_accum_steps=2),
        optim=OptimConfig(),
        schedule=LRSchedule(),
        runtime=RuntimeConfig(out_dir=out_dir),
    )
    shared = SharedConfig(
        experiment="test",
        config_path=config_path,
        project_home=tmp_path,
        dataset_dir=tmp_path / "data",
        train_out_dir=out_dir,
        sample_out_dir=tmp_path / "sample",
    )

    class FakeVmapDeps(FakeDeps):
        def __init__(self):
            super().__init__()
            self.vmap_called = False

        def vmap_impl(self, func):
            self.vmap_called = True

            def wrapped(x, y):
                return torch.tensor([0.4, 0.6], requires_grad=True)

            return wrapped

    deps = FakeVmapDeps()
    # Explicitly set the vmap attribute to our implementation
    object.__setattr__(deps, "vmap", deps.vmap_impl)

    trainer = Trainer(cfg, shared, deps=deps)  # type: ignore
    trainer.model = FakeModel()

    X = torch.zeros((32), dtype=torch.long)
    Y = torch.zeros((32), dtype=torch.long)

    loss = trainer._train_step_vmap(X, Y, 2)
    assert isinstance(loss, torch.Tensor)
    assert deps.vmap_called


def test_default_trainer_dependencies_coverage():
    """Harden default_trainer_dependencies."""
    deps = default_trainer_dependencies()
    assert callable(deps.initialize_batches)
    assert callable(deps.initialize_model)
    assert callable(deps.initialize_components)
    assert callable(deps.create_manager)
    assert callable(deps.load_checkpoint)
    assert callable(deps.apply_checkpoint)
    assert callable(deps.save_checkpoint)
    assert callable(deps.propagate_metadata)
    assert callable(deps.run_evaluation)
    assert callable(deps.get_lr)


def test_trainer_initialization_with_checkpoint(tmp_path):
    """Harden Trainer init with an existing checkpoint."""
    out_dir = tmp_path / "out"
    config_path = tmp_path / "config.toml"
    config_path.touch()
    cfg = TrainerConfig(
        model=ModelConfig(),
        data=DataConfig(),
        optim=OptimConfig(),
        schedule=LRSchedule(),
        runtime=RuntimeConfig(out_dir=out_dir),
    )
    shared = SharedConfig(
        experiment="test",
        config_path=config_path,
        project_home=tmp_path,
        dataset_dir=tmp_path / "data",
        train_out_dir=out_dir,
        sample_out_dir=tmp_path / "sample",
    )

    class CheckpointDeps(FakeDeps):
        def __init__(self):
            super().__init__()

            class FakeCheckpoint:
                def __init__(self):
                    self.iter_num = 100
                    self.best_val_loss = 0.5
                    self.model_args = {}
                    self.optimizer = {}
                    self.model = {}

            self.checkpoint_to_load = FakeCheckpoint()

    deps = CheckpointDeps()
    trainer = Trainer(cfg, shared, deps=deps)  # type: ignore
    assert trainer.iter_num == 100
    assert trainer.best_val_loss == 0.5
    assert deps.apply_checkpoint_called


def test_trainer_tensorboard_logging_modes(tmp_path):
    """Harden Trainer TensorBoard logging branch coverage."""
    out_dir = tmp_path / "out"
    config_path = tmp_path / "config.toml"
    config_path.touch()

    cfg = TrainerConfig(
        model=ModelConfig(),
        data=DataConfig(grad_accum_steps=1),
        optim=OptimConfig(),
        schedule=LRSchedule(),
        runtime=RuntimeConfig(
            out_dir=out_dir,
            max_iters=1,
            log_interval=1,
            tensorboard_update_mode="log",
        ),
    )
    shared = SharedConfig(
        experiment="test",
        config_path=config_path,
        project_home=tmp_path,
        dataset_dir=tmp_path / "data",
        train_out_dir=out_dir,
        sample_out_dir=tmp_path / "sample",
    )

    class FakeTBWriter(FakeWriter):
        def __init__(self):
            super().__init__()
            self.add_scalar_called = False

        def add_scalar(self, name, value, step):
            super().add_scalar(name, value, step)
            self.add_scalar_called = True

    writer = FakeTBWriter()

    class FakeTBDeps(FakeDeps):
        def __init__(self, writer):
            super().__init__()
            self.writer = writer

        def initialize_components(self, model, cfg, runtime, log_dir):
            return model, FakeScaler(), None, self.writer

        def train_step(self, trainer_obj, X, Y):
            return torch.tensor(0.5, requires_grad=True)

    deps = FakeTBDeps(writer)
    trainer = Trainer(cfg, shared, deps=deps)  # type: ignore
    trainer.run()
    assert writer.add_scalar_called


def test_trainer_cleanup_failures(tmp_path):
    """Harden Trainer cleanup failure paths."""
    out_dir = tmp_path / "out"
    config_path = tmp_path / "config.toml"
    config_path.touch()
    cfg = TrainerConfig(
        model=ModelConfig(),
        data=DataConfig(),
        optim=OptimConfig(),
        schedule=LRSchedule(),
        runtime=RuntimeConfig(out_dir=out_dir, max_iters=0),
    )
    shared = SharedConfig(
        experiment="test",
        config_path=config_path,
        project_home=tmp_path,
        dataset_dir=tmp_path / "data",
        train_out_dir=out_dir,
        sample_out_dir=tmp_path / "sample",
    )

    class FakeCleanupDeps(FakeDeps):
        def save_checkpoint(self, *args, **kwargs):
            raise RuntimeError("Save fail")

        def propagate_metadata(self, *args, **kwargs):
            raise Exception("Meta fail")

        def train_step(self, trainer_obj, X, Y):
            return torch.tensor(0.5, requires_grad=True)

    deps = FakeCleanupDeps()
    logger = FakeLogger()
    # Update frozen config via model_copy
    cfg = cfg.model_copy(update={"logger": logger})
    trainer = Trainer(cfg, shared, deps=deps)  # type: ignore
    trainer.run()
    assert len(logger.warnings) > 0


def test_trainer_eval_only_coverage(tmp_path):
    """Harden Trainer.run eval_only branch."""
    out_dir = tmp_path / "out"
    config_path = tmp_path / "config.toml"
    config_path.touch()
    cfg = TrainerConfig(
        model=ModelConfig(),
        data=DataConfig(),
        optim=OptimConfig(),
        schedule=LRSchedule(),
        runtime=RuntimeConfig(
            out_dir=out_dir, max_iters=10, eval_interval=1, eval_iters=1, eval_only=True
        ),
    )
    shared = SharedConfig(
        experiment="test",
        config_path=config_path,
        project_home=tmp_path,
        dataset_dir=tmp_path / "data",
        train_out_dir=out_dir,
        sample_out_dir=tmp_path / "sample",
    )

    class FakeEvalOnlyDeps(FakeDeps):
        def __init__(self):
            super().__init__()
            self.eval_result = {"val": 0.5}

        def train_step(self, trainer_obj, X, Y):
            return torch.tensor(0.5, requires_grad=True)

    deps = FakeEvalOnlyDeps()
    trainer = Trainer(cfg, shared, deps=deps)  # type: ignore
    trainer.run()
    # Should break after first eval (iter 0)
    assert trainer.iter_num == 0


def test_trainer_run_max_iters_break(tmp_path):
    """Harden the break condition when iter_num >= max_iters."""
    out_dir = tmp_path / "out"
    config_path = tmp_path / "config.toml"
    config_path.touch()
    cfg = TrainerConfig(
        model=ModelConfig(),
        data=DataConfig(),
        optim=OptimConfig(),
        schedule=LRSchedule(),
        runtime=RuntimeConfig(out_dir=out_dir, max_iters=0),  # Should break immediately
    )
    shared = SharedConfig(
        experiment="test",
        config_path=config_path,
        project_home=tmp_path,
        dataset_dir=tmp_path / "data",
        train_out_dir=out_dir,
        sample_out_dir=tmp_path / "sample",
    )

    class FakeMaxItersDeps(FakeDeps):
        def __init__(self):
            super().__init__()
            self.eval_result = {"val": 0.5}

        def train_step(self, trainer_obj, X, Y):
            return torch.tensor(0.5, requires_grad=True)

    deps = FakeMaxItersDeps()
    trainer = Trainer(cfg, shared, deps=deps)  # type: ignore
    trainer.run()
    assert trainer.iter_num == 0  # max_iters=0 means it breaks before step
