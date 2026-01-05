"""Property-based tests for MLflow integration and configuration."""

from __future__ import annotations

from pathlib import Path

from hypothesis import given, strategies as st
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


class FakeLogger:
    def __init__(self) -> None:
        self.infos: list[str] = []
        self.warnings: list[str] = []
        self.debugs: list[str] = []
        self.errors: list[str] = []

    def info(self, msg, *args, **kwargs):
        self.infos.append(msg)

    def warning(self, msg, *args, **kwargs):
        self.warnings.append(msg)

    def debug(self, msg, *args, **kwargs):
        self.debugs.append(msg)

    def error(self, msg, *args, **kwargs):
        self.errors.append(msg)


class FakeMLflowClient:
    def __init__(self) -> None:
        self.tracking_uri: str | None = None
        self.experiment_name: str | None = None
        self.params: dict[str, object] = {}
        self.metrics: list[tuple[dict[str, float], int | None]] = []
        self.artifacts: list[tuple[str, str | None]] = []
        self.tags: dict[str, object] = {}
        self.start_run_called = 0
        self.end_run_called = 0
        self.should_fail_start = False
        self.should_fail_log = False

    def set_tracking_uri(self, uri: str, /) -> None:
        self.tracking_uri = uri

    def set_experiment(self, experiment_name: str, /) -> None:
        self.experiment_name = experiment_name

    def start_run(self, *, run_name: str | None = None, description: str | None = None):
        del run_name, description
        if self.should_fail_start:
            raise RuntimeError("down")
        self.start_run_called += 1
        return object()

    def end_run(self) -> None:
        self.end_run_called += 1

    def log_params(self, params: dict[str, object], /) -> None:
        if self.should_fail_log:
            raise RuntimeError("fail log")
        self.params.update(params)

    def log_metrics(
        self, metrics: dict[str, float], /, *, step: int | None = None
    ) -> None:
        if self.should_fail_log:
            raise RuntimeError("fail log")
        self.metrics.append((metrics, step))

    def log_artifact(self, local_path: str, /, *, artifact_path: str | None = None):
        if self.should_fail_log:
            raise RuntimeError("fail log")
        self.artifacts.append((local_path, artifact_path))

    def log_artifacts(
        self, local_dir: str, /, *, artifact_path: str | None = None
    ) -> None:
        if self.should_fail_log:
            raise RuntimeError("fail log")
        self.artifacts.append((local_dir, artifact_path))

    def set_tag(self, key: str, value: object, /) -> None:
        self.tags[key] = value


# Strategies for MLflow config
mlflow_st = st.fixed_dictionaries(
    {
        "mlflow_enabled": st.booleans(),
        "mlflow_tracking_uri": st.one_of(st.none(), st.text(min_size=1)),
        "mlflow_experiment_name": st.one_of(st.none(), st.text(min_size=1)),
        "mlflow_run_name": st.one_of(st.none(), st.text(min_size=1)),
        "mlflow_log_system_metrics": st.booleans(),
    }
)


@given(mlflow_st)
def test_mlflow_manager_initialization_properties(mlflow_params):
    """Test that MLflowManager initializes correctly with various configurations."""
    runtime = RuntimeConfig(out_dir=Path("out"), **mlflow_params)
    shared = SharedConfig(
        experiment="test_exp",
        config_path=Path("config.toml"),
        project_home=Path("."),
        dataset_dir=Path("data"),
        train_out_dir=Path("out"),
        sample_out_dir=Path("sample"),
    )
    logger = FakeLogger()

    manager = MLflowManager(runtime, shared, logger)
    assert manager.cfg == runtime
    assert manager.shared == shared
    assert manager.logger == logger
    assert manager._active_run is None


def test_mlflow_setup_properties():
    """Test MLflow setup behavior based on enablement."""
    runtime = RuntimeConfig(
        out_dir=Path("out"),
        mlflow_enabled=True,
        mlflow_tracking_uri="http://localhost:5000",
        mlflow_experiment_name="test_experiment",
    )
    shared = SharedConfig(
        experiment="test_exp",
        config_path=Path("config.toml"),
        project_home=Path("."),
        dataset_dir=Path("data"),
        train_out_dir=Path("out"),
        sample_out_dir=Path("sample"),
    )
    logger = FakeLogger()
    mlflow_client = FakeMLflowClient()

    manager = MLflowManager(runtime, shared, logger, mlflow_client=mlflow_client)
    manager.setup()

    assert mlflow_client.tracking_uri == "http://localhost:5000"
    assert mlflow_client.experiment_name == "test_experiment"
    assert mlflow_client.start_run_called == 1


def test_mlflow_log_config_properties(tmp_path: Path):
    """Test config logging properties."""
    runtime = RuntimeConfig(out_dir=tmp_path / "out", mlflow_enabled=True)
    shared = SharedConfig(
        experiment="test_exp",
        config_path=tmp_path / "test_config.toml",
        project_home=tmp_path,
        dataset_dir=tmp_path / "data",
        train_out_dir=runtime.out_dir,
        sample_out_dir=tmp_path / "sample",
    )
    shared.config_path.parent.mkdir(parents=True, exist_ok=True)
    shared.config_path.write_text("model.n_layer = 1\n")

    trainer_cfg = TrainerConfig(
        model=ModelConfig(),
        data=DataConfig(),
        optim=OptimConfig(),
        schedule=LRSchedule(),
        runtime=runtime,
    )

    mlflow_client = FakeMLflowClient()
    manager = MLflowManager(runtime, shared, FakeLogger(), mlflow_client=mlflow_client)
    manager._active_run = object()
    manager.log_config(trainer_cfg)

    assert mlflow_client.params  # logged config params
    assert any(
        "config" in art[1] if art[1] else False for art in mlflow_client.artifacts
    )
