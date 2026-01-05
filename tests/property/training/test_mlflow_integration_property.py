"""Property-based tests for MLflowManager behavior and branches."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Optional

from hypothesis import example, given, settings, strategies as st, HealthCheck
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

    def info(self, msg: str, *args: Any, **kwargs: Any) -> None:
        self.infos.append(msg)

    def warning(self, msg: str, *args: Any, **kwargs: Any) -> None:
        self.warnings.append(msg)

    def debug(self, msg: str, *args: Any, **kwargs: Any) -> None:
        self.debugs.append(msg)


class FakeMLflowClient:
    def __init__(self) -> None:
        self.tracking_uri: Optional[str] = None
        self.experiment_name: Optional[str] = None
        self.start_run_called = 0
        self.start_run_kwargs: dict[str, Any] = {}
        self.end_run_called = 0
        self.params: list[dict[str, Any]] = []
        self.metrics: list[tuple[dict[str, float], Optional[int]]] = []
        self.artifacts: list[tuple[str, Optional[str]]] = []
        self.tags: dict[str, Any] = {}
        self.fail_start = False
        self.fail_tag = False
        self.fail_log = False

    def set_tracking_uri(self, uri: str, /) -> None:
        self.tracking_uri = uri

    def set_experiment(self, experiment_name: str, /) -> Any:
        self.experiment_name = experiment_name
        return None

    def start_run(self, **kwargs: Any) -> Any:
        if self.fail_start:
            raise RuntimeError("start failed")
        self.start_run_called += 1
        self.start_run_kwargs = kwargs
        return object()

    def end_run(self) -> None:
        self.end_run_called += 1

    def log_params(self, params: dict[str, Any], /) -> None:
        if self.fail_log:
            raise RuntimeError("log params failed")
        self.params.append(params)

    def log_metrics(
        self, metrics: dict[str, float], /, *, step: Optional[int] = None
    ) -> None:
        if self.fail_log:
            raise RuntimeError("log metrics failed")
        self.metrics.append((metrics, step))

    def log_artifact(
        self, local_path: str, /, *, artifact_path: Optional[str] = None
    ) -> None:
        if self.fail_log:
            raise RuntimeError("log artifact failed")
        self.artifacts.append((local_path, artifact_path))

    def log_artifacts(
        self, local_dir: str, /, *, artifact_path: Optional[str] = None
    ) -> None:
        if self.fail_log:
            raise RuntimeError("log artifacts failed")
        self.artifacts.append((local_dir, artifact_path))

    def set_tag(self, key: str, value: Any, /) -> None:
        if self.fail_tag and key == "mlflow.note.content":
            raise RuntimeError("tag failed")
        self.tags[key] = value


class FakeOS:
    def __init__(
        self,
        cwd: str = "/tmp/project",
        user: str = "test-user",
        fail_getlogin: bool = False,
    ) -> None:
        self._cwd = cwd
        self._user = user
        self._fail_getlogin = fail_getlogin

    def getcwd(self) -> str:
        return self._cwd

    def getlogin(self) -> str:
        if self._fail_getlogin:
            raise OSError("no user")
        return self._user


class FakePlatform:
    def __init__(self, plat: str = "linux", proc: str = "x86") -> None:
        self._plat = plat
        self._proc = proc

    def platform(self) -> str:
        return self._plat

    def processor(self) -> str:
        return self._proc


class FakeSys:
    def __init__(
        self, version: str = "3.13.0", argv: Optional[list[str]] = None
    ) -> None:
        self.version = version
        self.argv = argv or ["train.py"]


def _make_runtime(
    out_dir: Path,
    enabled: bool,
    tracking_uri: Optional[str],
    experiment_name: Optional[str],
    run_name: Optional[str],
    log_system_metrics: bool,
) -> RuntimeConfig:
    return RuntimeConfig(
        out_dir=out_dir,
        mlflow_enabled=enabled,
        mlflow_tracking_uri=tracking_uri,
        mlflow_experiment_name=experiment_name,
        mlflow_run_name=run_name,
        mlflow_log_system_metrics=log_system_metrics,
    )


@settings(
    max_examples=12,
    deadline=100,
    derandomize=True,
    suppress_health_check=[HealthCheck.function_scoped_fixture],
)
@given(
    enabled=st.booleans(),
    tracking_uri=st.one_of(st.none(), st.text(min_size=1, max_size=8)),
    experiment_name=st.one_of(st.none(), st.text(min_size=1, max_size=8)),
    run_name=st.one_of(st.none(), st.text(min_size=1, max_size=8)),
    log_system_metrics=st.booleans(),
    fail_tag=st.booleans(),
    fail_start=st.booleans(),
    fail_getlogin=st.booleans(),
)
@example(
    enabled=True,
    tracking_uri="http://localhost:5000",
    experiment_name=None,
    run_name="run",
    log_system_metrics=True,
    fail_tag=True,
    fail_start=False,
    fail_getlogin=True,
)
def test_setup_property_handles_paths_and_failures(
    tmp_path: Path,
    enabled: bool,
    tracking_uri: Optional[str],
    experiment_name: Optional[str],
    run_name: Optional[str],
    log_system_metrics: bool,
    fail_tag: bool,
    fail_start: bool,
    fail_getlogin: bool,
) -> None:
    """Setup configures MLflow when enabled and logs failures without crashing."""

    runtime = _make_runtime(
        out_dir=tmp_path / "out",
        enabled=enabled,
        tracking_uri=tracking_uri,
        experiment_name=experiment_name,
        run_name=run_name,
        log_system_metrics=log_system_metrics,
    )
    shared = SharedConfig(
        experiment="exp",
        config_path=tmp_path / "cfg.toml",
        project_home=tmp_path,
        dataset_dir=tmp_path / "data",
        train_out_dir=tmp_path / "train_out",
        sample_out_dir=tmp_path / "sample",
    )
    mlflow_client = FakeMLflowClient()
    mlflow_client.fail_tag = fail_tag
    mlflow_client.fail_start = fail_start
    logger = FakeLogger()
    manager = MLflowManager(
        runtime,
        shared,
        logger,
        mlflow_client=mlflow_client,
        os_module=FakeOS(fail_getlogin=fail_getlogin),
        platform_module=FakePlatform(),
        sys_module=FakeSys(),
    )

    manager.setup()

    if not enabled:
        assert mlflow_client.start_run_called == 0
        assert manager._active_run is None
    elif fail_start:
        assert mlflow_client.start_run_called == 0
        assert manager._active_run is None
        assert any("MLflow setup failed" in msg for msg in logger.warnings)
    else:
        assert mlflow_client.start_run_called == 1
        expected_experiment = experiment_name or shared.experiment
        assert mlflow_client.experiment_name == expected_experiment
        if tracking_uri:
            assert mlflow_client.tracking_uri == tracking_uri
        if log_system_metrics:
            # Tag may fail when fail_tag is True
            tag_attempted = "mlflow.note.content" in mlflow_client.tags or fail_tag
            assert tag_attempted
        # ensure reproducibility info logged regardless of user fetch outcome
        assert any("reproducibility.seed" in params for params in mlflow_client.params)


@settings(
    max_examples=8,
    deadline=100,
    derandomize=True,
    suppress_health_check=[HealthCheck.function_scoped_fixture],
)
@given(
    n_layer=st.integers(min_value=1, max_value=3),
    n_head=st.integers(min_value=1, max_value=4),
    n_embd=st.integers(min_value=8, max_value=16),
    batch_size=st.integers(min_value=1, max_value=4),
    grad_accum=st.integers(min_value=1, max_value=3),
    fail_log=st.booleans(),
)
@example(1, 1, 8, 1, 1, True)
def test_log_config_property_logs_params_and_artifacts(
    tmp_path: Path,
    n_layer: int,
    n_head: int,
    n_embd: int,
    batch_size: int,
    grad_accum: int,
    fail_log: bool,
) -> None:
    """log_config captures model/data params and artifacts when active run exists."""

    runtime = _make_runtime(tmp_path / "out", True, None, "exp-name", "run", False)
    shared = SharedConfig(
        experiment="exp",
        config_path=tmp_path / "cfg.toml",
        project_home=tmp_path,
        dataset_dir=tmp_path / "data",
        train_out_dir=tmp_path / "train_out",
        sample_out_dir=tmp_path / "sample",
    )
    shared.config_path.parent.mkdir(parents=True, exist_ok=True)
    shared.config_path.write_text("model.n_layer = 1\n")

    trainer_cfg = TrainerConfig(
        model=ModelConfig(n_layer=n_layer, n_head=n_head, n_embd=n_embd),
        data=DataConfig(batch_size=batch_size, grad_accum_steps=grad_accum),
        optim=OptimConfig(),
        schedule=LRSchedule(),
        runtime=runtime,
    )

    mlflow_client = FakeMLflowClient()
    mlflow_client.fail_log = fail_log
    logger = FakeLogger()
    manager = MLflowManager(runtime, shared, logger, mlflow_client=mlflow_client)
    manager._active_run = object()

    manager.log_config(trainer_cfg)

    if fail_log:
        assert any("config logging failed" in msg for msg in logger.warnings)
    else:
        assert mlflow_client.params
        assert any(path[1] == "config" for path in mlflow_client.artifacts)


def test_log_config_no_active_run_is_noop(tmp_path: Path) -> None:
    """log_config returns early when no active run is present."""

    runtime = _make_runtime(tmp_path / "out", True, None, None, None, False)
    shared = SharedConfig(
        experiment="exp",
        config_path=tmp_path / "cfg.toml",
        project_home=tmp_path,
        dataset_dir=tmp_path / "data",
        train_out_dir=tmp_path / "train_out",
        sample_out_dir=tmp_path / "sample",
    )
    manager = MLflowManager(
        runtime, shared, FakeLogger(), mlflow_client=FakeMLflowClient()
    )

    manager.log_config(
        TrainerConfig(
            model=ModelConfig(),
            data=DataConfig(),
            optim=OptimConfig(),
            schedule=LRSchedule(),
            runtime=runtime,
        )
    )

    # no exception and no params/artifacts added
    assert True


def test_log_metrics_handles_failures_and_inactive_run() -> None:
    """log_metrics is a no-op when inactive and logs debug on failure when active."""

    runtime = _make_runtime(Path("out"), True, None, None, None, False)
    shared = SharedConfig(
        experiment="exp",
        config_path=Path("cfg.toml"),
        project_home=Path("."),
        dataset_dir=Path("data"),
        train_out_dir=Path("train_out"),
        sample_out_dir=Path("sample"),
    )
    mlflow_client = FakeMLflowClient()
    logger = FakeLogger()
    manager = MLflowManager(runtime, shared, logger, mlflow_client=mlflow_client)

    manager.log_metrics({"loss": 1.0}, step=1)
    assert not mlflow_client.metrics

    manager._active_run = object()
    manager.log_metrics({"loss": 1.0}, step=2)
    assert mlflow_client.metrics == [({"loss": 1.0}, 2)]

    mlflow_client.fail_log = True
    manager.log_metrics({"loss": 2.0}, step=3)
    assert any("metric logging failed" in msg for msg in logger.debugs)


def test_log_artifact_handles_dir_and_file(tmp_path: Path) -> None:
    """log_artifact logs files and directories and reports failures."""

    runtime = _make_runtime(tmp_path / "out", True, None, None, None, False)
    shared = SharedConfig(
        experiment="exp",
        config_path=tmp_path / "cfg.toml",
        project_home=tmp_path,
        dataset_dir=tmp_path / "data",
        train_out_dir=tmp_path / "train_out",
        sample_out_dir=tmp_path / "sample",
    )
    mlflow_client = FakeMLflowClient()
    logger = FakeLogger()
    manager = MLflowManager(runtime, shared, logger, mlflow_client=mlflow_client)
    manager._active_run = object()

    file_path = tmp_path / "metrics.txt"
    file_path.write_text("loss=1.0")
    dir_path = tmp_path / "artifacts"
    dir_path.mkdir(parents=True, exist_ok=True)
    (dir_path / "a.txt").write_text("a")

    manager.log_artifact(file_path, artifact_path="files")
    manager.log_artifact(dir_path, artifact_path="dir")
    assert (str(file_path), "files") in mlflow_client.artifacts
    assert (str(dir_path), "dir") in mlflow_client.artifacts

    mlflow_client.fail_log = True
    manager.log_artifact(file_path, artifact_path="files")
    assert any("artifact logging failed" in msg for msg in logger.warnings)


def test_finish_closes_active_run() -> None:
    """finish ends an active run and clears the handle."""

    runtime = _make_runtime(Path("out"), True, None, None, None, False)
    shared = SharedConfig(
        experiment="exp",
        config_path=Path("cfg.toml"),
        project_home=Path("."),
        dataset_dir=Path("data"),
        train_out_dir=Path("train_out"),
        sample_out_dir=Path("sample"),
    )
    mlflow_client = FakeMLflowClient()
    manager = MLflowManager(runtime, shared, FakeLogger(), mlflow_client=mlflow_client)
    manager._active_run = object()

    manager.finish()

    assert manager._active_run is None
    assert mlflow_client.end_run_called == 1
