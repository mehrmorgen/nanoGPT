from __future__ import annotations

from pathlib import Path
from typing import Any, Optional

from ml_playground.configuration.models import (
    RuntimeConfig,
    SharedConfig,
    TrainerConfig,
    ModelConfig,
    DataConfig,
    OptimConfig,
    LRSchedule,
)
from ml_playground.training.mlflow_integration import MLflowManager


class _FakeMLflowClient:
    def __init__(self) -> None:
        self.params: list[dict[str, Any]] = []
        self.metrics: list[tuple[dict[str, float], Optional[int]]] = []
        self.artifacts: list[tuple[str, Optional[str]]] = []
        self.tags: dict[str, Any] = {}
        self.start_run_called = 0
        self.end_run_called = 0
        self.fail_note_tag = False

    def set_tracking_uri(self, _uri: str, /) -> None: ...

    def set_experiment(self, _experiment_name: str, /) -> Any: ...

    def create_experiment(
        self, _name: str, /, *, artifact_location: Optional[str] = None
    ) -> str:
        _ = artifact_location
        return "id"

    def start_run(self, **kwargs: Any) -> Any:
        self.start_run_called += 1
        return object()

    def end_run(self) -> None:
        self.end_run_called += 1

    def log_params(self, params: dict[str, Any], /) -> None:
        self.params.append(params)

    def log_metrics(
        self, metrics: dict[str, float], /, *, step: Optional[int] = None
    ) -> None:
        self.metrics.append((metrics, step))

    def log_artifact(
        self, local_path: str, /, *, artifact_path: Optional[str] = None
    ) -> None:
        self.artifacts.append((local_path, artifact_path))

    def log_artifacts(
        self, local_dir: str, /, *, artifact_path: Optional[str] = None
    ) -> None:
        self.artifacts.append((local_dir, artifact_path))

    def set_tag(self, key: str, value: Any, /) -> None:
        if self.fail_note_tag and key == "mlflow.note.content":
            raise RuntimeError("tag failed")
        self.tags[key] = value


class _NullLogger:
    def info(self, *args: Any, **kwargs: Any) -> None: ...
    def warning(self, *args: Any, **kwargs: Any) -> None: ...
    def debug(self, *args: Any, **kwargs: Any) -> None: ...


def _runtime(tmp_path: Path, enabled: bool = False) -> RuntimeConfig:
    return RuntimeConfig(out_dir=tmp_path / "out", mlflow_enabled=enabled)


def _shared(tmp_path: Path) -> SharedConfig:
    return SharedConfig(
        experiment="exp",
        config_path=tmp_path / "config.toml",
        project_home=tmp_path,
        dataset_dir=tmp_path / "data",
        train_out_dir=tmp_path / "train_out",
        sample_out_dir=tmp_path / "sample_out",
    )


def _trainer_cfg(tmp_path: Path) -> TrainerConfig:
    return TrainerConfig(
        model=ModelConfig(n_layer=1, n_head=1, n_embd=8, block_size=1),
        data=DataConfig(
            batch_size=1,
            block_size=1,
            tokenizer="char",
            ngram_size=1,
            grad_accum_steps=1,
        ),
        optim=OptimConfig(),
        schedule=LRSchedule(),
        runtime=RuntimeConfig(out_dir=tmp_path / "out", max_iters=1),
    )


def test_mlflow_manager_noop_when_disabled(tmp_path: Path) -> None:
    client = _FakeMLflowClient()
    mgr = MLflowManager(
        _runtime(tmp_path, enabled=False),
        _shared(tmp_path),
        _NullLogger(),
        mlflow_client=client,
    )

    mgr.setup()
    mgr.log_config(_trainer_cfg(tmp_path))
    mgr.log_metrics({"loss": 1.0}, step=1)
    mgr.log_artifact(tmp_path / "file.txt")
    mgr.finish()

    assert client.start_run_called == 0
    assert client.params == []
    assert client.metrics == []
    assert client.artifacts == []
    assert client.end_run_called == 0


def test_mlflow_manager_setup_noop_when_disabled(tmp_path: Path) -> None:
    client = _FakeMLflowClient()
    mgr = MLflowManager(
        _runtime(tmp_path, enabled=False),
        _shared(tmp_path),
        _NullLogger(),
        mlflow_client=client,
    )
    mgr.setup()
    assert client.start_run_called == 0


def test_mlflow_manager_log_artifact_handles_file_and_dir(tmp_path: Path) -> None:
    client = _FakeMLflowClient()
    runtime = _runtime(tmp_path, enabled=True)
    shared = _shared(tmp_path)
    logger = _NullLogger()
    mgr = MLflowManager(runtime, shared, logger, mlflow_client=client)
    mgr.setup()

    # file artifact
    file_path = tmp_path / "artifact.txt"
    file_path.write_text("data", encoding="utf-8")
    mgr.log_artifact(file_path)
    # dir artifact
    dir_path = tmp_path / "dir_artifact"
    dir_path.mkdir()
    (dir_path / "nested.txt").write_text("x", encoding="utf-8")
    mgr.log_artifact(dir_path, artifact_path="nested")
    mgr.finish()

    assert any(str(file_path) in a[0] for a in client.artifacts)
    assert any(str(dir_path) in a[0] for a in client.artifacts)


def test_mlflow_manager_log_config_skips_missing_config_path(tmp_path: Path) -> None:
    client = _FakeMLflowClient()
    runtime = _runtime(tmp_path, enabled=True)
    shared = _shared(tmp_path)
    logger = _NullLogger()

    mgr = MLflowManager(runtime, shared, logger, mlflow_client=client)
    mgr.setup()

    assert not shared.config_path.exists()
    mgr.log_config(_trainer_cfg(tmp_path))
    assert client.artifacts == []


def test_mlflow_manager_setup_skips_system_metrics_when_disabled(
    tmp_path: Path,
) -> None:
    client = _FakeMLflowClient()
    runtime = RuntimeConfig(
        out_dir=tmp_path / "out",
        mlflow_enabled=True,
        mlflow_log_system_metrics=False,
    )
    shared = _shared(tmp_path)
    logger = _NullLogger()

    mgr = MLflowManager(runtime, shared, logger, mlflow_client=client)
    mgr.setup()

    assert "mlflow.note.content" not in client.tags


def test_mlflow_manager_setup_sets_system_metrics_tag_when_enabled(
    tmp_path: Path,
) -> None:
    client = _FakeMLflowClient()
    runtime = RuntimeConfig(
        out_dir=tmp_path / "out",
        mlflow_enabled=True,
        mlflow_log_system_metrics=True,
    )
    shared = _shared(tmp_path)

    mgr = MLflowManager(runtime, shared, _NullLogger(), mlflow_client=client)
    mgr.setup()
    assert client.tags.get("mlflow.note.content") == "System metrics enabled"


def test_mlflow_manager_setup_system_metrics_tag_failure_is_non_fatal(
    tmp_path: Path,
) -> None:
    client = _FakeMLflowClient()
    client.fail_note_tag = True
    runtime = RuntimeConfig(
        out_dir=tmp_path / "out",
        mlflow_enabled=True,
        mlflow_log_system_metrics=True,
    )
    shared = _shared(tmp_path)

    mgr = MLflowManager(runtime, shared, _NullLogger(), mlflow_client=client)
    mgr.setup()
    assert client.start_run_called == 1


def test_mlflow_manager_log_config_logs_config_artifact_when_present(
    tmp_path: Path,
) -> None:
    client = _FakeMLflowClient()
    runtime = _runtime(tmp_path, enabled=True)
    shared = _shared(tmp_path)
    shared.config_path.parent.mkdir(parents=True, exist_ok=True)
    shared.config_path.write_text("[train]\n", encoding="utf-8")
    logger = _NullLogger()

    mgr = MLflowManager(runtime, shared, logger, mlflow_client=client)
    mgr.setup()
    mgr.log_config(_trainer_cfg(tmp_path))

    assert any(str(shared.config_path) == art[0] for art in client.artifacts)


def test_mlflow_manager_log_metrics_success_path(tmp_path: Path) -> None:
    client = _FakeMLflowClient()
    runtime = _runtime(tmp_path, enabled=True)
    shared = _shared(tmp_path)
    logger = _NullLogger()

    mgr = MLflowManager(runtime, shared, logger, mlflow_client=client)
    mgr.setup()
    mgr.log_metrics({"loss": 1.0}, step=1)

    assert client.metrics == [({"loss": 1.0}, 1)]


def test_mlflow_manager_log_metrics_is_noop_when_inactive(tmp_path: Path) -> None:
    client = _FakeMLflowClient()
    runtime = _runtime(tmp_path, enabled=True)
    shared = _shared(tmp_path)

    mgr = MLflowManager(runtime, shared, _NullLogger(), mlflow_client=client)
    mgr.log_metrics({"loss": 1.0}, step=1)
    assert client.metrics == []


def test_mlflow_manager_finish_ends_run_when_active(tmp_path: Path) -> None:
    client = _FakeMLflowClient()
    runtime = _runtime(tmp_path, enabled=True)
    shared = _shared(tmp_path)

    mgr = MLflowManager(runtime, shared, _NullLogger(), mlflow_client=client)
    mgr.setup()
    mgr.finish()
    assert client.end_run_called == 1
