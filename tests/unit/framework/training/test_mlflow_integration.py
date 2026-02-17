from __future__ import annotations

from pathlib import Path
from typing import Any, Optional, Iterator, Mapping, cast

from ml_playground.framework.configuration.models import (
    RuntimeConfig,
    TrainerConfig,
    ModelConfig,
    DataConfig,
    OptimConfig,
    LRSchedule,
)
from ml_playground.framework.training.mlflow_integration import MLflowManager

from tests.support.config_builders import create_metadata_config
from tests.unit.framework.training._helpers import LoggerStub


class _FakeMLflowRun:
    def __enter__(self) -> object:
        return self

    def __exit__(self, *exc: object) -> bool | None:
        return None

    def __iter__(self) -> Iterator[object]:
        return iter(())


class _FakeMLflowClient:
    def __init__(self) -> None:
        self.params: list[dict[str, Any]] = []
        self.metrics: list[tuple[dict[str, float], Optional[int]]] = []
        self.artifacts: list[tuple[str, Optional[str]]] = []
        self.tags: dict[str, object] = {}
        self.start_run_called = 0
        self.end_run_called = 0
        self.fail_note_tag = False

    def set_tracking_uri(self, _uri: str, /) -> None: ...

    def set_experiment(self, _experiment_name: str, /) -> object:
        return object()

    def get_experiment_by_name(self, _name: str, /) -> object:
        return None

    def create_experiment(self, _name: str, /, **kwargs: object) -> str:
        _ = kwargs
        return "id"

    def start_run(self, **kwargs: object) -> _FakeMLflowRun:
        self.start_run_called += 1
        return _FakeMLflowRun()

    def end_run(self) -> None:
        self.end_run_called += 1

    def log_params(self, params: Mapping[str, object], /) -> None:
        self.params.append(dict(params))

    def log_metrics(
        self, metrics: Mapping[str, float], /, *, step: Optional[int] = None
    ) -> None:
        self.metrics.append((dict(metrics), step))

    def log_artifact(
        self, local_path: str, /, *, artifact_path: Optional[str] = None
    ) -> None:
        self.artifacts.append((local_path, artifact_path))

    def log_artifacts(
        self, local_dir: str, /, *, artifact_path: Optional[str] = None
    ) -> None:
        self.artifacts.append((local_dir, artifact_path))

    def set_tag(self, key: str, value: object, /) -> None:
        if self.fail_note_tag and key == "mlflow.note.content":
            raise RuntimeError("tag failed")
        self.tags[key] = value


# Reuse LoggerStub which is strictly typed
_NullLogger = LoggerStub


def _runtime(tmp_path: Path, enabled: bool = False) -> RuntimeConfig:
    return RuntimeConfig(out_dir=tmp_path / "out", mlflow_enabled=enabled)


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
        create_metadata_config(tmp_path, experiment="exp", mkdir=False),
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
        create_metadata_config(tmp_path, experiment="exp", mkdir=False),
        _NullLogger(),
        mlflow_client=client,
    )
    mgr.setup()
    assert client.start_run_called == 0


def test_mlflow_manager_log_artifact_handles_file_and_dir(tmp_path: Path) -> None:
    client = _FakeMLflowClient()
    runtime = _runtime(tmp_path, enabled=True)
    shared = create_metadata_config(tmp_path, experiment="exp", mkdir=False)
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
    shared = create_metadata_config(tmp_path, experiment="exp", mkdir=False)
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
    shared = create_metadata_config(tmp_path, experiment="exp", mkdir=False)
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
    shared = create_metadata_config(tmp_path, experiment="exp", mkdir=False)

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
    shared = create_metadata_config(tmp_path, experiment="exp", mkdir=False)

    mgr = MLflowManager(runtime, shared, _NullLogger(), mlflow_client=client)
    mgr.setup()
    assert client.start_run_called == 1


def test_mlflow_manager_log_config_logs_config_artifact_when_present(
    tmp_path: Path,
) -> None:
    client = _FakeMLflowClient()
    runtime = _runtime(tmp_path, enabled=True)
    shared = create_metadata_config(tmp_path, experiment="exp", mkdir=False)
    shared.config_path.parent.mkdir(parents=True, exist_ok=True)
    shared.config_path.write_text("[training]\n", encoding="utf-8")
    logger = _NullLogger()

    mgr = MLflowManager(runtime, shared, logger, mlflow_client=client)
    mgr.setup()
    mgr.log_config(_trainer_cfg(tmp_path))

    assert any(str(shared.config_path) == art[0] for art in client.artifacts)


def test_mlflow_manager_log_metrics_success_path(tmp_path: Path) -> None:
    client = _FakeMLflowClient()
    runtime = _runtime(tmp_path, enabled=True)
    shared = create_metadata_config(tmp_path, experiment="exp", mkdir=False)
    logger = _NullLogger()

    mgr = MLflowManager(runtime, shared, logger, mlflow_client=client)
    mgr.setup()
    mgr.log_metrics({"loss": 1.0}, step=1)

    assert client.metrics == [({"loss": 1.0}, 1)]


def test_mlflow_manager_log_metrics_is_noop_when_inactive(tmp_path: Path) -> None:
    client = _FakeMLflowClient()
    runtime = _runtime(tmp_path, enabled=True)
    shared = create_metadata_config(tmp_path, experiment="exp", mkdir=False)

    mgr = MLflowManager(runtime, shared, _NullLogger(), mlflow_client=client)
    mgr.log_metrics({"loss": 1.0}, step=1)
    assert client.metrics == []


def test_mlflow_manager_finish_ends_run_when_active(tmp_path: Path) -> None:
    client = _FakeMLflowClient()
    runtime = _runtime(tmp_path, enabled=True)
    shared = create_metadata_config(tmp_path, experiment="exp", mkdir=False)

    mgr = MLflowManager(runtime, shared, _NullLogger(), mlflow_client=client)
    mgr.setup()
    mgr.finish()
    assert client.end_run_called == 1


def test_mlflow_manager_setup_uses_tracking_uri_and_existing_experiment(
    tmp_path: Path,
) -> None:
    class _Client(_FakeMLflowClient):
        def __init__(self) -> None:
            super().__init__()
            self.tracking_uri: str | None = None
            self.experiment_lookups: list[str] = []
            self.created: list[str] = []

        def set_tracking_uri(self, uri: str, /) -> None:
            self.tracking_uri = uri

        def get_experiment_by_name(self, name: str, /) -> object:
            self.experiment_lookups.append(name)
            return object()

    client = _Client()
    runtime = RuntimeConfig(
        out_dir=tmp_path / "out",
        mlflow_enabled=True,
        mlflow_tracking_uri="sqlite:///../out/mlflow.db",
        mlflow_experiment_name="exp-name",
    )
    shared = create_metadata_config(tmp_path, experiment="exp", mkdir=False)
    logger = _NullLogger()

    mgr = MLflowManager(runtime, shared, logger, mlflow_client=client)
    mgr.setup()

    assert client.tracking_uri is not None
    assert client.experiment_lookups == ["exp-name"]
    assert client.created == []
    assert client.start_run_called == 1
    mgr.finish()


def test_mlflow_manager_setup_failure_sets_active_run_none(tmp_path: Path) -> None:
    class _Client(_FakeMLflowClient):
        def set_tracking_uri(self, _uri: str, /) -> None:
            raise RuntimeError("boom")

    runtime = RuntimeConfig(
        out_dir=tmp_path / "out",
        mlflow_enabled=True,
        mlflow_tracking_uri="sqlite:///../out/mlflow.db",
    )
    shared = create_metadata_config(tmp_path, experiment="exp", mkdir=False)
    mgr = MLflowManager(runtime, shared, _NullLogger(), mlflow_client=_Client())
    mgr.setup()
    assert mgr._active_run is None  # pyright: ignore[reportPrivateUsage]


def test_mlflow_manager_log_metrics_failure_is_non_fatal(tmp_path: Path) -> None:
    class _Client(_FakeMLflowClient):
        def log_metrics(
            self, metrics: Mapping[str, float], /, *, step: Optional[int] = None
        ) -> None:
            raise RuntimeError("fail")

    mgr = MLflowManager(
        _runtime(tmp_path, enabled=True),
        create_metadata_config(tmp_path, experiment="exp", mkdir=False),
        _NullLogger(),
        mlflow_client=_Client(),
    )
    mgr._active_run = _FakeMLflowRun()  # pyright: ignore[reportPrivateUsage]
    mgr.log_metrics({"loss": 1.0}, step=1)


def test_mlflow_manager_log_artifact_failure_is_non_fatal(tmp_path: Path) -> None:
    class _Client(_FakeMLflowClient):
        def log_artifact(
            self, local_path: str, /, *, artifact_path: Optional[str] = None
        ) -> None:
            raise RuntimeError("fail")

        def log_artifacts(
            self, local_dir: str, /, *, artifact_path: Optional[str] = None
        ) -> None:
            raise RuntimeError("fail")

    mgr = MLflowManager(
        _runtime(tmp_path, enabled=True),
        create_metadata_config(tmp_path, experiment="exp", mkdir=False),
        _NullLogger(),
        mlflow_client=_Client(),
    )
    mgr._active_run = _FakeMLflowRun()  # pyright: ignore[reportPrivateUsage]
    mgr.log_artifact(tmp_path, artifact_path="art")


def test_mlflow_manager_finish_no_active_run(tmp_path: Path) -> None:
    mgr = MLflowManager(
        _runtime(tmp_path, enabled=True),
        create_metadata_config(tmp_path, experiment="exp", mkdir=False),
        _NullLogger(),
        mlflow_client=_FakeMLflowClient(),
    )
    mgr.finish()


def test_mlflow_manager_setup_fails_when_mlflow_is_none(tmp_path: Path) -> None:
    """MLflowManager.setup should handle when the mlflow module is None (not installed)."""

    class _NoneMlflow:
        def __getattr__(self, name: str) -> Any:
            raise AttributeError(f"'NoneType' object has no attribute '{name}'")

    logger = _NullLogger()
    mgr = MLflowManager(
        _runtime(tmp_path, enabled=True),
        create_metadata_config(tmp_path, experiment="exp", mkdir=False),
        logger,
        mlflow_client=cast(Any, _NoneMlflow()),  # Simulates mlflow module being None
    )

    mgr.setup()
    assert any("MLflow setup failed" in msg for msg in logger.warnings)
    assert mgr._active_run is None  # pyright: ignore[reportPrivateUsage]


def test_mlflow_manager_log_text_success(tmp_path: Path) -> None:
    class _Client(_FakeMLflowClient):
        def __init__(self) -> None:
            super().__init__()
            self.texts: list[tuple[str, str]] = []

        def log_text(self, text: str, artifact_file: str) -> None:
            self.texts.append((text, artifact_file))

    client = _Client()
    mgr = MLflowManager(
        _runtime(tmp_path, enabled=True),
        create_metadata_config(tmp_path, experiment="exp", mkdir=False),
        _NullLogger(),
        mlflow_client=client,
    )
    mgr._active_run = _FakeMLflowRun()  # pyright: ignore[reportPrivateUsage]
    mgr.log_text("hello", "dir/test.txt")
    assert client.texts == [("hello", "dir/test.txt")]


def test_mlflow_manager_log_text_fallback(tmp_path: Path) -> None:
    client = _FakeMLflowClient()
    mgr = MLflowManager(
        _runtime(tmp_path, enabled=True),
        create_metadata_config(tmp_path, experiment="exp", mkdir=False),
        _NullLogger(),
        mlflow_client=client,
    )
    mgr._active_run = _FakeMLflowRun()  # pyright: ignore[reportPrivateUsage]
    mgr.log_text("hello", "dir/test.txt")

    assert len(client.artifacts) == 1
    # It logs from a temp file to artifact_path
    local_path, art_path = client.artifacts[0]
    assert art_path == str(Path("dir/test.txt").parent)


def test_mlflow_manager_log_text_root_fallback(tmp_path: Path) -> None:
    client = _FakeMLflowClient()
    mgr = MLflowManager(
        _runtime(tmp_path, enabled=True),
        create_metadata_config(tmp_path, experiment="exp", mkdir=False),
        _NullLogger(),
        mlflow_client=client,
    )
    mgr._active_run = _FakeMLflowRun()  # pyright: ignore[reportPrivateUsage]
    mgr.log_text("hello", "test.txt")

    assert len(client.artifacts) == 1
    local_path, art_path = client.artifacts[0]
    assert art_path is None


def test_mlflow_manager_log_text_failure(tmp_path: Path) -> None:
    class _Client(_FakeMLflowClient):
        def log_text(self, text: str, artifact_file: str) -> None:
            raise RuntimeError("fail log text")

    logger = _NullLogger()
    mgr = MLflowManager(
        _runtime(tmp_path, enabled=True),
        create_metadata_config(tmp_path, experiment="exp", mkdir=False),
        logger,
        mlflow_client=_Client(),
    )
    mgr._active_run = _FakeMLflowRun()  # pyright: ignore[reportPrivateUsage]
    mgr.log_text("hello", "test.txt")
    assert any(
        "MLflow text logging failed: fail log text" in msg for msg in logger.warnings
    )


def test_mlflow_manager_log_text_inactive(tmp_path: Path) -> None:
    client = _FakeMLflowClient()
    mgr = MLflowManager(
        _runtime(tmp_path, enabled=True),
        create_metadata_config(tmp_path, experiment="exp", mkdir=False),
        _NullLogger(),
        mlflow_client=client,
    )
    mgr.log_text("hello", "test.txt")
    assert getattr(client, "texts", None) is None and not client.artifacts
