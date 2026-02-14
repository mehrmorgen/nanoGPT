from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast

import pytest

from ml_playground.framework.configuration.models import (
    DataConfig,
    LRSchedule,
    ModelConfig,
    OptimConfig,
    PreparerConfig,
    RuntimeConfig,
    SampleConfig,
    SamplerConfig,
    TrainerConfig,
)
import ml_playground.runtime_cli.runners as runners
from ml_playground.runtime_cli.runners import CLIDependencies
from ml_playground.framework.runtime.core.results import ToolResult
from tests.support.config_builders import create_metadata_config


def _mk_runtime(out_dir: Path) -> RuntimeConfig:
    return RuntimeConfig(device="cpu", dtype="float32", seed=0, out_dir=out_dir)


class _NullLogger:
    def debug(self, msg: object, *args: object, **kwargs: object) -> None: ...

    def info(self, msg: object, *args: object, **kwargs: object) -> None: ...

    def warning(self, msg: object, *args: object, **kwargs: object) -> None: ...

    def error(self, msg: object, *args: object, **kwargs: object) -> None: ...


def test_run_prepare_handles_tool_result(tmp_path: Path) -> None:
    metadata = create_metadata_config(tmp_path)
    prepare_cfg = PreparerConfig().model_copy(update={"logger": _NullLogger()})

    calls: list[str] = []

    def fake_impl(*_args: Any, **_kwargs: Any) -> ToolResult:
        calls.append("impl")
        return ToolResult.create(
            success=True,
            exit_code=0,
            namespace="ml",
            category="prepare",
            command="demo",
        )

    def fake_handle(result: Any, learning_mode: bool) -> None:  # noqa: ARG001
        calls.append("handle")

    deps = runners.create_default_cli_dependencies()
    # Replace the specific ones we want to trace
    deps = CLIDependencies(
        **{  # type: ignore
            **deps.__dict__,
            "run_prepare": fake_impl,
            "handle_tool_result": fake_handle,
        }
    )

    result = runners.run_prepare(
        "demo", prepare_cfg, metadata.config_path, metadata, deps
    )

    assert result.success is True
    assert "impl" in calls and "handle" in calls


def test_run_train_handles_tool_result(tmp_path: Path) -> None:
    metadata = create_metadata_config(tmp_path)
    train_cfg = TrainerConfig(
        model=ModelConfig(),
        data=DataConfig(),
        optim=OptimConfig(),
        schedule=LRSchedule(),
        runtime=_mk_runtime(metadata.train_out_dir),
        logger=_NullLogger(),
    )
    calls: list[str] = []

    def fake_impl(*_args: Any, **_kwargs: Any) -> ToolResult:
        calls.append("impl")
        return ToolResult.create(
            success=True, exit_code=0, namespace="ml", category="train", command="demo"
        )

    def fake_handle(result: Any, learning_mode: bool) -> None:  # noqa: ARG001
        calls.append("handle")

    deps = runners.create_default_cli_dependencies()
    deps = CLIDependencies(
        **{  # type: ignore
            **deps.__dict__,
            "run_train": fake_impl,
            "handle_tool_result": fake_handle,
        }
    )

    result = runners.run_train("demo", train_cfg, metadata.config_path, metadata, deps)

    assert result.success is True
    assert "impl" in calls and "handle" in calls


def test_run_sample_handles_tool_result(tmp_path: Path) -> None:
    metadata = create_metadata_config(tmp_path)
    sample_cfg = SamplerConfig(
        runtime=_mk_runtime(metadata.sample_out_dir),
        sample=SampleConfig(),
        logger=_NullLogger(),
    )
    calls: list[str] = []

    def fake_impl(*_args: Any, **_kwargs: Any) -> ToolResult:
        calls.append("impl")
        return ToolResult.create(
            success=True, exit_code=0, namespace="ml", category="sample", command="demo"
        )

    def fake_handle(result: Any, learning_mode: bool) -> None:  # noqa: ARG001
        calls.append("handle")

    deps = runners.create_default_cli_dependencies()
    deps = CLIDependencies(
        **{  # type: ignore
            **deps.__dict__,
            "run_sample": fake_impl,
            "handle_tool_result": fake_handle,
        }
    )

    result = runners.run_sample(
        "demo", sample_cfg, metadata.config_path, metadata, deps
    )

    assert result.success is True
    assert "impl" in calls and "handle" in calls


def test_run_train_cmd_missing_training_raises(tmp_path: Path) -> None:
    metadata = create_metadata_config(tmp_path)
    exp = SimpleNamespace(training=None, metadata=metadata)
    deps = CLIDependencies(load_experiment=lambda *_: exp)

    with pytest.raises(RuntimeError, match="training config is required for training"):
        runners.run_train_cmd("demo", None, deps)


def test_run_prepare_cmd_missing_prepare_raises(tmp_path: Path) -> None:
    metadata = create_metadata_config(tmp_path)
    exp = SimpleNamespace(prepare=None, metadata=metadata)
    deps = CLIDependencies(load_experiment=lambda *_: exp)

    with pytest.raises(
        RuntimeError, match="prepare config is required for preparation"
    ):
        runners.run_prepare_cmd("demo", None, deps)


def test_run_sample_cmd_missing_sampling_raises(tmp_path: Path) -> None:
    metadata = create_metadata_config(tmp_path)
    exp = SimpleNamespace(sampling=None, metadata=metadata)
    deps = CLIDependencies(load_experiment=lambda *_: exp)

    with pytest.raises(RuntimeError, match="sampling config is required for sampling"):
        runners.run_sample_cmd("demo", None, deps)


def test_run_train_cmd_missing_metadata_config_path_attr_raises() -> None:
    exp = SimpleNamespace(
        training=SimpleNamespace(runtime=SimpleNamespace()),
        metadata=SimpleNamespace(train_out_dir=Path("/tmp")),
    )
    deps = CLIDependencies(load_experiment=lambda *_: exp)

    with pytest.raises(
        RuntimeError, match="metadata.config_path is required for training"
    ):
        runners.run_train_cmd("demo", None, deps)


def test_run_prepare_cmd_missing_metadata_config_path_attr_raises() -> None:
    exp = SimpleNamespace(
        prepare=SimpleNamespace(),
        metadata=SimpleNamespace(),
    )
    deps = CLIDependencies(load_experiment=lambda *_: exp)

    with pytest.raises(
        RuntimeError, match="metadata.config_path is required for preparation"
    ):
        runners.run_prepare_cmd("demo", None, deps)


def test_run_sample_cmd_missing_metadata_config_path_attr_raises() -> None:
    exp = SimpleNamespace(
        sampling=SimpleNamespace(runtime=SimpleNamespace()),
        metadata=SimpleNamespace(train_out_dir=Path("/tmp")),
    )
    deps = CLIDependencies(load_experiment=lambda *_: exp)

    with pytest.raises(
        RuntimeError, match="metadata.config_path is required for sampling"
    ):
        runners.run_sample_cmd("demo", None, deps)


def test_run_train_cmd_noncallable_ensure_branch(tmp_path: Path) -> None:
    metadata = create_metadata_config(tmp_path)
    exp = SimpleNamespace(
        training=SimpleNamespace(runtime=SimpleNamespace()),
        metadata=metadata,
    )
    calls: list[str] = []
    deps = CLIDependencies(
        load_experiment=lambda *_: exp,
        ensure_train_prerequisites=cast(Any, object()),
        run_train=lambda *_a, **_k: ToolResult.create(
            success=True, exit_code=0, namespace="ml", category="train", command="demo"
        ),
        handle_tool_result=lambda *_a, **_k: calls.append("handled"),
    )

    runners.run_train_cmd("demo", None, deps)
    assert calls == ["handled"]


def test_run_sample_cmd_noncallable_ensure_branch(tmp_path: Path) -> None:
    metadata = create_metadata_config(tmp_path)
    exp = SimpleNamespace(
        sampling=SimpleNamespace(runtime=SimpleNamespace()),
        metadata=metadata,
    )
    calls: list[str] = []
    deps = CLIDependencies(
        load_experiment=lambda *_: exp,
        ensure_sample_prerequisites=cast(Any, object()),
        run_sample=lambda *_a, **_k: ToolResult.create(
            success=True, exit_code=0, namespace="ml", category="sample", command="demo"
        ),
        handle_tool_result=lambda *_a, **_k: calls.append("handled"),
    )

    runners.run_sample_cmd("demo", None, deps)
    assert calls == ["handled"]


def test_normalize_cli_path_darwin_non_private_path() -> None:
    original_platform = runners.sys.platform
    runners.sys.platform = "darwin"
    try:
        path = Path("/tmp/not-private")
        assert runners._normalize_cli_path(path) == path
    finally:
        runners.sys.platform = original_platform
