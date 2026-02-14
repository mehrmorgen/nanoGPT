from __future__ import annotations

from contextlib import contextmanager
import sys
from pathlib import Path
from types import SimpleNamespace, TracebackType
from typing import Any, Mapping

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
from ml_playground.runtime_cli import runners
from ml_playground.framework.runtime.core.results import ToolResult
from tests.support.config_builders import create_metadata_config


def _mk_runtime(out_dir: Path) -> RuntimeConfig:
    return RuntimeConfig(device="cpu", dtype="float32", seed=0, out_dir=out_dir)


class _NullLogger:
    def debug(
        self,
        msg: object,
        *args: object,
        exc_info: _ExcInfoType = None,
        stack_info: bool = False,
        stacklevel: int = 1,
        extra: Mapping[str, object] | None = None,
    ) -> None:
        del msg, args, exc_info, stack_info, stacklevel, extra

    def info(
        self,
        msg: object,
        *args: object,
        exc_info: _ExcInfoType = None,
        stack_info: bool = False,
        stacklevel: int = 1,
        extra: Mapping[str, object] | None = None,
    ) -> None:
        del msg, args, exc_info, stack_info, stacklevel, extra

    def warning(
        self,
        msg: object,
        *args: object,
        exc_info: _ExcInfoType = None,
        stack_info: bool = False,
        stacklevel: int = 1,
        extra: Mapping[str, object] | None = None,
    ) -> None:
        del msg, args, exc_info, stack_info, stacklevel, extra

    def error(
        self,
        msg: object,
        *args: object,
        exc_info: _ExcInfoType = None,
        stack_info: bool = False,
        stacklevel: int = 1,
        extra: Mapping[str, object] | None = None,
    ) -> None:
        del msg, args, exc_info, stack_info, stacklevel, extra


_ExcInfoType = (
    bool
    | BaseException
    | tuple[type[BaseException], BaseException, TracebackType | None]
    | None
)


@contextmanager
def _override_mapping(mapping: dict, key: str, value: object) -> Any:
    """Temporarily set a key in a mapping, restoring on exit."""
    had_key = key in mapping
    original = mapping.get(key)
    mapping[key] = value
    try:
        yield
    finally:
        if had_key:
            mapping[key] = original
        else:
            mapping.pop(key, None)


def test_run_prepare_handles_tool_result(tmp_path: Path) -> None:
    metadata = create_metadata_config(tmp_path)
    prepare_cfg = PreparerConfig().model_copy(update={"logger": _NullLogger()})

    calls: list[str] = []

    def fake_impl(*_args: object, **_kwargs: object) -> ToolResult:
        calls.append("impl")
        return ToolResult.create(
            success=True,
            exit_code=0,
            namespace="ml",
            category="prepare",
            command="demo",
        )

    def fake_handle(result: ToolResult, learning_mode: bool) -> None:  # noqa: ARG001
        calls.append("handle")

    deps = runners.CLIDependencies(
        run_prepare=fake_impl,
        handle_tool_result=fake_handle,
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

    def fake_impl(*_args: object, **_kwargs: object) -> ToolResult:
        calls.append("impl")
        return ToolResult.create(
            success=True, exit_code=0, namespace="ml", category="train", command="demo"
        )

    def fake_handle(result: ToolResult, learning_mode: bool) -> None:  # noqa: ARG001
        calls.append("handle")

    deps = runners.CLIDependencies(
        run_train=fake_impl,
        handle_tool_result=fake_handle,
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

    def fake_impl(*_args: object, **_kwargs: object) -> ToolResult:
        calls.append("impl")
        return ToolResult.create(
            success=True, exit_code=0, namespace="ml", category="sample", command="demo"
        )

    def fake_handle(result: ToolResult, learning_mode: bool) -> None:  # noqa: ARG001
        calls.append("handle")

    deps = runners.CLIDependencies(
        run_sample=fake_impl,
        handle_tool_result=fake_handle,
    )

    result = runners.run_sample(
        "demo", sample_cfg, metadata.config_path, metadata, deps
    )

    assert result.success is True
    assert "impl" in calls and "handle" in calls


def test_runtime_cli_module_present_in_sys_modules() -> None:
    """Ensure runtime_cli resolves via sys.modules once imported."""
    dummy = SimpleNamespace(marker="existing")
    with _override_mapping(sys.modules, "ml_playground.runtime_cli", dummy):
        mod = sys.modules.get("ml_playground.runtime_cli")
    assert mod is dummy
