from __future__ import annotations

from contextlib import contextmanager
import importlib
from pathlib import Path
from types import SimpleNamespace

from ml_playground.configuration.models import (
    DataConfig,
    LRSchedule,
    ModelConfig,
    OptimConfig,
    PreparerConfig,
    RuntimeConfig,
    SampleConfig,
    SamplerConfig,
    SharedConfig,
    TrainerConfig,
)
from ml_playground.runtime.cli import runners
from ml_playground.runtime.core.results import ToolResult


def _mk_shared(tmp_path: Path) -> SharedConfig:
    dataset_dir = tmp_path / "dataset"
    train_dir = tmp_path / "train"
    sample_dir = tmp_path / "sample"
    dataset_dir.mkdir(exist_ok=True)
    train_dir.mkdir(exist_ok=True)
    sample_dir.mkdir(exist_ok=True)
    cfg_path = tmp_path / "config.toml"
    cfg_path.write_text("{}", encoding="utf-8")
    return SharedConfig(
        experiment="demo",
        config_path=cfg_path,
        project_home=tmp_path,
        dataset_dir=dataset_dir,
        train_out_dir=train_dir,
        sample_out_dir=sample_dir,
    )


def _mk_runtime(out_dir: Path) -> RuntimeConfig:
    return RuntimeConfig(device="cpu", dtype="float32", seed=0, out_dir=out_dir)


class _NullLogger:
    def debug(self, msg: str, *args: object, **kwargs: object) -> None: ...

    def info(self, msg: str, *args: object, **kwargs: object) -> None: ...

    def warning(self, msg: str, *args: object, **kwargs: object) -> None: ...

    def error(self, msg: str, *args: object, **kwargs: object) -> None: ...


@contextmanager
def _swap_cli_impls(
    cli_module: object,
    *,
    run_prepare_impl: object | None = None,
    run_train_impl: object | None = None,
    run_sample_impl: object | None = None,
    handle_tool_result: object | None = None,
):
    run_prepare_orig = getattr(cli_module, "run_prepare_impl", None)
    run_train_orig = getattr(cli_module, "run_train_impl", None)
    run_sample_orig = getattr(cli_module, "run_sample_impl", None)
    handle_orig = getattr(cli_module, "handle_tool_result", None)

    if run_prepare_impl is not None:
        setattr(cli_module, "run_prepare_impl", run_prepare_impl)
    if run_train_impl is not None:
        setattr(cli_module, "run_train_impl", run_train_impl)
    if run_sample_impl is not None:
        setattr(cli_module, "run_sample_impl", run_sample_impl)
    if handle_tool_result is not None:
        setattr(cli_module, "handle_tool_result", handle_tool_result)
    try:
        yield
    finally:
        if run_prepare_orig is not None:
            setattr(cli_module, "run_prepare_impl", run_prepare_orig)
        if run_train_orig is not None:
            setattr(cli_module, "run_train_impl", run_train_orig)
        if run_sample_orig is not None:
            setattr(cli_module, "run_sample_impl", run_sample_orig)
        if handle_orig is not None:
            setattr(cli_module, "handle_tool_result", handle_orig)


def test_run_prepare_handles_tool_result(tmp_path: Path) -> None:
    shared = _mk_shared(tmp_path)
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

    cli_pkg = importlib.import_module("ml_playground.runtime.cli")
    shim = SimpleNamespace(run_prepare_impl=fake_impl, handle_tool_result=fake_handle)

    with _swap_cli_impls(
        cli_pkg,
        run_prepare_impl=shim.run_prepare_impl,
        handle_tool_result=shim.handle_tool_result,
    ):
        result = runners.run_prepare("demo", prepare_cfg, shared.config_path, shared)

    assert result.success is True
    assert "impl" in calls and "handle" in calls


def test_run_train_handles_tool_result(tmp_path: Path) -> None:
    shared = _mk_shared(tmp_path)
    train_cfg = TrainerConfig(
        model=ModelConfig(),
        data=DataConfig(),
        optim=OptimConfig(),
        schedule=LRSchedule(),
        runtime=_mk_runtime(shared.train_out_dir),
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

    cli_pkg = importlib.import_module("ml_playground.runtime.cli")
    shim = SimpleNamespace(run_train_impl=fake_impl, handle_tool_result=fake_handle)

    with _swap_cli_impls(
        cli_pkg,
        run_train_impl=shim.run_train_impl,
        handle_tool_result=shim.handle_tool_result,
    ):
        result = runners.run_train("demo", train_cfg, shared.config_path, shared)

    assert result.success is True
    assert "impl" in calls and "handle" in calls


def test_run_sample_handles_tool_result(tmp_path: Path) -> None:
    shared = _mk_shared(tmp_path)
    sample_cfg = SamplerConfig(
        runtime=_mk_runtime(shared.sample_out_dir),
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

    cli_pkg = importlib.import_module("ml_playground.runtime.cli")
    shim = SimpleNamespace(run_sample_impl=fake_impl, handle_tool_result=fake_handle)

    with _swap_cli_impls(
        cli_pkg,
        run_sample_impl=shim.run_sample_impl,
        handle_tool_result=shim.handle_tool_result,
    ):
        result = runners.run_sample("demo", sample_cfg, shared.config_path, shared)

    assert result.success is True
    assert "impl" in calls and "handle" in calls
