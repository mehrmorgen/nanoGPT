from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace
from typing import Any

from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

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


def _make_shared(tmp_path: Path) -> SharedConfig:
    cfg_path = tmp_path / "config.toml"
    cfg_path.write_text("{}", encoding="utf-8")
    return SharedConfig(
        experiment="demo",
        config_path=cfg_path,
        project_home=tmp_path,
        dataset_dir=tmp_path / "dataset",
        train_out_dir=tmp_path / "train",
        sample_out_dir=tmp_path / "sample",
    )


class _LoggerStub:
    def debug(self, msg: str, *args: object, **kwargs: object) -> None: ...

    def info(self, msg: str, *args: object, **kwargs: object) -> None: ...

    def warning(self, msg: str, *args: object, **kwargs: object) -> None: ...

    def error(self, msg: str, *args: object, **kwargs: object) -> None: ...


@contextmanager
def _swap_cli_impls(
    *,
    run_prepare_impl: object | None = None,
    run_train_impl: object | None = None,
    run_sample_impl: object | None = None,
    handle_tool_result: object | None = None,
):
    import importlib

    cli_pkg = importlib.import_module("ml_playground.runtime.cli")

    run_prepare_orig = getattr(cli_pkg, "run_prepare_impl", None)
    run_train_orig = getattr(cli_pkg, "run_train_impl", None)
    run_sample_orig = getattr(cli_pkg, "run_sample_impl", None)
    handle_orig = getattr(cli_pkg, "handle_tool_result", None)

    if run_prepare_impl is not None:
        setattr(cli_pkg, "run_prepare_impl", run_prepare_impl)
    if run_train_impl is not None:
        setattr(cli_pkg, "run_train_impl", run_train_impl)
    if run_sample_impl is not None:
        setattr(cli_pkg, "run_sample_impl", run_sample_impl)
    if handle_tool_result is not None:
        setattr(cli_pkg, "handle_tool_result", handle_tool_result)
    try:
        yield
    finally:
        if run_prepare_orig is not None:
            setattr(cli_pkg, "run_prepare_impl", run_prepare_orig)
        if run_train_orig is not None:
            setattr(cli_pkg, "run_train_impl", run_train_orig)
        if run_sample_orig is not None:
            setattr(cli_pkg, "run_sample_impl", run_sample_orig)
        if handle_orig is not None:
            setattr(cli_pkg, "handle_tool_result", handle_orig)


def _tool_result(category: str) -> ToolResult:
    return ToolResult.create(
        success=True,
        exit_code=0,
        namespace="ml",
        category=category,
        command="demo",
    )


@given(learning_mode=st.booleans())
@settings(
    max_examples=5,
    deadline=None,
    derandomize=True,
    suppress_health_check=[HealthCheck.function_scoped_fixture],
)
def test_run_prepare_cmd_invokes_dependencies(
    tmp_path: Path, learning_mode: bool
) -> None:
    shared = _make_shared(tmp_path)
    calls: dict[str, Any] = {"load": [], "ensure": [], "run": [], "handle": []}

    def fake_load(exp: str, cfg_path: Path | None) -> Any:
        calls["load"].append((exp, cfg_path))
        return SimpleNamespace(
            prepare=PreparerConfig(logger=_LoggerStub()), shared=shared
        )

    def fake_run(*_args: object, **_kwargs: object) -> ToolResult:
        calls["run"].append(True)
        return _tool_result("prepare")

    def fake_handle(result: ToolResult, lm: bool) -> None:
        calls["handle"].append((result.operation_id.category, lm))

    deps = runners.CLIDependencies(
        load_experiment=fake_load,
        ensure_train_prerequisites=lambda _: calls["ensure"].append("train"),  # noqa: ARG005
        ensure_sample_prerequisites=lambda _: calls["ensure"].append("sample"),  # noqa: ARG005
        run_prepare=fake_run,
        run_train=fake_run,
        run_sample=fake_run,
    )

    with (
        runners.override_cli_dependencies(deps),
        _swap_cli_impls(handle_tool_result=fake_handle),
    ):
        runners.run_prepare_cmd("demo", shared.config_path, learning_mode=learning_mode)

    assert calls["load"] == [("demo", shared.config_path)]
    assert calls["run"] == [True]
    assert calls["handle"] == [("prepare", learning_mode)]
    assert calls["ensure"] == []


@given(learning_mode=st.booleans())
@settings(
    max_examples=5,
    deadline=None,
    derandomize=True,
    suppress_health_check=[HealthCheck.function_scoped_fixture],
)
def test_run_train_cmd_invokes_prereqs_and_handle(
    tmp_path: Path, learning_mode: bool
) -> None:
    shared = _make_shared(tmp_path)
    calls: dict[str, Any] = {"load": [], "ensure": [], "run": [], "handle": []}

    def fake_load(exp: str, cfg_path: Path | None) -> Any:
        calls["load"].append((exp, cfg_path))
        trainer_cfg = TrainerConfig(
            model=ModelConfig(),
            data=DataConfig(),
            optim=OptimConfig(),
            schedule=LRSchedule(),
            runtime=RuntimeConfig(
                device="cpu", dtype="float32", seed=0, out_dir=shared.train_out_dir
            ),
            logger=_LoggerStub(),
        )
        return SimpleNamespace(train=trainer_cfg, shared=shared)

    def fake_run(*_args: object, **_kwargs: object) -> ToolResult:
        calls["run"].append(True)
        return _tool_result("train")

    def fake_handle(result: ToolResult, lm: bool) -> None:
        calls["handle"].append((result.operation_id.category, lm))

    deps = runners.CLIDependencies(
        load_experiment=fake_load,
        ensure_train_prerequisites=lambda _: calls["ensure"].append("train"),
        ensure_sample_prerequisites=lambda _: calls["ensure"].append("sample"),
        run_prepare=fake_run,
        run_train=fake_run,
        run_sample=fake_run,
    )

    with (
        runners.override_cli_dependencies(deps),
        _swap_cli_impls(handle_tool_result=fake_handle),
    ):
        runners.run_train_cmd("demo", shared.config_path, learning_mode=learning_mode)

    assert calls["load"] == [("demo", shared.config_path)]
    assert calls["ensure"] == ["train"]
    assert calls["run"] == [True]
    assert calls["handle"] == [("train", learning_mode)]


@given(learning_mode=st.booleans())
@settings(
    max_examples=5,
    deadline=None,
    derandomize=True,
    suppress_health_check=[HealthCheck.function_scoped_fixture],
)
def test_run_sample_cmd_invokes_prereqs_and_handle(
    tmp_path: Path, learning_mode: bool
) -> None:
    shared = _make_shared(tmp_path)
    calls: dict[str, Any] = {"load": [], "ensure": [], "run": [], "handle": []}

    def fake_load(exp: str, cfg_path: Path | None) -> Any:
        calls["load"].append((exp, cfg_path))
        sampler_cfg = SamplerConfig(
            runtime=RuntimeConfig(
                device="cpu", dtype="float32", seed=0, out_dir=shared.sample_out_dir
            ),
            sample=SampleConfig(),
            logger=_LoggerStub(),
        )
        return SimpleNamespace(sample=sampler_cfg, shared=shared)

    def fake_run(*_args: object, **_kwargs: object) -> ToolResult:
        calls["run"].append(True)
        return _tool_result("sample")

    def fake_handle(result: ToolResult, lm: bool) -> None:
        calls["handle"].append((result.operation_id.category, lm))

    deps = runners.CLIDependencies(
        load_experiment=fake_load,
        ensure_train_prerequisites=lambda _: calls["ensure"].append("train"),
        ensure_sample_prerequisites=lambda _: calls["ensure"].append("sample"),
        run_prepare=fake_run,
        run_train=fake_run,
        run_sample=fake_run,
    )

    with (
        runners.override_cli_dependencies(deps),
        _swap_cli_impls(handle_tool_result=fake_handle),
    ):
        runners.run_sample_cmd("demo", shared.config_path, learning_mode=learning_mode)

    assert calls["load"] == [("demo", shared.config_path)]
    assert calls["ensure"] == ["sample"]
    assert calls["run"] == [True]
    assert calls["handle"] == [("sample", learning_mode)]


@given(learning_mode=st.booleans())
@settings(
    max_examples=5,
    deadline=None,
    derandomize=True,
    suppress_health_check=[HealthCheck.function_scoped_fixture],
)
def test_run_wrappers_delegate_to_cli_package(
    tmp_path: Path, learning_mode: bool
) -> None:
    calls: list[str] = []
    shared = _make_shared(tmp_path)

    def fake_impl(*_args: object, **_kwargs: object) -> ToolResult:
        calls.append("impl")
        return _tool_result("mixed")

    def fake_handle(result: ToolResult, lm: bool) -> None:
        calls.append(f"handle:{lm}")
        assert lm is learning_mode

    logger = _LoggerStub()
    prepare_cfg = PreparerConfig(logger=logger)
    trainer_cfg = TrainerConfig(
        model=ModelConfig(),
        data=DataConfig(),
        optim=OptimConfig(),
        schedule=LRSchedule(),
        runtime=RuntimeConfig(
            device="cpu", dtype="float32", seed=0, out_dir=shared.train_out_dir
        ),
        logger=logger,
    )
    sampler_cfg = SamplerConfig(
        runtime=RuntimeConfig(
            device="cpu", dtype="float32", seed=0, out_dir=shared.sample_out_dir
        ),
        sample=SampleConfig(),
        logger=logger,
    )

    with _swap_cli_impls(
        run_prepare_impl=fake_impl,
        run_train_impl=fake_impl,
        run_sample_impl=fake_impl,
        handle_tool_result=fake_handle,
    ):
        runners.run_prepare(
            "exp",
            prepare_cfg,
            shared.config_path,
            shared,
            learning_mode_engine=None,
            learning_mode=learning_mode,
        )
        runners.run_train(
            "exp",
            trainer_cfg,
            shared.config_path,
            shared,
            learning_mode_engine=None,
            learning_mode=learning_mode,
        )
        runners.run_sample(
            "exp",
            sampler_cfg,
            shared.config_path,
            shared,
            learning_mode_engine=None,
            learning_mode=learning_mode,
        )

    assert calls.count("impl") == 3
    assert calls.count(f"handle:{learning_mode}") == 3
