from __future__ import annotations

import logging
from collections.abc import Callable
from contextlib import ExitStack
from pathlib import Path
from types import SimpleNamespace
from typing import Any, ContextManager, cast

from hypothesis import given, settings
import hypothesis.strategies as st
import pytest
import typer
from typer.testing import CliRunner

import ml_playground.runtime.cli as cli
from ml_playground.configuration.models import (
    DataConfig,
    ExperimentConfig,
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
from ml_playground.core.logging_protocol import LoggerLike
from ml_playground.runtime.cli import (
    CLIDependencies,
    log_command_status,
    log_directory,
    override_cli_dependencies,
    run_prepare,
    run_prepare_impl,
    run_sample,
    run_sample_impl,
    run_train,
    run_train_impl,
)
from ml_playground.runtime.core.results import ToolResult

CLI_RUNNER = CliRunner()


class _BufferLogger(LoggerLike):
    """Test logger conforming to LoggerLike."""

    def __init__(self) -> None:
        self.messages: list[str] = []

    def debug(
        self, msg: str, *args: object, **kwargs: object
    ) -> None:  # pragma: no cover - unused
        self.messages.append(str(msg))

    def info(self, msg: str, *args: object, **kwargs: object) -> None:
        self.messages.append(str(msg))

    def warning(
        self, msg: str, *args: object, **kwargs: object
    ) -> None:  # pragma: no cover - unused
        self.messages.append(str(msg))

    def error(
        self, msg: str, *args: object, **kwargs: object
    ) -> None:  # pragma: no cover - unused
        self.messages.append(str(msg))


@given(exit_code=st.integers(min_value=1, max_value=16))
@settings(max_examples=5, deadline=None, derandomize=True)
def test_run_or_exit_maps_known_exceptions(exit_code: int) -> None:
    def _raise() -> None:
        raise FileNotFoundError("missing")

    with pytest.raises(typer.Exit) as excinfo:
        cli.run_or_exit(_raise, exception_exit_code=exit_code)

    assert excinfo.value.exit_code == exit_code


def test_run_or_exit_keyboard_interrupt_logs(caplog: pytest.LogCaptureFixture) -> None:
    with caplog.at_level(logging.INFO, logger="ml_playground.runtime.cli"):

        def _raise_keyboard_interrupt() -> None:
            raise KeyboardInterrupt

        result = cli.run_or_exit(
            _raise_keyboard_interrupt, keyboard_interrupt_msg="Interrupted"
        )

    assert result is None
    assert "Interrupted" in caplog.text


def test_extract_exp_config_handles_context() -> None:
    ctx = cast(cli.typer.Context, SimpleNamespace(obj=None))
    assert cli.extract_exp_config(ctx) is None
    ctx.obj = {"exp_config": Path("/tmp/example.toml")}
    assert cli.extract_exp_config(ctx) == Path("/tmp/example.toml")


def test_log_directory_variants(tmp_path: Path) -> None:
    logger = _BufferLogger()
    log_directory("tag", "unset", None, logger)
    missing = tmp_path / "missing"
    log_directory("tag", "missing", missing, logger)
    existing = tmp_path / "exists"
    existing.mkdir()
    (existing / "file.txt").write_text("data", encoding="utf-8")
    log_directory("tag", "existing", existing, logger)
    assert any("<not set>" in msg for msg in logger.messages)
    assert any("missing" in msg for msg in logger.messages)
    assert any("Contents" in msg for msg in logger.messages)
    # Non-Path inputs should be ignored silently
    logger.messages.clear()
    log_directory("tag", "not_path", cast(Any, "/tmp/example"), logger)
    assert logger.messages == []


def test_log_command_status_handles_missing_path(
    tmp_path: Path, shared_config_factory: Callable[[Path], SharedConfig]
) -> None:
    logger = _BufferLogger()
    shared = shared_config_factory(tmp_path)
    log_command_status("tag", shared, tmp_path / "missing", logger)
    assert any("missing" in message for message in logger.messages)


def test_log_command_status_swallows_errors(
    tmp_path: Path,
    shared_config_factory: Callable[[Path], SharedConfig],
    override_attr: Callable[[object, str, object], ContextManager[None]],
) -> None:
    logger = _BufferLogger()
    shared = shared_config_factory(tmp_path)

    def boom(*_args: object, **_kwargs: object) -> None:
        raise OSError("boom")

    with override_attr(cli, "log_directory", boom):
        log_command_status("tag", shared, shared.dataset_dir, logger)

    assert logger.messages == []


def test_run_prepare_impl_executes_pipeline(
    tmp_path: Path,
    shared_config_factory: Callable[[Path], SharedConfig],
    override_attr: Callable[[object, str, object], ContextManager[None]],
) -> None:
    shared = shared_config_factory(tmp_path)
    calls: dict[str, Any] = {}

    class FakePipeline:
        def __init__(self) -> None:
            calls["init"] = True

        def run(self) -> None:
            calls["ran"] = True

    prepare_cfg = PreparerConfig(
        tokenizer_type="char", raw_text_path=shared.dataset_dir / "raw.txt"
    ).model_copy(update={"logger": logging.getLogger("ml_playground.runtime.cli")})
    (shared.dataset_dir / "raw.txt").write_text("data", encoding="utf-8")

    def _create_pipeline(_: PreparerConfig, __: SharedConfig) -> FakePipeline:
        return FakePipeline()

    with override_attr(cli, "create_pipeline", _create_pipeline):
        result = run_prepare_impl("demo", prepare_cfg, shared.config_path, shared, None)

    assert result.success is True
    assert calls == {"init": True, "ran": True}


def test_run_train_impl_requires_runtime(
    tmp_path: Path, shared_config_factory: Callable[[Path], SharedConfig]
) -> None:
    shared = shared_config_factory(tmp_path)
    cfg = cast(
        TrainerConfig,
        SimpleNamespace(
            runtime=None, logger=logging.getLogger("ml_playground.runtime.cli")
        ),
    )

    result = run_train_impl("demo", cfg, shared.config_path, shared, None)
    assert result.success is False
    assert "Runtime configuration is missing" in (result.stderr or "")


def test_run_sample_impl_requires_runtime(
    tmp_path: Path, shared_config_factory: Callable[[Path], SharedConfig]
) -> None:
    shared = shared_config_factory(tmp_path)
    cfg = cast(
        SamplerConfig,
        SimpleNamespace(
            runtime=None, logger=logging.getLogger("ml_playground.runtime.cli")
        ),
    )

    result = run_sample_impl("demo", cfg, shared.config_path, shared, None)
    assert result.success is False
    assert "Runtime configuration is missing" in (result.stderr or "")


def test_run_prepare_executes_pipeline(
    tmp_path: Path,
    shared_config_factory: Callable[[Path], SharedConfig],
    override_attr: Callable[[object, str, object], ContextManager[None]],
) -> None:
    shared = shared_config_factory(tmp_path)
    calls: dict[str, Any] = {}

    class FakePipeline:
        def __init__(self) -> None:
            calls["init"] = True

        def run(self) -> None:
            calls["ran"] = True

    prepare_cfg = cast(
        PreparerConfig,
        SimpleNamespace(logger=logging.getLogger("ml_playground.runtime.cli")),
    )

    def _create_pipeline(_: PreparerConfig, __: SharedConfig) -> FakePipeline:
        return FakePipeline()

    with override_attr(cli, "create_pipeline", _create_pipeline):
        run_prepare("demo", prepare_cfg, shared.config_path, shared)

    assert calls == {"init": True, "ran": True}


def test_run_train_and_sample_wrappers(
    tmp_path: Path,
    shared_config_factory: Callable[[Path], SharedConfig],
    override_attr: Callable[[object, str, object], ContextManager[None]],
) -> None:
    shared = shared_config_factory(tmp_path)
    log_calls: list[tuple[str, Path]] = []
    trainer_called: dict[str, Any] = {}
    sampler_called: dict[str, Any] = {}

    class FakeTrainer:
        def __init__(self, cfg: object, shared_cfg: object) -> None:
            trainer_called["cfg"] = cfg
            trainer_called["shared"] = shared_cfg

        def run(self) -> None:
            trainer_called["ran"] = True

    class FakeSampler:
        def __init__(self, cfg: object, shared_cfg: object) -> None:
            sampler_called["cfg"] = cfg
            sampler_called["shared"] = shared_cfg

        def run(self) -> None:
            sampler_called["ran"] = True

    runtime_cfg = RuntimeConfig(
        device="cpu", dtype="float32", seed=11, out_dir=shared.train_out_dir
    )
    train_cfg = TrainerConfig(
        model=ModelConfig(),
        data=DataConfig(),
        optim=OptimConfig(),
        schedule=LRSchedule(),
        runtime=runtime_cfg,
    ).model_copy(update={"logger": logging.getLogger("ml_playground.runtime.cli")})
    sample_cfg = SamplerConfig(
        runtime=runtime_cfg,
        sample=SampleConfig(),
    ).model_copy(update={"logger": logging.getLogger("ml_playground.runtime.cli")})

    def _load_experiment(name: str, exp_config: Path | None) -> ExperimentConfig:
        assert name == "demo"
        assert exp_config is None
        return cast(
            ExperimentConfig,
            SimpleNamespace(
                prepare=PreparerConfig(),
                train=train_cfg,
                sample=sample_cfg,
                shared=shared,
            ),
        )

    def _ensure_train(_: ExperimentConfig) -> None:  # noqa: ARG001
        log_calls.append(("ensure_train", shared.train_out_dir))

    def _ensure_sample(_: ExperimentConfig) -> None:  # noqa: ARG001
        log_calls.append(("ensure_sample", shared.sample_out_dir))

    def _noop_prepare(
        experiment: str,
        prepare_cfg: object,
        config_path: Path,
        shared_cfg: object,
        learning_mode_engine: object | None,
    ) -> ToolResult:
        return ToolResult.create(
            success=True,
            exit_code=0,
            namespace="ml",
            category="prepare",
            command=experiment,
        )

    def _noop_train(
        experiment: str,
        train_cfg: object,
        config_path: Path,
        shared_cfg: object,
        learning_mode_engine: object | None,
    ) -> ToolResult:
        return ToolResult.create(
            success=True,
            exit_code=0,
            namespace="ml",
            category="train",
            command=experiment,
        )

    def _noop_sample(
        experiment: str,
        sample_cfg: object,
        config_path: Path,
        shared_cfg: object,
        learning_mode_engine: object | None,
    ) -> ToolResult:
        return ToolResult.create(
            success=True,
            exit_code=0,
            namespace="ml",
            category="sample",
            command=experiment,
        )

    deps = CLIDependencies(
        load_experiment=_load_experiment,
        ensure_train_prerequisites=_ensure_train,
        ensure_sample_prerequisites=_ensure_sample,
        run_prepare=_noop_prepare,
        run_train=_noop_train,
        run_sample=_noop_sample,
    )

    def _log_command_status(tag: str, _s: object, d: Path, _l: object) -> None:
        log_calls.append((tag, d))

    def _device_setup(
        device: str,
        dtype: str,
        seed: int,
        *,
        cuda_is_available: object | None = None,
        torch_module: object | None = None,
    ) -> None:
        del device, dtype, seed, cuda_is_available, torch_module

    with ExitStack() as stack:
        stack.enter_context(override_cli_dependencies(deps))
        stack.enter_context(override_attr(cli, "CoreTrainer", FakeTrainer))
        stack.enter_context(override_attr(cli, "Sampler", FakeSampler))
        stack.enter_context(
            override_attr(cli, "log_command_status", _log_command_status)
        )
        stack.enter_context(override_attr(cli, "global_device_setup", _device_setup))
        run_train("demo", train_cfg, shared.config_path, shared)
        run_sample("demo", sample_cfg, shared.config_path, shared)

    assert trainer_called.get("ran") is True
    assert sampler_called.get("ran") is True
    assert ("pre-train", shared.train_out_dir) in log_calls
    assert ("post-train", shared.train_out_dir) in log_calls
    assert ("pre-sample", shared.sample_out_dir) in log_calls
    assert ("post-sample", shared.sample_out_dir) in log_calls
