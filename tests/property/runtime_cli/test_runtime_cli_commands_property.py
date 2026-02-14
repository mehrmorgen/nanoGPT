from __future__ import annotations

import logging
from collections.abc import Callable
from contextlib import ExitStack
from pathlib import Path
from types import SimpleNamespace
from typing import Any, ContextManager, Mapping, cast

from hypothesis import given, settings
import hypothesis.strategies as st
import pytest
import typer
from typer.testing import CliRunner

import ml_playground.runtime_cli.commands as cli_commands
import ml_playground.runtime_cli.device as cli_device
import ml_playground.runtime_cli.main as cli_main
import ml_playground.runtime_cli.typer_helpers as cli_helpers
from ml_playground.framework.configuration.models import (
    DataConfig,
    ExperimentConfig,
    LRSchedule,
    ModelConfig,
    OptimConfig,
    PreparerConfig,
    RuntimeConfig,
    SampleConfig,
    SamplerConfig,
    MetadataConfig,
    TrainerConfig,
)
from ml_playground.framework.core.logging_protocol import LoggerLike
from ml_playground.runtime_cli.runners import CLIDependencies
from ml_playground.framework.runtime.core.results import ToolResult
from ml_playground.framework.training.loop.runner import TrainerDependencies


CLI_RUNNER = CliRunner()


class _BufferLogger(LoggerLike):
    """Test logger conforming to LoggerLike."""

    def __init__(self) -> None:
        self.messages: list[str] = []

    def debug(
        self,
        msg: object,
        *args: object,
        exc_info: Any = None,
        stack_info: bool = False,
        stacklevel: int = 1,
        extra: Mapping[str, object] | None = None,
    ) -> None:
        self.messages.append(str(msg))

    def info(
        self,
        msg: object,
        *args: object,
        exc_info: Any = None,
        stack_info: bool = False,
        stacklevel: int = 1,
        extra: Mapping[str, object] | None = None,
    ) -> None:
        self.messages.append(str(msg))

    def warning(
        self,
        msg: object,
        *args: object,
        exc_info: Any = None,
        stack_info: bool = False,
        stacklevel: int = 1,
        extra: Mapping[str, object] | None = None,
    ) -> None:
        self.messages.append(str(msg))

    def error(
        self,
        msg: object,
        *args: object,
        exc_info: Any = None,
        stack_info: bool = False,
        stacklevel: int = 1,
        extra: Mapping[str, object] | None = None,
    ) -> None:
        self.messages.append(str(msg))


@given(  # type: ignore[reportAny]
    exit_code=st.integers(min_value=1, max_value=16)
)
@settings(max_examples=5, deadline=None, derandomize=True)
def test_run_or_exit_maps_known_exceptions(exit_code: int) -> None:
    def _raise() -> None:
        raise FileNotFoundError("missing")

    with pytest.raises(typer.Exit) as excinfo:
        cli_helpers.run_or_exit(_raise, exception_exit_code=exit_code)

    assert excinfo.value.exit_code == exit_code


def test_run_or_exit_keyboard_interrupt_logs(caplog: pytest.LogCaptureFixture) -> None:
    with caplog.at_level(logging.INFO, logger="ml_playground.runtime_cli"):

        def _raise_keyboard_interrupt() -> None:
            raise KeyboardInterrupt

        result = cli_helpers.run_or_exit(
            _raise_keyboard_interrupt, keyboard_interrupt_msg="Interrupted"
        )

    assert result is None
    assert "Interrupted" in caplog.text


def test_extract_exp_config_handles_context() -> None:
    ctx = cast(typer.Context, SimpleNamespace(obj=None))
    assert cli_helpers.extract_exp_config(ctx) is None
    ctx.obj = {"exp_config": Path("/tmp/example.toml")}
    assert cli_helpers.extract_exp_config(ctx) == Path("/tmp/example.toml")


def test_log_directory_variants(tmp_path: Path) -> None:
    logger = _BufferLogger()
    cli_commands.log_directory("tag", "unset", None, logger)
    missing = tmp_path / "missing"
    cli_commands.log_directory("tag", "missing", missing, logger)
    existing = tmp_path / "exists"
    existing.mkdir()
    (existing / "file.txt").write_text("data", encoding="utf-8")
    cli_commands.log_directory("tag", "existing", existing, logger)
    assert any("<not set>" in msg for msg in logger.messages)
    assert any("missing" in msg for msg in logger.messages)
    assert any("Contents" in msg for msg in logger.messages)
    # Non-Path inputs should be ignored silently
    logger.messages.clear()
    cli_commands.log_directory("tag", "not_path", cast(Any, "/tmp/example"), logger)
    assert logger.messages == []


def test_log_command_status_handles_missing_path(
    tmp_path: Path, metadata_config_factory: Callable[[Path], MetadataConfig]
) -> None:
    logger = _BufferLogger()
    metadata = metadata_config_factory(tmp_path)
    cli_commands.log_command_status("tag", metadata, tmp_path / "missing", logger)
    assert any("missing" in message for message in logger.messages)


def test_log_command_status_swallows_errors(
    tmp_path: Path,
    metadata_config_factory: Callable[[Path], MetadataConfig],
    override_attr: Callable[[object, str, object], ContextManager[None]],
) -> None:
    metadata = metadata_config_factory(tmp_path)

    class FailingPath:
        def exists(self) -> bool:
            raise OSError("boom")

        def __str__(self) -> str:
            return "/failing/path"

    # Force log_directory to fail by passing an object that booms on access
    # Note: log_directory checks isinstance(path, Path) before calling exists()
    # BUT log_command_status calls log_directory(..., metadata.dataset_dir, ...)

    # Actually, we rely on log_command_status to call log_directory, and log_directory works on paths.
    # To avoid patching, we need to pass a path that causes failure.
    # However, log_directory is robust.
    # The original test mocked log_directory to raise.
    # We can fake the path if we type ignore the isinstance check or if we mock the logger?
    # No, we want to test log_command_status exception handling.
    # If we can't easily make Path operations fail without patching, we validly tested what we could.
    # But wait, log_command_status calls `log_directory`.
    # `log_directory` swallows explicit OSErrors during iteration.
    # `log_command_status` swallows generic exceptions during its whole execution.

    # Let's try to pass a path that triggers an error.
    # If we pass a FailingPath that IS NOT a Path, log_directory returns early (guard check).
    # So we need it to look like a Path but fail.
    # Python's pathlib.Path is hard to fake inheritance-wise.

    # Strategy: Pass an object that causes failure in `log_directory` BEFORE the isinstance check?
    # No, the check is first.

    # Alternative: Use a real path but make it fail permissions?
    # Hard in unit tests without sudo/chmod.

    # Let's simply simulate the "swallows errors" behavior by testing `log_directory` directly with a failing content listed?
    # Or accept that `log_command_status` generic catch-all might trigger on other things.

    # Re-reading `log_command_status`:
    # try:
    #     log_directory(tag, "out_dir", out_dir, logger)
    #     log_directory(tag, "dataset_dir", metadata.dataset_dir, logger)
    # except Exception as e:
    #     logger.warning(...)

    # If we pass a metadata object whose `dataset_dir` property raises when accessed?
    # But `metadata.dataset_dir` is a field.

    # What if `log_directory` fails?
    # We can't mock `log_directory`.

    # What if we pass a logger that raises?
    class ExplodingLogger(_BufferLogger):
        def info(self, msg: object, *args: object, **kwargs: object) -> None:
            raise OSError("logging boom")

    failing_logger = ExplodingLogger()

    # This will trigger the exception in log_command_status
    cli_commands.log_command_status(
        "tag", metadata, metadata.dataset_dir, failing_logger
    )

    # Wait, if logger raises, the catch block calls logger.warning, which might also raise?
    # If logger.warning raises, it propagates. The original test asserted "swallows errors".
    # If it propagates, it fails "swallows errors".
    # We want to verify `log_command_status` catches exceptions.

    # Let's make `info` raise, and `warning` working.
    class SemiExplodingLogger(_BufferLogger):
        def info(self, msg: object, *args: object, **kwargs: object) -> None:
            raise OSError("logging boom")

    failing_logger = SemiExplodingLogger()

    cli_commands.log_command_status(
        "tag", metadata, metadata.dataset_dir, failing_logger
    )

    # Verify warning was logged
    assert any("Failed to log artifacts" in msg for msg in failing_logger.messages)


def test_run_prepare_impl_executes_pipeline(
    tmp_path: Path,
    metadata_config_factory: Callable[[Path], MetadataConfig],
    override_attr: Callable[[object, str, object], ContextManager[None]],
) -> None:
    metadata = metadata_config_factory(tmp_path)
    calls: dict[str, Any] = {}

    class FakePipeline:
        def __init__(self) -> None:
            calls["init"] = True

        def run(self) -> None:
            calls["ran"] = True

    prepare_cfg = PreparerConfig.model_validate(
        {"tokenizer_type": "char", "raw_text_path": metadata.dataset_dir / "raw.txt"}
    ).model_copy(update={"logger": logging.getLogger("ml_playground.runtime_cli")})
    (metadata.dataset_dir / "raw.txt").write_text("data", encoding="utf-8")

    def _create_pipeline(_: PreparerConfig, __: MetadataConfig) -> FakePipeline:
        return FakePipeline()

    deps = CLIDependencies(
        create_pipeline=_create_pipeline,
        run_prepare=cli_commands.run_prepare_impl,
    )
    result = cli_commands.run_prepare_impl(
        "demo", prepare_cfg, metadata.config_path, metadata, deps
    )

    assert result.success is True
    assert calls == {"init": True, "ran": True}


def test_run_train_impl_requires_runtime(
    tmp_path: Path, metadata_config_factory: Callable[[Path], MetadataConfig]
) -> None:
    metadata = metadata_config_factory(tmp_path)
    cfg = cast(
        TrainerConfig,
        SimpleNamespace(
            runtime=None, logger=logging.getLogger("ml_playground.runtime_cli")
        ),
    )

    deps = cli_main.create_default_cli_dependencies()
    result = cli_commands.run_train_impl(
        "demo", cfg, metadata.config_path, metadata, deps
    )
    assert result.success is False
    assert "Runtime configuration is missing" in (result.stderr or "")


def test_run_sample_impl_requires_runtime(
    tmp_path: Path, metadata_config_factory: Callable[[Path], MetadataConfig]
) -> None:
    metadata = metadata_config_factory(tmp_path)
    cfg = cast(
        SamplerConfig,
        SimpleNamespace(
            runtime=None, logger=logging.getLogger("ml_playground.runtime_cli")
        ),
    )

    deps = cli_main.create_default_cli_dependencies()
    result = cli_commands.run_sample_impl(
        "demo", cfg, metadata.config_path, metadata, deps
    )
    assert result.success is False
    assert "Runtime configuration is missing" in (result.stderr or "")


def test_run_prepare_executes_pipeline(
    tmp_path: Path,
    metadata_config_factory: Callable[[Path], MetadataConfig],
    override_attr: Callable[[object, str, object], ContextManager[None]],
) -> None:
    metadata = metadata_config_factory(tmp_path)
    calls: dict[str, Any] = {}

    class FakePipeline:
        def __init__(self) -> None:
            calls["init"] = True

        def run(self) -> None:
            calls["ran"] = True

    prepare_cfg = cast(
        PreparerConfig,
        SimpleNamespace(logger=logging.getLogger("ml_playground.runtime_cli")),
    )

    def _create_pipeline(_: PreparerConfig, __: MetadataConfig) -> FakePipeline:
        return FakePipeline()

    deps = CLIDependencies(
        create_pipeline=_create_pipeline,
        run_prepare=cli_commands.run_prepare_impl,
    )
    cli_commands.run_prepare_impl(
        "demo", prepare_cfg, metadata.config_path, metadata, deps
    )

    assert calls == {"init": True, "ran": True}


def test_run_train_and_sample_wrappers(
    tmp_path: Path,
    metadata_config_factory: Callable[[Path], MetadataConfig],
    override_attr: Callable[[object, str, object], ContextManager[None]],
) -> None:
    metadata = metadata_config_factory(tmp_path)
    log_calls: list[tuple[str, Path]] = []
    trainer_called: dict[str, Any] = {}
    sampler_called: dict[str, Any] = {}

    class FakeTrainer:
        def __init__(
            self,
            cfg: object,
            metadata_cfg: object,
            trainer_deps: TrainerDependencies | None = None,
        ) -> None:
            trainer_called["cfg"] = cfg
            trainer_called["metadata"] = metadata_cfg

        def run(self) -> tuple[int, float]:
            trainer_called["ran"] = True
            return 0, 0.0

    class FakeSampler:
        def __init__(
            self,
            cfg: object,
            metadata_cfg: object,
            _sampler_deps: object | None = None,
        ) -> None:
            sampler_called["cfg"] = cfg
            sampler_called["metadata"] = metadata_cfg

        def run(self) -> tuple[int, float]:
            sampler_called["ran"] = True
            return 0, 0.0

    runtime_cfg = RuntimeConfig(
        device="cpu", dtype="float32", seed=11, out_dir=metadata.train_out_dir
    )
    train_cfg = TrainerConfig(
        model=ModelConfig(),
        data=DataConfig(),
        optim=OptimConfig(),
        schedule=LRSchedule(),
        runtime=runtime_cfg,
    ).model_copy(update={"logger": logging.getLogger("ml_playground.runtime_cli")})
    sample_cfg = SamplerConfig(
        runtime=runtime_cfg,
        sample=SampleConfig(),
    ).model_copy(update={"logger": logging.getLogger("ml_playground.runtime_cli")})

    def _load_experiment(name: str, exp_config: Path | None) -> ExperimentConfig:
        assert name == "demo"
        assert exp_config is None
        return cast(
            ExperimentConfig,
            SimpleNamespace(
                prepare=PreparerConfig(),
                training=train_cfg,
                sampling=sample_cfg,
                metadata=metadata,
            ),
        )

    def _ensure_train(_: ExperimentConfig) -> None:  # noqa: ARG001
        log_calls.append(("ensure_train", metadata.train_out_dir))

    def _ensure_sample(_: ExperimentConfig) -> None:  # noqa: ARG001
        log_calls.append(("ensure_sample", metadata.sample_out_dir))

    def _noop_prepare(
        experiment: str,
        prepare_cfg: Any,
        config_path: Path,
        metadata_cfg: Any,
        deps: CLIDependencies,
        learning_mode_engine: Any | None = None,
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
        train_cfg: Any,
        config_path: Path,
        metadata_cfg: Any,
        deps: CLIDependencies,
        learning_mode_engine: Any | None = None,
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
        sample_cfg: Any,
        config_path: Path,
        metadata_cfg: Any,
        deps: CLIDependencies,
        learning_mode_engine: Any | None = None,
    ) -> ToolResult:
        return ToolResult.create(
            success=True,
            exit_code=0,
            namespace="ml",
            category="sample",
            command=experiment,
        )

    def _log_command_status(tag: str, _s: Any, d: Path | None, _l: Any) -> None:
        if d is not None:
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

    deps = CLIDependencies(
        load_experiment=_load_experiment,
        ensure_train_prerequisites=_ensure_train,
        ensure_sample_prerequisites=_ensure_sample,
        run_prepare=_noop_prepare,
        run_train=cli_commands.run_train_impl,
        run_sample=cli_commands.run_sample_impl,
        trainer_factory=lambda cfg, metadata, deps=None: FakeTrainer(
            cfg, metadata, deps
        ),
        sampler_factory=lambda cfg, metadata, deps=None: FakeSampler(
            cfg, metadata, deps
        ),
        log_command_status=_log_command_status,
        global_device_setup=_device_setup,
        handle_tool_result=lambda _r, _l: None,
    )

    with ExitStack() as stack:
        stack.enter_context(
            override_attr(cli_commands, "log_command_status", _log_command_status)
        )
        stack.enter_context(
            override_attr(cli_device, "global_device_setup", _device_setup)
        )
        cli_commands.run_train_impl(
            "demo", train_cfg, metadata.config_path, metadata, deps
        )
        cli_commands.run_sample_impl(
            "demo", sample_cfg, metadata.config_path, metadata, deps
        )

    assert trainer_called.get("ran") is True
    assert sampler_called.get("ran") is True
    assert ("pre-train", metadata.train_out_dir) in log_calls
    assert ("post-train", metadata.train_out_dir) in log_calls
    assert ("pre-sample", metadata.sample_out_dir) in log_calls
    assert ("post-sample", metadata.sample_out_dir) in log_calls
