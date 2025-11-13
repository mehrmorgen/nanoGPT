from __future__ import annotations

import logging
from collections.abc import Callable
from contextlib import ExitStack
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
from typing import Any, ContextManager, cast

from hypothesis import example, given, settings
import hypothesis.strategies as st
import pytest
import typer
from typer.testing import CliRunner

import ml_playground.runtime.cli as cli
from ml_playground.runtime.cli import (
    CLIDependencies,
    global_device_setup,
    log_command_status,
    log_directory,
    run_prepare,
    run_prepare_impl,
    run_sample,
    run_sample_impl,
    run_train,
    run_train_impl,
    override_cli_dependencies,
)
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
from ml_playground.runtime.core.results import (
    LearningModeEngine,
    ToolResult,
)

_EXCEPTION_TYPES = st.sampled_from([FileNotFoundError, ValueError, TypeError])
_MESSAGES = st.text(min_size=1, max_size=32)
_EXPERIMENT_NAMES = st.text(
    alphabet="abcdefghijklmnopqrstuvwxyz0123456789_",
    min_size=1,
    max_size=8,
)


def test_run_or_exit_keyboard_interrupt_logs_message(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """KeyboardInterrupt should log info when `keyboard_interrupt_msg` is provided and return cleanly."""
    with caplog.at_level(logging.INFO, logger="ml_playground.runtime.cli"):

        def _raise_keyboard_interrupt() -> None:
            raise KeyboardInterrupt

        result = cli.run_or_exit(
            _raise_keyboard_interrupt,
            keyboard_interrupt_msg="Interrupted",
        )

    assert result is None
    assert "Interrupted" in caplog.messages


def test_extract_exp_config_handles_missing_and_present_context() -> None:
    """`_extract_exp_config` must gracefully handle contexts with and without `exp_config`."""

    ctx = cast(typer.Context, SimpleNamespace(obj=None))
    assert cli.extract_exp_config(ctx) is None

    ctx.obj = {"exp_config": Path("/tmp/example.toml")}
    assert cli.extract_exp_config(ctx) == Path("/tmp/example.toml")


def test_run_prepare_impl_executes_pipeline(
    tmp_path: Path,
    shared_config_factory: Callable[[Path], SharedConfig],
) -> None:
    """Test run prepare impl executes pipeline."""
    shared = shared_config_factory(tmp_path)
    raw_file = tmp_path / "raw.txt"
    raw_file.write_text("hello world", encoding="utf-8")
    cfg = PreparerConfig(tokenizer_type="char", raw_text_path=raw_file)

    run_prepare_impl("demo", cfg, shared.config_path, shared)

    assert (shared.dataset_dir / "train.bin").exists()
    assert (shared.dataset_dir / "val.bin").exists()
    assert (shared.dataset_dir / "meta.pkl").exists()


def test_log_dir_reports_states(tmp_path: Path) -> None:
    """Test log dir reports states."""
    class ListLogger:
        def __init__(self) -> None:
            self.infos: list[str] = []

        def info(self, message: str) -> None:
            self.infos.append(str(message))

    logger = ListLogger()

    log_directory("tag", "unset", None, logger)
    missing_path = tmp_path / "missing"
    log_directory("tag", "missing", missing_path, logger)

    existing = tmp_path / "existing"
    existing.mkdir()
    (existing / "file.txt").write_text("data", encoding="utf-8")
    log_directory("tag", "existing", existing, logger)

    assert any("<not set>" in msg for msg in logger.infos)
    assert any("missing" in msg for msg in logger.infos)
    assert any("Contents" in msg for msg in logger.infos)
    # Non-Path inputs should be ignored silently
    logger.infos.clear()
    log_directory("tag", "not_path", cast(Any, "/tmp/example"), logger)
    assert logger.infos == []


def test_log_command_status_handles_missing_directory(
    tmp_path: Path, shared_config_factory: Callable[[Path], SharedConfig]
) -> None:
    """Test log command status handles missing directory."""
    class ListLogger:
        def __init__(self) -> None:
            self.messages: list[str] = []

        def info(self, message: str) -> None:
            self.messages.append(str(message))

    logger = ListLogger()
    shared = shared_config_factory(tmp_path)

    log_command_status("tag", shared, cast(Path, None), logger)

    assert any("<not set>" in message for message in logger.messages)


def test_log_command_status_handles_missing_path(
    tmp_path: Path,
    shared_config_factory: Callable[[Path], SharedConfig],
) -> None:
    """Test log command status handles missing path."""
    class ListLogger:
        def __init__(self) -> None:
            self.messages: list[str] = []

        def info(self, message: str) -> None:
            self.messages.append(str(message))

    logger = ListLogger()
    missing = tmp_path / "missing"

    log_command_status("tag", shared_config_factory(tmp_path), missing, logger)

    assert any("missing" in message for message in logger.messages)


def test_log_dir_ignores_non_path() -> None:
    """Test log dir ignores non path."""
    class ListLogger:
        def __init__(self) -> None:
            self.messages: list[str] = []

        def info(self, message: str) -> None:
            self.messages.append(str(message))

    logger = ListLogger()
    log_directory("tag", "name", cast(Any, "/tmp/example"), logger)

    assert logger.messages == []


def test_log_command_status_swallows_exceptions(
    tmp_path: Path,
    shared_config_factory: Callable[[Path], SharedConfig],
    override_attr: Callable[[object, str, object], ContextManager[None]],
) -> None:
    """Test log command status swallows exceptions."""
    shared = shared_config_factory(tmp_path)

    class ListLogger:
        def __init__(self) -> None:
            self.messages: list[str] = []

        def info(self, message: str) -> None:
            self.messages.append(str(message))

    logger = ListLogger()

    def boom(*_args: object, **_kwargs: object) -> None:
        raise OSError("boom")

    with override_attr(cli, "log_directory", boom):
        log_command_status("tag", shared, shared.dataset_dir, logger)

    assert logger.messages == []


def test_run_prepare_executes_pipeline(
    tmp_path: Path,
    shared_config_factory: Callable[[Path], SharedConfig],
    override_attr: Callable[[object, str, object], ContextManager[None]],
) -> None:
    """Test run prepare executes pipeline."""
    shared = shared_config_factory(tmp_path)
    calls: dict[str, Any] = {}

    class FakePipeline:
        def __init__(self) -> None:
            calls["init"] = True

        def run(self) -> None:
            calls["ran"] = True

    prepare_cfg = cast(
        PreparerConfig, SimpleNamespace(logger=logging.getLogger("ml_playground.cli"))
    )

    with override_attr(cli, "create_pipeline", lambda cfg, shared_cfg: FakePipeline()):
        run_prepare("demo", prepare_cfg, shared.config_path, shared)

    assert calls == {"init": True, "ran": True}


def test_run_train_impl_requires_runtime(
    tmp_path: Path,
    shared_config_factory: Callable[[Path], SharedConfig],
) -> None:
    """Test run train impl requires runtime."""
    shared = shared_config_factory(tmp_path)
    cfg = cast(
        TrainerConfig,
        SimpleNamespace(runtime=None, logger=logging.getLogger("ml_playground.cli")),
    )

    with pytest.raises(typer.Exit):
        run_train("demo", cfg, shared.config_path, shared)


def test_run_sample_impl_requires_runtime(
    tmp_path: Path,
    shared_config_factory: Callable[[Path], SharedConfig],
) -> None:
    """Test run sample impl requires runtime."""
    shared = shared_config_factory(tmp_path)
    cfg = cast(
        SamplerConfig,
        SimpleNamespace(runtime=None, logger=logging.getLogger("ml_playground.cli")),
    )

    with pytest.raises(typer.Exit):
        run_sample("demo", cfg, shared.config_path, shared)


def test_run_train_requires_runtime(
    tmp_path: Path,
    shared_config_factory: Callable[[Path], SharedConfig],
) -> None:
    """Test run train requires runtime."""
    shared = shared_config_factory(tmp_path)
    cfg = cast(
        TrainerConfig,
        SimpleNamespace(runtime=None, logger=logging.getLogger("ml_playground.cli")),
    )

    with pytest.raises(typer.Exit) as excinfo:
        run_train("demo", cfg, shared.config_path, shared)

    assert excinfo.value.exit_code == 1


def test_run_train_executes_flow(
    tmp_path: Path,
    shared_config_factory: Callable[[Path], SharedConfig],
    override_attr: Callable[[object, str, object], ContextManager[None]],
) -> None:
    """Test run train executes flow."""
    shared = shared_config_factory(tmp_path)
    log_calls: list[tuple[str, Path]] = []
    trainer_called: dict[str, Any] = {}

    class FakeTrainer:
        def __init__(self, cfg: object, shared_cfg: object) -> None:
            trainer_called["cfg"] = cfg
            trainer_called["shared"] = shared_cfg

        def run(self) -> None:
            trainer_called["ran"] = True

    def fake_global(device: str, dtype: str, seed: int) -> None:
        trainer_called["global_setup"] = (device, dtype, seed)

    logger = logging.getLogger("ml_playground.cli")
    runtime = cast(
        RuntimeConfig, SimpleNamespace(device="cpu", dtype="float32", seed=42)
    )
    cfg = cast(TrainerConfig, SimpleNamespace(runtime=runtime, logger=logger))

    exp = ExperimentConfig(
        prepare=PreparerConfig(),
        train=TrainerConfig(
            model=ModelConfig(),
            data=DataConfig(),
            optim=OptimConfig(),
            schedule=LRSchedule(),
            runtime=RuntimeConfig(out_dir=shared.train_out_dir),
        ),
        sample=SamplerConfig(
            runtime=RuntimeConfig(out_dir=shared.sample_out_dir),
            sample=SampleConfig(),
        ),
        shared=shared,
    )

    def _load_experiment(name: str, exp_config: Path | None) -> ExperimentConfig:
        assert name == "demo"
        assert exp_config is None
        return exp

    deps = CLIDependencies(
        load_experiment=_load_experiment,
        ensure_train_prerequisites=lambda _: None,
        ensure_sample_prerequisites=lambda _: None,
        run_prepare=lambda *args, **kwargs: None,
        run_train=lambda *args, **kwargs: None,  # not overridden
        run_sample=lambda *args, **kwargs: None,
    )

    with ExitStack() as stack:
        stack.enter_context(override_cli_dependencies(deps))
        stack.enter_context(override_attr(cli, "CoreTrainer", FakeTrainer))
        stack.enter_context(
            override_attr(
                cli,
                "log_command_status",
                lambda tag, shared_cfg, out_dir, _logger: log_calls.append(
                    (tag, out_dir)
                ),
            )
        )
        stack.enter_context(override_attr(cli, "global_device_setup", fake_global))
        run_train("demo", cfg, shared.config_path, shared)

    assert trainer_called["cfg"] is cfg
    assert trainer_called["shared"] is shared
    assert trainer_called.get("ran") is True
    assert trainer_called["global_setup"] == ("cpu", "float32", 42)
    assert ("pre-train", shared.train_out_dir) in log_calls
    assert ("post-train", shared.train_out_dir) in log_calls


def test_run_train_logs_status(
    tmp_path: Path,
    shared_config_factory: Callable[[Path], SharedConfig],
    override_attr: Callable[[object, str, object], ContextManager[None]],
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Test run train logs status."""
    shared = shared_config_factory(tmp_path)

    class FakeTrainer:
        def __init__(self, cfg: object, shared_cfg: object) -> None:  # noqa: D401
            self.cfg = cfg
            self.shared_cfg = shared_cfg

        def run(self) -> None:
            pass

    runtime = cast(
        RuntimeConfig, SimpleNamespace(device="cpu", dtype="float32", seed=21)
    )
    logger_name = "ml_playground.cli.run_train_logs"
    train_cfg = cast(
        TrainerConfig,
        SimpleNamespace(runtime=runtime, logger=logging.getLogger(logger_name)),
    )

    exp = ExperimentConfig(
        prepare=PreparerConfig(),
        train=TrainerConfig(
            model=ModelConfig(),
            data=DataConfig(),
            optim=OptimConfig(),
            schedule=LRSchedule(),
            runtime=RuntimeConfig(out_dir=shared.train_out_dir),
        ),
        sample=SamplerConfig(
            runtime=RuntimeConfig(out_dir=shared.sample_out_dir),
            sample=SampleConfig(),
        ),
        shared=shared,
    )

    def _load_experiment(name: str, exp_config: Path | None) -> ExperimentConfig:
        assert name == "demo"
        assert exp_config is None
        return exp

    deps = CLIDependencies(
        load_experiment=_load_experiment,
        ensure_train_prerequisites=lambda _: None,
        ensure_sample_prerequisites=lambda _: None,
        run_prepare=lambda *args, **kwargs: None,
        run_train=lambda *args, **kwargs: None,
        run_sample=lambda *args, **kwargs: None,
    )

    caplog.set_level(logging.INFO, logger=logger_name)

    with ExitStack() as stack:
        stack.enter_context(override_cli_dependencies(deps))
        stack.enter_context(override_attr(cli, "CoreTrainer", FakeTrainer))
        stack.enter_context(
            override_attr(cli, "global_device_setup", lambda *args, **kwargs: None)
        )
        run_train("demo", train_cfg, shared.config_path, shared)

    text = "\n".join(caplog.messages)
    assert "Running trainer for experiment" in text
    assert "Trainer for demo finished" in text
    assert "[pre-train]" in text
    assert "[post-train]" in text


def test_run_sample_requires_runtime(
    tmp_path: Path,
    shared_config_factory: Callable[[Path], SharedConfig],
) -> None:
    """Test run sample requires runtime."""
    shared = shared_config_factory(tmp_path)
    cfg = cast(
        SamplerConfig,
        SimpleNamespace(runtime=None, logger=logging.getLogger("ml_playground.cli")),
    )

    with pytest.raises(typer.Exit) as excinfo:
        run_sample("demo", cfg, shared.config_path, shared)

    assert excinfo.value.exit_code == 1


def test_run_sample_executes_flow(
    tmp_path: Path,
    shared_config_factory: Callable[[Path], SharedConfig],
    override_attr: Callable[[object, str, object], ContextManager[None]],
) -> None:
    """Test run sample executes flow."""
    shared = shared_config_factory(tmp_path)
    log_calls: list[tuple[str, Path]] = []
    sampler_called: dict[str, Any] = {}

    class FakeSampler:
        def __init__(self, cfg: object, shared_cfg: object) -> None:
            sampler_called["cfg"] = cfg
            sampler_called["shared"] = shared_cfg

        def run(self) -> None:
            sampler_called["ran"] = True

    def fake_global(device: str, dtype: str, seed: int) -> None:
        sampler_called["global"] = (device, dtype, seed)

    logger = logging.getLogger("ml_playground.cli")
    runtime = cast(
        RuntimeConfig, SimpleNamespace(device="cpu", dtype="float32", seed=24)
    )
    cfg = cast(SamplerConfig, SimpleNamespace(runtime=runtime, logger=logger))

    exp = ExperimentConfig(
        prepare=PreparerConfig(),
        train=TrainerConfig(
            model=ModelConfig(),
            data=DataConfig(),
            optim=OptimConfig(),
            schedule=LRSchedule(),
            runtime=RuntimeConfig(out_dir=shared.train_out_dir),
        ),
        sample=SamplerConfig(
            runtime=RuntimeConfig(out_dir=shared.sample_out_dir),
            sample=SampleConfig(),
        ),
        shared=shared,
    )

    def _load_experiment(name: str, exp_config: Path | None) -> ExperimentConfig:
        assert name == "demo"
        assert exp_config is None
        return exp

    deps = CLIDependencies(
        load_experiment=_load_experiment,
        ensure_train_prerequisites=lambda _: None,
        ensure_sample_prerequisites=lambda _: None,
        run_prepare=lambda *args, **kwargs: None,
        run_train=lambda *args, **kwargs: None,
        run_sample=lambda *args, **kwargs: None,  # not overridden
    )

    with ExitStack() as stack:
        stack.enter_context(override_cli_dependencies(deps))
        stack.enter_context(override_attr(cli, "Sampler", FakeSampler))
        stack.enter_context(
            override_attr(
                cli,
                "log_command_status",
                lambda tag, shared_cfg, out_dir, _logger: log_calls.append(
                    (tag, out_dir)
                ),
            )
        )
        stack.enter_context(override_attr(cli, "global_device_setup", fake_global))
        run_sample("demo", cfg, shared.config_path, shared)

    assert sampler_called["cfg"] is cfg
    assert sampler_called["shared"] is shared
    assert sampler_called.get("ran") is True
    assert sampler_called["global"] == ("cpu", "float32", 24)
    assert ("pre-sample", shared.sample_out_dir) in log_calls
    assert ("post-sample", shared.sample_out_dir) in log_calls


def test_global_options_handles_ensure_object_errors() -> None:
    """Test global options handles ensure object errors."""
    class BadContext:
        def __init__(self) -> None:
            self.obj: dict[str, object] | None = None

        def ensure_object(self, *_args: object, **_kwargs: object) -> None:
            raise TypeError("boom")

    ctx = cast(typer.Context, BadContext())
    cli.global_options(ctx)

    assert ctx.obj is None


def test_run_or_exit_handles_runtime_error() -> None:
    """Test run or exit handles runtime error."""
    def _raise() -> None:
        raise RuntimeError("boom")

    with pytest.raises(typer.Exit) as exc:
        cli.run_or_exit(_raise, exception_exit_code=9)

    assert exc.value.exit_code == 9


def test_global_device_setup_handles_runtime_error(
    override_attr: Callable[[object, str, object], ContextManager[None]],
) -> None:
    """Test global device setup handles runtime error."""
    class BadTorch:
        def manual_seed(self, seed: int) -> None:  # pragma: no cover - invoked
            raise RuntimeError("fail")

    # Should not raise even though torch operations fail
    with override_attr(cli, "torch", BadTorch()):
        global_device_setup("cpu", "float32", 123)


def test_global_device_setup_sets_cuda_state(
    override_attr: Callable[[object, str, object], ContextManager[None]],
) -> None:
    """Test global device setup sets cuda state."""
    seed_calls: list[tuple[str, int]] = []

    fake_torch = SimpleNamespace(
        manual_seed=lambda seed: seed_calls.append(("cpu", seed)),
        cuda=SimpleNamespace(
            manual_seed=lambda seed: seed_calls.append(("cuda", seed)),
            is_available=lambda: True,
        ),
        backends=SimpleNamespace(
            cuda=SimpleNamespace(matmul=SimpleNamespace(fp32_precision="highest")),
            cudnn=SimpleNamespace(fp32_precision="highest"),
        ),
    )

    with override_attr(cli, "torch", fake_torch):
        global_device_setup("cuda", "float16", 7, cuda_is_available=lambda: True)

    assert ("cpu", 7) in seed_calls
    assert ("cuda", 7) in seed_calls
    assert fake_torch.backends.cuda.matmul.fp32_precision == "tf32"
    assert fake_torch.backends.cudnn.fp32_precision == "tf32"


def test_run_train_impl_invokes_trainer(
    tmp_path: Path,
    shared_config_factory: Callable[[Path], SharedConfig],
    override_attr: Callable[[object, str, object], ContextManager[None]],
) -> None:
    """Test run train impl invokes trainer."""
    shared = shared_config_factory(tmp_path)
    log_calls: list[tuple[str, Path]] = []
    trainer_called: dict[str, Any] = {}

    class FakeTrainer:
        def __init__(self, cfg: object, shared_cfg: object) -> None:
            trainer_called["cfg"] = cfg
            trainer_called["shared"] = shared_cfg

        def run(self) -> None:
            trainer_called["ran"] = True

    def fake_global(
        device: str, dtype: str, seed: int, *, cuda_is_available=None
    ) -> None:
        trainer_called["global_setup"] = (device, dtype, seed)

    logger = logging.getLogger("ml_playground.cli")
    runtime = cast(
        RuntimeConfig, SimpleNamespace(device="cpu", dtype="float32", seed=11)
    )
    train_cfg = cast(TrainerConfig, SimpleNamespace(runtime=runtime, logger=logger))

    with ExitStack() as stack:
        stack.enter_context(override_attr(cli, "CoreTrainer", FakeTrainer))
        stack.enter_context(
            override_attr(
                cli,
                "log_command_status",
                lambda tag, shared_cfg, out_dir, logger: log_calls.append(
                    (tag, out_dir)
                ),
            )
        )
        stack.enter_context(override_attr(cli, "global_device_setup", fake_global))
        run_train_impl("demo", train_cfg, shared.config_path, shared)

    assert trainer_called["cfg"] is train_cfg
    assert trainer_called["shared"] is shared
    assert trainer_called.get("ran") is True
    assert trainer_called["global_setup"] == ("cpu", "float32", 11)
    assert ("post-train", shared.train_out_dir) in log_calls


def test_run_sample_impl_invokes_sampler(
    tmp_path: Path,
    shared_config_factory: Callable[[Path], SharedConfig],
    override_attr: Callable[[object, str, object], ContextManager[None]],
) -> None:
    """Test run sample impl invokes sampler."""
    shared = shared_config_factory(tmp_path)
    log_calls: list[tuple[str, Path]] = []
    sampler_called: dict[str, Any] = {}

    class FakeSampler:
        def __init__(self, cfg: object, shared_cfg: object) -> None:
            sampler_called["cfg"] = cfg
            sampler_called["shared"] = shared_cfg

        def run(self) -> None:
            sampler_called["ran"] = True

    def fake_global(
        device: str, dtype: str, seed: int, *, cuda_is_available=None
    ) -> None:
        sampler_called["global_setup"] = (device, dtype, seed)

    logger = logging.getLogger("ml_playground.cli")
    runtime = cast(
        RuntimeConfig, SimpleNamespace(device="cpu", dtype="float32", seed=5)
    )
    sample_cfg = cast(SamplerConfig, SimpleNamespace(runtime=runtime, logger=logger))

    with ExitStack() as stack:
        stack.enter_context(override_attr(cli, "Sampler", FakeSampler))
        stack.enter_context(
            override_attr(
                cli,
                "log_command_status",
                lambda tag, shared_cfg, out_dir, logger: log_calls.append(
                    (tag, out_dir)
                ),
            )
        )
        stack.enter_context(override_attr(cli, "global_device_setup", fake_global))
        run_sample_impl("demo", sample_cfg, shared.config_path, shared)
    assert sampler_called["shared"] is shared
    assert sampler_called.get("ran") is True
    assert sampler_called["global_setup"] == ("cpu", "float32", 5)
    assert ("pre-sample", shared.sample_out_dir) in log_calls
    assert ("post-sample", shared.sample_out_dir) in log_calls


@given(exc_type=_EXCEPTION_TYPES, message=_MESSAGES, exit_code=st.integers(1, 32))
@example(exc_type=FileNotFoundError, message="missing.txt", exit_code=1)
@settings(max_examples=25, deadline=None, derandomize=True)
def test_run_or_exit_maps_known_exceptions_to_exit(
    exc_type: type[Exception], message: str, exit_code: int
) -> None:
    """`run_or_exit` should map known exception types to a `typer.Exit` with the provided code."""

    def _raise() -> None:
        raise exc_type(message)

    with pytest.raises(typer.Exit) as excinfo:
        cli.run_or_exit(_raise, exception_exit_code=exit_code)

    assert excinfo.value.exit_code == exit_code


@given(experiment=_EXPERIMENT_NAMES)
@example(experiment="alpha")
@settings(max_examples=15, deadline=None, derandomize=True)
def test_prepare_command_invokes_custom_dependency(experiment: str) -> None:
    """`prepare` CLI command must call the injected dependency exactly once for any experiment name."""

    with TemporaryDirectory() as tmpdir:
        base = Path(tmpdir)
        dataset_dir = base / "dataset"
        dataset_dir.mkdir()
        (dataset_dir / "meta.pkl").write_text("meta", encoding="utf-8")

        train_dir = base / "train"
        train_dir.mkdir()
        sample_dir = base / "sample"
        sample_dir.mkdir()

        config_path = dataset_dir / f"{experiment}.toml"
        config_path.write_text("{}", encoding="utf-8")

        shared = SharedConfig(
            experiment=experiment,
            config_path=config_path,
            project_home=base,
            dataset_dir=dataset_dir,
            train_out_dir=train_dir,
            sample_out_dir=sample_dir,
        )

        exp = ExperimentConfig(
            prepare=PreparerConfig(),
            train=TrainerConfig(
                model=ModelConfig(),
                data=DataConfig(),
                optim=OptimConfig(),
                schedule=LRSchedule(),
                runtime=RuntimeConfig(out_dir=train_dir),
            ),
            sample=SamplerConfig(
                runtime=RuntimeConfig(out_dir=sample_dir),
                sample=SampleConfig(),
            ),
            shared=shared,
        )

        calls: dict[str, int] = {"prepare": 0}

        def _load_experiment(name: str, exp_config: Path | None) -> ExperimentConfig:
            assert name == experiment
            assert exp_config is None
            return exp

        def _run_prepare(
            name: str,
            prepare_cfg: PreparerConfig,
            config_path_arg: Path,
            shared_cfg: SharedConfig,
            learning_mode_engine: LearningModeEngine | None = None,
        ) -> ToolResult:
            from ml_playground.tools.core.interfaces import ToolResult

            calls["prepare"] += 1
            assert name == experiment
            assert prepare_cfg is exp.prepare
            assert shared_cfg is shared
            return ToolResult.create(
                success=True,
                exit_code=0,
                namespace="ml",
                category="prepare",
                command=name,
                stdout="ok",
            )

        deps = CLIDependencies(
            load_experiment=_load_experiment,
            ensure_train_prerequisites=lambda _: None,
            ensure_sample_prerequisites=lambda _: None,
            run_prepare=_run_prepare,
            run_train=lambda *args, **kwargs: None,
            run_sample=lambda *args, **kwargs: None,
        )

        runner = CliRunner()
        with override_cli_dependencies(deps):
            result = runner.invoke(
                cli.app,
                [
                    "prepare",
                    experiment,
                ],
            )

        assert result.exit_code == 0
        assert calls["prepare"] == 1
