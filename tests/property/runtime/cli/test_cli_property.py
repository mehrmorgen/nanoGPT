"""Runtime CLI property tests aligned with the current architecture."""

from __future__ import annotations

import logging
from collections.abc import Callable
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
from typing import Any, ContextManager, cast

import hypothesis.strategies as st
from hypothesis import example, given, settings
import pytest
import typer
from typer.testing import CliRunner

from ml_playground.runtime.cli.main import (
    app,
    CLIDependencies,
    extract_exp_config,
    global_device_setup,
    log_command_status,
    log_directory,
    override_cli_dependencies,
    run_or_exit,
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
from ml_playground.runtime.core.results import LearningModeEngine, ToolResult


class LoggerProbe:
    """Minimal logger fake that satisfies the LoggerLike protocol."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, str]] = []

    def _record(self, level: str, msg: str, *args: Any, **kwargs: Any) -> None:
        if args:
            try:
                msg = msg % args
            except Exception:
                msg = " ".join([msg, *map(str, args)])
        self.calls.append((level, str(msg)))

    def debug(
        self, msg: str, *args: Any, **kwargs: Any
    ) -> None:  # pragma: no cover - unused
        self._record("debug", msg, *args)

    def info(self, msg: str, *args: Any, **kwargs: Any) -> None:
        self._record("info", msg, *args)

    def warning(
        self, msg: str, *args: Any, **kwargs: Any
    ) -> None:  # pragma: no cover - unused
        self._record("warning", msg, *args)

    def error(
        self, msg: str, *args: Any, **kwargs: Any
    ) -> None:  # pragma: no cover - unused
        self._record("error", msg, *args)

    @property
    def infos(self) -> list[str]:
        return [message for level, message in self.calls if level == "info"]


_EXCEPTIONS = st.sampled_from([FileNotFoundError, ValueError, RuntimeError])
_MESSAGES = st.text(min_size=1, max_size=32)
_EXPERIMENT_NAMES = st.text(
    alphabet="abcdefghijklmnopqrstuvwxyz0123456789_", min_size=1, max_size=8
)


def test_run_or_exit_keyboard_interrupt_logs_message(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """KeyboardInterrupt should log the provided message and exit cleanly."""

    with caplog.at_level(logging.INFO, logger="ml_playground.runtime.cli"):

        def _raise_keyboard_interrupt() -> None:
            raise KeyboardInterrupt

        run_or_exit(_raise_keyboard_interrupt, keyboard_interrupt_msg="Interrupted")

    assert "Interrupted" in caplog.messages


def test_extract_exp_config_handles_missing_and_present_context() -> None:
    """extract_exp_config should read the experiment path when available."""

    ctx = cast(typer.Context, cast(Any, SimpleNamespace(obj=None)))
    assert extract_exp_config(ctx) is None

    ctx.obj = {"exp_config": Path("/tmp/demo.toml")}
    assert extract_exp_config(ctx) == Path("/tmp/demo.toml")


def test_log_directory_reports_states(tmp_path: Path) -> None:
    logger = LoggerProbe()

    log_directory("tag", "unset", None, logger)
    missing = tmp_path / "missing"
    log_directory("tag", "missing", missing, logger)

    existing = tmp_path / "existing"
    existing.mkdir()
    (existing / "file.txt").write_text("data", encoding="utf-8")
    log_directory("tag", "existing", existing, logger)

    info_text = "\n".join(logger.infos)
    assert "<not set>" in info_text
    assert "missing" in info_text
    assert "Contents" in info_text


def test_log_command_status_handles_missing_paths(
    tmp_path: Path, shared_config_factory: Callable[[Path], SharedConfig]
) -> None:
    logger = LoggerProbe()
    shared = shared_config_factory(tmp_path)

    log_command_status("tag", shared, None, logger)
    assert any("<not set>" in message for message in logger.infos)


def test_log_command_status_swallows_log_directory_errors(
    tmp_path: Path,
    shared_config_factory: Callable[[Path], SharedConfig],
    override_attr: Callable[[object, str, object], ContextManager[None]],
) -> None:
    logger = LoggerProbe()
    shared = shared_config_factory(tmp_path)

    def boom(*_args: object, **_kwargs: object) -> None:
        raise OSError("boom")

    import ml_playground.runtime.cli as cli_pkg

    with override_attr(cli_pkg, "log_directory", boom):
        log_command_status("tag", shared, shared.dataset_dir, logger)

    assert logger.infos == []


def test_global_device_setup_handles_runtime_error() -> None:
    class BadTorch:
        def manual_seed(self, seed: int) -> None:  # pragma: no cover - invoked
            raise RuntimeError("fail")

    global_device_setup("cpu", "float32", 123, torch_module=BadTorch())


def test_global_device_setup_sets_cuda_state() -> None:
    seed_calls: list[tuple[str, int]] = []

    def _record_cpu_seed(seed: int) -> None:
        seed_calls.append(("cpu", seed))

    def _record_cuda_seed(seed: int) -> None:
        seed_calls.append(("cuda", seed))

    def _cuda_available() -> bool:
        return True

    fake_torch = SimpleNamespace(
        manual_seed=_record_cpu_seed,
        cuda=SimpleNamespace(
            manual_seed=_record_cuda_seed,
            is_available=_cuda_available,
        ),
        backends=SimpleNamespace(
            cuda=SimpleNamespace(matmul=SimpleNamespace(fp32_precision="highest")),
            cudnn=SimpleNamespace(fp32_precision="highest"),
        ),
    )

    global_device_setup(
        "cuda", "float16", 7, torch_module=fake_torch, cuda_is_available=_cuda_available
    )

    assert ("cpu", 7) in seed_calls
    assert ("cuda", 7) in seed_calls
    assert fake_torch.backends.cuda.matmul.fp32_precision == "tf32"
    assert fake_torch.backends.cudnn.fp32_precision == "tf32"


@given(exc_type=_EXCEPTIONS, message=_MESSAGES, exit_code=st.integers(1, 32))
@example(exc_type=FileNotFoundError, message="missing.txt", exit_code=1)
@settings(max_examples=20, deadline=None, derandomize=True)
def test_run_or_exit_maps_known_exceptions_to_exit(
    exc_type: type[Exception], message: str, exit_code: int
) -> None:
    def _raise() -> None:
        raise exc_type(message)

    with pytest.raises(typer.Exit) as excinfo:
        run_or_exit(_raise, exception_exit_code=exit_code)

    assert excinfo.value.exit_code == exit_code


@given(experiment=_EXPERIMENT_NAMES)
@example(experiment="alpha")
@settings(max_examples=10, deadline=None, derandomize=True)
def test_prepare_command_invokes_override(experiment: str) -> None:
    """The CLI prepare command should delegate to the injected dependency."""

    with TemporaryDirectory() as tmpdir:
        base = Path(tmpdir)
        dataset_dir = base / "dataset"
        dataset_dir.mkdir()
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

        def _noop_train(
            name: str,
            train_cfg: TrainerConfig,
            config_path_arg: Path,
            shared_cfg: SharedConfig,
            learning_mode_engine: LearningModeEngine | None = None,
        ) -> ToolResult:
            return ToolResult.create(
                success=True,
                exit_code=0,
                namespace="ml",
                category="train",
                command=name,
                stdout="ok",
            )

        def _noop_sample(
            name: str,
            sample_cfg: SamplerConfig,
            config_path_arg: Path,
            shared_cfg: SharedConfig,
            learning_mode_engine: LearningModeEngine | None = None,
        ) -> ToolResult:
            return ToolResult.create(
                success=True,
                exit_code=0,
                namespace="ml",
                category="sample",
                command=name,
                stdout="ok",
            )

        def _noop_train_prereqs(exp_cfg: ExperimentConfig) -> None:
            return None

        def _noop_sample_prereqs(exp_cfg: ExperimentConfig) -> None:
            return None

        deps = CLIDependencies(
            load_experiment=_load_experiment,
            ensure_train_prerequisites=_noop_train_prereqs,
            ensure_sample_prerequisites=_noop_sample_prereqs,
            run_prepare=_run_prepare,
            run_train=_noop_train,
            run_sample=_noop_sample,
        )

        runner = CliRunner()
        with override_cli_dependencies(deps):
            result = runner.invoke(app, ["prepare", experiment])

        assert result.exit_code == 0
        assert calls["prepare"] == 1
