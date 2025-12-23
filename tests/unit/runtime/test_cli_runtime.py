"""Typed runtime CLI unit tests using shared protocols (no monkeypatch)."""

from __future__ import annotations

import logging
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator

import pytest
import typer

import ml_playground.runtime.cli as runtime_cli
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
from ml_playground.core.logging_protocol import LoggerLike
from ml_playground.runtime.core.results import LearningInfo, ToolResult
from ml_playground.runtime.protocols import PrepareConfigLike, SharedConfigLike


class LoggerStub(LoggerLike):
    def __init__(self) -> None:
        self.messages: list[str] = []
        self.errors: list[str] = []

    def debug(self, msg: str, *args: object, **kwargs: object) -> None:
        self.messages.append(str(msg))

    def info(self, msg: str, *args: object, **kwargs: object) -> None:
        self.messages.append(str(msg))

    def warning(self, msg: str, *args: object, **kwargs: object) -> None:
        self.messages.append(str(msg))

    def error(self, msg: str, *args: object, **kwargs: object) -> None:
        self.errors.append(str(msg))
        self.messages.append(str(msg))


@contextmanager
def override_attr(obj: object, name: str, value: object) -> Iterator[None]:
    original = getattr(obj, name)
    setattr(obj, name, value)
    try:
        yield
    finally:
        setattr(obj, name, original)


def _no_op_global_device_setup(
    device: str,
    dtype: str,
    seed: int,
    *,
    cuda_is_available: object | None = None,
    torch_module: object | None = None,
) -> None:
    return None


def _no_op_log_command_status(
    tag: str, shared: SharedConfig, out_dir: Path, logger: LoggerLike
) -> None:
    return None


def _make_shared(tmp_path: Path) -> SharedConfig:
    cfg = tmp_path / "config.toml"
    cfg.touch()
    dataset_dir = tmp_path / "dataset"
    train_out_dir = tmp_path / "train"
    sample_out_dir = tmp_path / "sample"
    dataset_dir.mkdir(exist_ok=True)
    train_out_dir.mkdir(exist_ok=True)
    sample_out_dir.mkdir(exist_ok=True)
    return SharedConfig(
        experiment="demo",
        config_path=cfg,
        project_home=tmp_path,
        dataset_dir=dataset_dir,
        train_out_dir=train_out_dir,
        sample_out_dir=sample_out_dir,
    )


def _make_runtime(out_dir: Path) -> RuntimeConfig:
    return RuntimeConfig(device="cpu", dtype="float32", seed=0, out_dir=out_dir)


def _tool_success(category: str, command: str) -> ToolResult:
    return ToolResult.create(
        success=True,
        exit_code=0,
        namespace="ml",
        category=category,
        command=command,
        stdout=f"{category}:{command}:ok",
    )


# ---------------------------------------------------------------------------
# handle_tool_result / run_or_exit
# ---------------------------------------------------------------------------


def test_handle_tool_result_success_learning_mode(
    capsys: pytest.CaptureFixture[str],
) -> None:
    info = LearningInfo(
        commands_executed=["prepare demo"],
        explanations=["expl"],
        best_practices=["practice"],
        related_concepts=["concept"],
    )
    result = ToolResult.create(
        success=True,
        exit_code=0,
        namespace="ml",
        category="prepare",
        command="demo",
        stdout="done",
        learning_info=info,
    )

    runtime_cli.handle_tool_result(result, learning_mode=True)
    captured = capsys.readouterr()
    assert "done" in captured.out
    assert "Learning Mode" in captured.out
    assert "practice" in captured.out


def test_handle_tool_result_failure_prints_stderr_and_exits() -> None:
    result = ToolResult.create(
        success=False,
        exit_code=3,
        namespace="ml",
        category="train",
        command="demo",
        stderr="failure",
    )
    with pytest.raises(typer.Exit) as exc:
        runtime_cli.handle_tool_result(result, learning_mode=False)
    assert exc.value.exit_code == 3


def test_run_or_exit_keyboard_interrupt_logs(caplog: pytest.LogCaptureFixture) -> None:
    def _raise() -> None:
        raise KeyboardInterrupt

    with caplog.at_level(logging.INFO, logger="ml_playground.runtime.cli"):
        runtime_cli.run_or_exit(_raise, keyboard_interrupt_msg="Cancelled")
    assert "Cancelled" in caplog.text


def test_run_or_exit_file_not_found_exits() -> None:
    def _raise() -> None:
        raise FileNotFoundError("missing")

    with pytest.raises(typer.Exit) as exc:
        runtime_cli.run_or_exit(_raise, exception_exit_code=7)
    assert exc.value.exit_code == 7


def test_run_or_exit_value_error_logs_and_exits(
    caplog: pytest.LogCaptureFixture,
) -> None:
    def _raise() -> None:
        raise ValueError("bad value")

    with caplog.at_level(logging.ERROR):
        with pytest.raises(typer.Exit) as exc:
            runtime_cli.run_or_exit(_raise, exception_exit_code=5)
    assert exc.value.exit_code == 5


# ---------------------------------------------------------------------------
# log_directory / log_command_status
# ---------------------------------------------------------------------------


def test_log_directory_variants(tmp_path: Path) -> None:
    logger = LoggerStub()
    runtime_cli.log_directory("tag", "unset", None, logger)
    missing = tmp_path / "missing"
    runtime_cli.log_directory("tag", "missing", missing, logger)
    existing = tmp_path / "exists"
    existing.mkdir()
    (existing / "file.txt").write_text("data", encoding="utf-8")
    runtime_cli.log_directory("tag", "existing", existing, logger)
    assert any("<not set>" in msg for msg in logger.messages)
    assert any("missing" in msg for msg in logger.messages)
    assert any("Contents" in msg for msg in logger.messages)


def test_log_command_status_handles_missing_path(tmp_path: Path) -> None:
    logger = LoggerStub()
    shared = _make_shared(tmp_path)
    runtime_cli.log_command_status("tag", shared, tmp_path / "missing", logger)
    assert any("missing" in message for message in logger.messages)


# ---------------------------------------------------------------------------
# prepare/train/sample wrappers with dependency overrides (typed)
# ---------------------------------------------------------------------------


def test_run_prepare_with_overrides(tmp_path: Path) -> None:
    shared = _make_shared(tmp_path)
    prepare_cfg = PreparerConfig(
        tokenizer_type="char", raw_text_path=shared.dataset_dir / "raw.txt"
    ).model_copy(update={"logger": logging.getLogger("ml_playground.runtime.cli")})

    def _load_experiment(experiment: str, exp_config: Path | None) -> SharedConfig:
        assert experiment == "demo"
        assert exp_config is None
        return shared

    def _run_prepare(
        experiment: str,
        prepare_cfg2: PrepareConfigLike,
        config_path: Path,
        shared_cfg: SharedConfigLike,
        learning_mode_engine: object | None,
    ) -> ToolResult:
        assert experiment == "demo"
        assert prepare_cfg2 is prepare_cfg
        assert config_path == shared.config_path
        assert shared_cfg is shared
        assert learning_mode_engine is None
        return _tool_success("prepare", experiment)

    deps = runtime_cli.CLIDependencies(
        load_experiment=_load_experiment,
        ensure_train_prerequisites=lambda _: None,
        ensure_sample_prerequisites=lambda _: None,
        run_prepare=_run_prepare,
        run_train=lambda name, cfg, cfg_path, shared_cfg, lm: _tool_success(
            "train", name
        ),
        run_sample=lambda name, cfg, cfg_path, shared_cfg, lm: _tool_success(
            "sample", name
        ),
    )

    with (
        override_attr(runtime_cli, "run_prepare_impl", _run_prepare),
        override_attr(runtime_cli, "global_device_setup", _no_op_global_device_setup),
        override_attr(runtime_cli, "log_command_status", _no_op_log_command_status),
        runtime_cli.override_cli_dependencies(deps),
    ):
        result = runtime_cli.run_prepare(
            "demo", prepare_cfg, shared.config_path, shared
        )

    assert result.success is True


def test_run_train_with_overrides(tmp_path: Path) -> None:
    shared = _make_shared(tmp_path)
    runtime_cfg = _make_runtime(shared.train_out_dir)
    train_cfg = TrainerConfig(
        model=ModelConfig(),
        data=DataConfig(),
        optim=OptimConfig(),
        schedule=LRSchedule(),
        runtime=runtime_cfg,
        logger=logging.getLogger("ml_playground.runtime.cli"),
    )

    def _load_experiment(experiment: str, exp_config: Path | None) -> SharedConfig:
        assert experiment == "demo"
        assert exp_config is None
        return shared

    def _run_prepare(
        experiment: str,
        prepare_cfg: PrepareConfigLike,
        config_path: Path,
        shared_cfg: SharedConfig,
        learning_mode_engine: object | None,
    ) -> ToolResult:
        return _tool_success("prepare", experiment)

    def _run_train(
        experiment: str,
        train_cfg2: TrainerConfig,
        config_path: Path,
        shared_cfg: SharedConfig,
        learning_mode_engine: object | None,
    ) -> ToolResult:
        assert experiment == "demo"
        assert train_cfg2 is train_cfg
        assert config_path == shared.config_path
        assert shared_cfg is shared
        assert learning_mode_engine is None
        return _tool_success("train", experiment)

    deps = runtime_cli.CLIDependencies(
        load_experiment=_load_experiment,
        ensure_train_prerequisites=lambda _: None,
        ensure_sample_prerequisites=lambda _: None,
        run_prepare=_run_prepare,
        run_train=_run_train,
        run_sample=lambda name, cfg, cfg_path, shared_cfg, lm: _tool_success(
            "sample", name
        ),
    )

    with (
        override_attr(runtime_cli, "run_train_impl", _run_train),
        override_attr(runtime_cli, "global_device_setup", _no_op_global_device_setup),
        override_attr(runtime_cli, "log_command_status", _no_op_log_command_status),
        runtime_cli.override_cli_dependencies(deps),
    ):
        result = runtime_cli.run_train("demo", train_cfg, shared.config_path, shared)

    assert result.success is True


def test_run_sample_with_overrides(tmp_path: Path) -> None:
    shared = _make_shared(tmp_path)
    runtime_cfg = _make_runtime(shared.sample_out_dir)
    sample_cfg = SamplerConfig(
        runtime=runtime_cfg,
        sample=SampleConfig(),
        logger=logging.getLogger("ml_playground.runtime.cli"),
    )

    def _load_experiment(experiment: str, exp_config: Path | None) -> SharedConfig:
        assert experiment == "demo"
        assert exp_config is None
        return shared

    def _run_prepare(
        experiment: str,
        prepare_cfg: PrepareConfigLike,
        config_path: Path,
        shared_cfg: SharedConfig,
        learning_mode_engine: object | None,
    ) -> ToolResult:
        return _tool_success("prepare", experiment)

    def _run_train(
        experiment: str,
        train_cfg: TrainerConfig,
        config_path: Path,
        shared_cfg: SharedConfig,
        learning_mode_engine: object | None,
    ) -> ToolResult:
        return _tool_success("train", experiment)

    def _run_sample(
        experiment: str,
        sample_cfg2: SamplerConfig,
        config_path: Path,
        shared_cfg: SharedConfig,
        learning_mode_engine: object | None,
    ) -> ToolResult:
        assert experiment == "demo"
        assert sample_cfg2 is sample_cfg
        assert config_path == shared.config_path
        assert shared_cfg is shared
        assert learning_mode_engine is None
        return _tool_success("sample", experiment)

    deps = runtime_cli.CLIDependencies(
        load_experiment=_load_experiment,
        ensure_train_prerequisites=lambda _: None,
        ensure_sample_prerequisites=lambda _: None,
        run_prepare=_run_prepare,
        run_train=_run_train,
        run_sample=_run_sample,
    )

    with (
        override_attr(runtime_cli, "run_sample_impl", _run_sample),
        override_attr(runtime_cli, "global_device_setup", _no_op_global_device_setup),
        override_attr(runtime_cli, "log_command_status", _no_op_log_command_status),
        runtime_cli.override_cli_dependencies(deps),
    ):
        result = runtime_cli.run_sample("demo", sample_cfg, shared.config_path, shared)

    assert result.success is True


# ---------------------------------------------------------------------------
# Typer helpers and global options
# ---------------------------------------------------------------------------


def test_extract_exp_config_handles_context(tmp_path: Path) -> None:
    ctx = typer.Context(runtime_cli.get_command(runtime_cli.app))
    ctx.obj = {}
    assert runtime_cli.extract_exp_config(ctx) is None
    cfg_path = tmp_path / "exp.toml"
    ctx.obj["exp_config"] = cfg_path
    assert runtime_cli.extract_exp_config(ctx) == cfg_path


def test_global_options_sets_exp_config(tmp_path: Path) -> None:
    ctx = typer.Context(runtime_cli.get_command(runtime_cli.app))
    cfg_path = tmp_path / "config.toml"
    cfg_path.write_text("[section]\nvalue=1\n")

    runtime_cli.global_options(
        ctx, exp_config=cfg_path, learning_mode=False, verbosity=1
    )

    assert ctx.obj is not None
    assert ctx.obj["exp_config"] == cfg_path


# ---------------------------------------------------------------------------
# main entry
# ---------------------------------------------------------------------------


def test_main_entry_handles_keyboard_interrupt() -> None:
    original_app = runtime_cli.app
    original_echo = runtime_cli.typer.echo

    class RaisingApp:
        def __call__(self) -> None:
            raise KeyboardInterrupt

    runtime_cli.app = RaisingApp()  # type: ignore[assignment]
    outputs: list[str] = []

    def _record_echo(msg: str, err: bool = False) -> None:
        outputs.append(str(msg))

    runtime_cli.typer.echo = _record_echo  # type: ignore[assignment]

    with pytest.raises(typer.Exit) as exc:
        runtime_cli.main_entry()

    assert exc.value.exit_code == 1
    assert any("Operation cancelled" in msg for msg in outputs)
    runtime_cli.app = original_app
    runtime_cli.typer.echo = original_echo  # type: ignore[assignment]


def test_main_entry_handles_generic_exception() -> None:
    original_app = runtime_cli.app
    original_echo = runtime_cli.typer.echo

    class RaisingApp:
        def __call__(self) -> None:
            raise RuntimeError("boom")

    runtime_cli.app = RaisingApp()  # type: ignore[assignment]
    outputs: list[str] = []

    def _record_echo(msg: str, err: bool = False) -> None:
        outputs.append(str(msg))

    runtime_cli.typer.echo = _record_echo  # type: ignore[assignment]

    with pytest.raises(typer.Exit) as exc:
        runtime_cli.main_entry()

    assert exc.value.exit_code == 1
    assert any("Runtime CLI execution failed" in msg for msg in outputs)
    runtime_cli.app = original_app
    runtime_cli.typer.echo = original_echo  # type: ignore[assignment]
