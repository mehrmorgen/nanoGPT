# ruff: noqa: TID251
from __future__ import annotations

import logging
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Callable

import pytest
import typer

import ml_playground.runtime.cli.main as runtime_cli
from ml_playground.core.logging_protocol import LoggerLike
from ml_playground.runtime.cli.main import (
    CLIDependencies,
    LearningModeEngine,
    VerbosityLevel,
    extract_exp_config,
    global_device_setup,
    handle_tool_result,
    log_command_status,
    log_directory,
    run_analyze,
    run_or_exit,
    run_prepare,
    run_sample,
    run_sample_cmd,
    run_train,
    run_train_cmd,
)
from ml_playground.runtime.core.results import LearningInfo, ToolResult
from ml_playground.runtime import runners as runtime_runners


# ---------------------------------------------------------------------------
# Helpers and fixtures
# ---------------------------------------------------------------------------


class DummyLogger:
    def __init__(self) -> None:
        self.infos: list[str] = []
        self.errors: list[str] = []

    def info(self, message: str) -> None:
        self.infos.append(message)

    def error(self, message: str) -> None:
        self.errors.append(message)


class RecordingLearningEngine:
    def __init__(self) -> None:
        self.calls: list[tuple[str, str, str, list[str]]] = []

    def explain_command(
        self,
        *,
        command: str,
        context: str,
        category: str,
        executed_commands: list[str],
    ) -> LearningInfo:
        self.calls.append((command, context, category, executed_commands))
        return LearningInfo(
            commands_executed=executed_commands,
            explanations=[f"Explain {command}"],
            best_practices=["Practice"],
            related_concepts=["Concept"],
        )


class FakeTyperContext:
    def __init__(self, initial_obj: dict[str, object] | None = None) -> None:
        self.obj: dict[str, object] | None = initial_obj

    def ensure_object(self, typ: type[dict[str, object]]) -> dict[str, object]:
        if self.obj is None:
            self.obj = typ()
        elif not isinstance(self.obj, typ):
            raise TypeError("Context object is not the expected type")
        return self.obj


@pytest.fixture
def learning_engine() -> LearningModeEngine:
    return LearningModeEngine(VerbosityLevel.STANDARD)


def _make_runtime(
    device: str = "cpu", dtype: str = "float32", seed: int = 0
) -> SimpleNamespace:
    return SimpleNamespace(device=device, dtype=dtype, seed=seed)


def _make_shared(tmp_path: Path) -> SimpleNamespace:
    cfg = tmp_path / "config.toml"
    cfg.touch()
    shared = SimpleNamespace(
        config_path=cfg,
        dataset_dir=tmp_path / "dataset",
        train_out_dir=tmp_path / "train",
        sample_out_dir=tmp_path / "sample",
    )
    shared.dataset_dir.mkdir(exist_ok=True)
    shared.train_out_dir.mkdir(exist_ok=True)
    shared.sample_out_dir.mkdir(exist_ok=True)
    return shared


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

    handle_tool_result(result, learning_mode=True)
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
        handle_tool_result(result, learning_mode=False)
    assert exc.value.exit_code == 3


def test_run_or_exit_keyboard_interrupt_logs(caplog: pytest.LogCaptureFixture) -> None:
    def _raise() -> None:
        raise KeyboardInterrupt

    with caplog.at_level(logging.INFO, logger="ml_playground.runtime.helpers"):
        run_or_exit(_raise, keyboard_interrupt_msg="Cancelled")
    assert "Cancelled" in caplog.text


def test_run_or_exit_file_not_found_exits() -> None:
    def _raise() -> None:
        raise FileNotFoundError("missing")

    with pytest.raises(typer.Exit) as exc:
        run_or_exit(_raise, exception_exit_code=7)
    assert exc.value.exit_code == 7


def test_run_or_exit_value_error_logs_and_exits(
    caplog: pytest.LogCaptureFixture,
) -> None:
    def _raise() -> None:
        raise ValueError("bad value")

    with caplog.at_level(logging.ERROR):
        with pytest.raises(typer.Exit) as exc:
            run_or_exit(_raise, exception_exit_code=5)
    assert exc.value.exit_code == 5
    assert "bad value" in caplog.text


def test_run_or_exit_runtime_error_logs_and_exits(
    caplog: pytest.LogCaptureFixture,
) -> None:
    def _raise() -> None:
        raise RuntimeError("boom")

    with caplog.at_level(logging.ERROR):
        with pytest.raises(typer.Exit) as exc:
            run_or_exit(_raise, exception_exit_code=9)
    assert exc.value.exit_code == 9
    assert "boom" in caplog.text


# ---------------------------------------------------------------------------
# runtime_runners integration (no monkeypatch)
# ---------------------------------------------------------------------------


def _make_runtime_hooks(
    *,
    pipeline_factory: Callable[[Any, Any], Any] | None = None,
    trainer_factory: Callable[[Any, Any], Any] | None = None,
    sampler_factory: Callable[[Any, Any], Any] | None = None,
    device_setup: Callable[[str, str, int], None] | None = None,
    log_status: Callable[[str, Any, Path, LoggerLike], None] | None = None,
) -> runtime_runners.RuntimeRunHooks:
    return runtime_runners.RuntimeRunHooks(
        pipeline_factory=pipeline_factory
        or (lambda cfg, shared: SimpleNamespace(run=lambda: None)),
        trainer_factory=trainer_factory
        or (lambda cfg, shared: SimpleNamespace(run=lambda: None)),
        sampler_factory=sampler_factory
        or (lambda cfg, shared: SimpleNamespace(run=lambda: None)),
        device_setup=device_setup or (lambda *_: None),
        log_status=log_status or (lambda *_: None),
    )


def test_run_prepare_impl_success(tmp_path: Path) -> None:
    shared = _make_shared(tmp_path)
    logger = DummyLogger()
    cfg = SimpleNamespace(logger=logger)

    def pipeline_factory(cfg_obj: Any, shared_obj: Any) -> Any:
        assert cfg_obj is cfg
        assert shared_obj is shared

        class Pipeline:
            def run(self_inner) -> None:
                logger.info("pipeline run")

        return Pipeline()

    hooks = _make_runtime_hooks(pipeline_factory=pipeline_factory)

    result = runtime_runners.run_prepare_impl(
        "demo",
        cfg,
        shared.config_path,
        shared,
        LearningModeEngine(VerbosityLevel.STANDARD),
        hooks=hooks,
    )

    assert result.success is True
    assert "pipeline run" in logger.infos
    assert result.learning_info is not None


def test_run_prepare_impl_failure(tmp_path: Path) -> None:
    shared = _make_shared(tmp_path)
    logger = DummyLogger()
    cfg = SimpleNamespace(logger=logger)

    class BoomPipeline:
        def run(self_inner) -> None:
            raise RuntimeError("boom")

    hooks = _make_runtime_hooks(pipeline_factory=lambda *_: BoomPipeline())
    engine = RecordingLearningEngine()

    result = runtime_runners.run_prepare_impl(
        "demo", cfg, shared.config_path, shared, engine, hooks=hooks
    )

    assert result.success is False
    assert logger.errors and "boom" in logger.errors[0]
    assert engine.calls and engine.calls[0][0] == "demo"
    assert result.learning_info is not None


def test_run_prepare_impl_without_learning_engine(tmp_path: Path) -> None:
    shared = _make_shared(tmp_path)
    logger = DummyLogger()
    cfg = SimpleNamespace(logger=logger)

    hooks = _make_runtime_hooks(
        pipeline_factory=lambda *_: SimpleNamespace(run=lambda: None)
    )

    result = runtime_runners.run_prepare_impl(
        "demo", cfg, shared.config_path, shared, None, hooks=hooks
    )

    assert result.success is True
    assert result.learning_info is not None
    assert result.learning_info.explanations == []


def test_run_train_impl_success(tmp_path: Path) -> None:
    shared = _make_shared(tmp_path)
    logger = DummyLogger()
    runtime = _make_runtime(device="cuda", dtype="float16", seed=42)
    cfg = SimpleNamespace(logger=logger, runtime=runtime)

    device_calls: list[tuple[str, str, int]] = []
    log_calls: list[tuple[str, Path]] = []
    trainer_calls: list[tuple[Any, Any]] = []

    class Trainer:
        def __init__(self, cfg_obj: Any, shared_obj: Any) -> None:
            trainer_calls.append((cfg_obj, shared_obj))

        def run(self) -> None:
            logger.info("trainer run")

    hooks = _make_runtime_hooks(
        trainer_factory=lambda cfg_obj, shared_obj: Trainer(cfg_obj, shared_obj),
        device_setup=lambda device, dtype, seed: device_calls.append(
            (device, dtype, seed)
        ),
        log_status=lambda tag, shared_obj, out_dir, log: log_calls.append(
            (tag, out_dir)
        ),
    )

    result = runtime_runners.run_train_impl(
        "demo",
        cfg,
        shared.config_path,
        shared,
        LearningModeEngine(VerbosityLevel.STANDARD),
        hooks=hooks,
    )

    assert result.success is True
    assert trainer_calls == [(cfg, shared)]
    assert device_calls == [("cuda", "float16", 42)]
    assert {tag for tag, _ in log_calls} == {"pre-train", "post-train"}
    assert result.learning_info is not None


def test_run_train_impl_failure(tmp_path: Path) -> None:
    shared = _make_shared(tmp_path)
    logger = DummyLogger()
    cfg = SimpleNamespace(logger=logger, runtime=_make_runtime())

    class BadTrainer:
        def __init__(self, *_: Any) -> None:
            pass

        def run(self) -> None:
            raise RuntimeError("trainer fail")

    hooks = _make_runtime_hooks(trainer_factory=lambda *_: BadTrainer())
    engine = RecordingLearningEngine()

    result = runtime_runners.run_train_impl(
        "demo", cfg, shared.config_path, shared, engine, hooks=hooks
    )

    assert result.success is False
    assert logger.errors and "trainer fail" in logger.errors[0]
    assert engine.calls and engine.calls[0][0] == "demo"
    assert result.learning_info is not None


def test_run_train_impl_missing_runtime(tmp_path: Path) -> None:
    shared = _make_shared(tmp_path)
    cfg = SimpleNamespace(logger=DummyLogger(), runtime=None)

    result = runtime_runners.run_train_impl(
        "demo", cfg, shared.config_path, shared, None, hooks=_make_runtime_hooks()
    )

    assert result.success is False
    assert "missing" in result.stderr
    assert cfg.logger.errors


def test_run_sample_impl_success(tmp_path: Path) -> None:
    shared = _make_shared(tmp_path)
    logger = DummyLogger()
    cfg = SimpleNamespace(logger=logger, runtime=_make_runtime())

    sample_calls: list[tuple[Any, Any]] = []

    class Sampler:
        def __init__(self, cfg_obj: Any, shared_obj: Any) -> None:
            sample_calls.append((cfg_obj, shared_obj))

        def run(self) -> None:
            logger.info("sampler run")

    hooks = _make_runtime_hooks(
        sampler_factory=lambda cfg_obj, shared_obj: Sampler(cfg_obj, shared_obj),
        device_setup=lambda *_: None,
    )

    result = runtime_runners.run_sample_impl(
        "demo",
        cfg,
        shared.config_path,
        shared,
        LearningModeEngine(VerbosityLevel.STANDARD),
        hooks=hooks,
    )

    assert result.success is True
    assert sample_calls == [(cfg, shared)]
    assert "sampler run" in logger.infos
    assert result.learning_info is not None


def test_run_sample_impl_failure(tmp_path: Path) -> None:
    shared = _make_shared(tmp_path)
    logger = DummyLogger()
    cfg = SimpleNamespace(logger=logger, runtime=_make_runtime())

    class BadSampler:
        def __init__(self, *_: Any) -> None:
            pass

        def run(self) -> None:
            raise RuntimeError("sampler fail")

    hooks = _make_runtime_hooks(sampler_factory=lambda *_: BadSampler())
    engine = RecordingLearningEngine()

    result = runtime_runners.run_sample_impl(
        "demo", cfg, shared.config_path, shared, engine, hooks=hooks
    )

    assert result.success is False
    assert "sampler fail" in result.stderr
    assert engine.calls and engine.calls[0][0] == "demo"
    assert result.learning_info is not None


def test_run_sample_impl_missing_runtime(tmp_path: Path) -> None:
    shared = _make_shared(tmp_path)
    cfg = SimpleNamespace(logger=DummyLogger(), runtime=None)

    result = runtime_runners.run_sample_impl(
        "demo", cfg, shared.config_path, shared, None, hooks=_make_runtime_hooks()
    )

    assert result.success is False
    assert "missing" in result.stderr
    assert cfg.logger.errors


# ---------------------------------------------------------------------------
# global_device_setup and logging helpers
# ---------------------------------------------------------------------------


def test_global_device_setup_handles_attribute_error() -> None:
    class TorchStub:
        def manual_seed(self, _seed: int) -> None:
            raise AttributeError("missing")

    global_device_setup(
        "cpu", "float32", 0, torch_module=TorchStub(), cuda_is_available=lambda: False
    )


def test_global_device_setup_cuda_branch() -> None:
    calls: list[str] = []

    class TorchStub:
        def __init__(self) -> None:
            def is_available() -> bool:
                calls.append("is_available")
                return True

            def manual_seed_cuda(seed: int) -> None:
                calls.append(f"cuda_seed:{seed}")

            self.cuda = SimpleNamespace(
                is_available=is_available, manual_seed=manual_seed_cuda
            )
            self.backends = SimpleNamespace(
                cuda=SimpleNamespace(matmul=SimpleNamespace()),
                cudnn=SimpleNamespace(),
            )

        def manual_seed(self, seed: int) -> None:
            calls.append(f"seed:{seed}")

    torch_stub = TorchStub()
    global_device_setup("cuda", "float16", 99, torch_module=torch_stub)

    assert "seed:99" in calls
    assert "cuda_seed:99" in calls
    assert torch_stub.backends.cuda.matmul.fp32_precision == "tf32"
    assert torch_stub.backends.cudnn.fp32_precision == "tf32"


def test_global_device_setup_tf32_attribute_errors() -> None:
    calls: list[str] = []

    class FailingMatmul:
        def __setattr__(self, key: str, value: Any) -> None:
            calls.append(f"matmul:{key}")
            raise AttributeError("no attr")

    class FailingBackend:
        def __init__(self) -> None:
            self.cuda = SimpleNamespace(matmul=FailingMatmul())

            class FailingCudnn:
                def __setattr__(self, key: str, value: Any) -> None:
                    calls.append(f"cudnn:{key}")
                    raise AttributeError("no attr")

            self.cudnn = FailingCudnn()

    class TorchStub:
        def __init__(self) -> None:
            self.cuda = SimpleNamespace(
                is_available=lambda: True,
                manual_seed=lambda seed: calls.append(f"seed_cuda:{seed}"),
            )
            self.backends = FailingBackend()

        def manual_seed(self, seed: int) -> None:
            calls.append(f"seed:{seed}")

    torch_stub = TorchStub()
    global_device_setup("cuda", "float16", 7, torch_module=torch_stub)
    assert "seed:7" in calls
    assert "matmul:fp32_precision" in calls
    assert "cudnn:fp32_precision" in calls


def test_log_directory_variants(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    logger = logging.getLogger("test")

    with caplog.at_level(logging.INFO, logger="test"):
        log_directory("tag", "dir", None, logger)
        log_directory("tag", "dir", tmp_path / "missing", logger)

    existing = tmp_path / "exists"
    existing.mkdir()
    (existing / "file.txt").write_text("data")
    with caplog.at_level(logging.INFO, logger="test"):
        log_directory("tag", "dir", existing, logger)

    assert "<not set>" in caplog.text
    assert "(missing)" in caplog.text
    assert "Contents" in caplog.text


def test_log_command_status_handles_errors(tmp_path: Path) -> None:
    shared = SimpleNamespace(dataset_dir=tmp_path / "dataset")
    logger = logging.getLogger("test")
    log_command_status("tag", shared, tmp_path / "out", logger)


# ---------------------------------------------------------------------------
# Wrapper helpers
# ---------------------------------------------------------------------------


def _fake_hooks_with_result(result: ToolResult) -> runtime_runners.RuntimeRunHooks:
    del result

    class _Runner:
        def run(self_inner) -> None:
            return None

    return runtime_runners.RuntimeRunHooks(
        pipeline_factory=lambda *_: _Runner(),
        trainer_factory=lambda *_: _Runner(),
        sampler_factory=lambda *_: _Runner(),
        device_setup=lambda *_: None,
        log_status=lambda *_: None,
    )


def test_run_prepare_wrapper_calls_handler() -> None:
    result = _tool_success("prepare", "demo")
    captured: list[tuple[ToolResult, bool]] = []

    shared = SimpleNamespace()
    cfg = SimpleNamespace(logger=DummyLogger())

    out = run_prepare(
        "demo",
        cfg,
        Path("cfg.toml"),
        shared,
        None,
        learning_mode=True,
        result_handler=lambda res, lm: captured.append((res, lm)),
        hooks=_fake_hooks_with_result(result),
    )

    assert out.success is True
    assert captured == [(out, True)]


def test_run_train_wrapper_calls_handler() -> None:
    result = _tool_success("train", "demo")
    captured: list[tuple[ToolResult, bool]] = []
    shared = SimpleNamespace()
    cfg = SimpleNamespace(logger=DummyLogger(), runtime=_make_runtime())

    out = run_train(
        "demo",
        cfg,
        Path("cfg.toml"),
        shared,
        None,
        learning_mode=False,
        result_handler=lambda res, lm: captured.append((res, lm)),
        hooks=_fake_hooks_with_result(result),
    )

    assert out.success is True
    assert captured == [(out, False)]


def test_run_sample_wrapper_calls_handler() -> None:
    result = _tool_success("sample", "demo")
    captured: list[tuple[ToolResult, bool]] = []
    shared = SimpleNamespace()
    cfg = SimpleNamespace(logger=DummyLogger(), runtime=_make_runtime())

    out = run_sample(
        "demo",
        cfg,
        Path("cfg.toml"),
        shared,
        None,
        learning_mode=True,
        result_handler=lambda res, lm: captured.append((res, lm)),
        hooks=_fake_hooks_with_result(result),
    )

    assert out.success is True
    assert captured == [(out, True)]


# ---------------------------------------------------------------------------
# Command helpers
# ---------------------------------------------------------------------------


def _make_cli_dependencies(
    shared: SimpleNamespace, calls: dict[str, list[Any]]
) -> CLIDependencies:
    prepare_cfg = SimpleNamespace(logger=DummyLogger())
    train_cfg = SimpleNamespace(logger=DummyLogger(), runtime=_make_runtime())
    sample_cfg = SimpleNamespace(logger=DummyLogger(), runtime=_make_runtime())

    def load_experiment(name: str, exp_config: Path | None) -> Any:
        calls.setdefault("load", []).append((name, exp_config))
        return SimpleNamespace(
            prepare=prepare_cfg, train=train_cfg, sample=sample_cfg, shared=shared
        )

    def ensure_train(exp: Any) -> None:
        calls.setdefault("ensure_train", []).append(exp)

    def ensure_sample(exp: Any) -> None:
        calls.setdefault("ensure_sample", []).append(exp)

    def run_prepare_dep(
        name: str, cfg: Any, cfg_path: Path, shared_obj: Any, engine: Any
    ) -> ToolResult:
        calls.setdefault("run_prepare", []).append((name, cfg, cfg_path, engine))
        return _tool_success("prepare", name)

    def run_train_dep(
        name: str, cfg: Any, cfg_path: Path, shared_obj: Any, engine: Any
    ) -> ToolResult:
        calls.setdefault("run_train", []).append((name, cfg, cfg_path, engine))
        return _tool_success("train", name)

    def run_sample_dep(
        name: str, cfg: Any, cfg_path: Path, shared_obj: Any, engine: Any
    ) -> ToolResult:
        calls.setdefault("run_sample", []).append((name, cfg, cfg_path, engine))
        return _tool_success("sample", name)

    return CLIDependencies(
        load_experiment=load_experiment,
        ensure_train_prerequisites=ensure_train,
        ensure_sample_prerequisites=ensure_sample,
        run_prepare=run_prepare_dep,
        run_train=run_train_dep,
        run_sample=run_sample_dep,
    )


def test_run_train_cmd_invokes_dependencies(tmp_path: Path) -> None:
    shared = _make_shared(tmp_path)
    calls: dict[str, list[Any]] = {}
    deps = _make_cli_dependencies(shared, calls)

    captured: list[tuple[ToolResult, bool]] = []
    run_train_cmd(
        "demo",
        None,
        deps=deps,
        learning_engine=None,
        learning_mode=True,
        result_handler=lambda res, lm: captured.append((res, lm)),
    )

    assert calls["load"] == [("demo", None)]
    assert calls["ensure_train"]
    assert calls["run_train"]
    assert captured and captured[0][1] is True


def test_run_sample_cmd_invokes_dependencies(tmp_path: Path) -> None:
    shared = _make_shared(tmp_path)
    calls: dict[str, list[Any]] = {}
    deps = _make_cli_dependencies(shared, calls)

    captured: list[tuple[ToolResult, bool]] = []
    run_sample_cmd(
        "demo",
        None,
        deps=deps,
        learning_engine=None,
        learning_mode=False,
        result_handler=lambda res, lm: captured.append((res, lm)),
    )

    assert calls["load"] == [("demo", None)]
    assert calls["ensure_sample"]
    assert calls["run_sample"]
    assert captured and captured[0][1] is False


def test_extract_exp_config_variants(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    ctx_with_path = SimpleNamespace(obj={"exp_config": tmp_path})
    assert extract_exp_config(ctx_with_path) == tmp_path

    ctx_missing = SimpleNamespace(obj={})
    assert extract_exp_config(ctx_missing) is None

    ctx_wrong = SimpleNamespace(obj={"exp_config": "not-a-path"})
    with caplog.at_level(logging.DEBUG):
        assert extract_exp_config(ctx_wrong) is None
    assert "Unexpected exp_config value" in caplog.text


def test_run_analyze_supported(
    caplog: pytest.LogCaptureFixture, learning_engine: LearningModeEngine
) -> None:
    with caplog.at_level(logging.INFO, logger="ml_playground.runtime.cli"):
        result = run_analyze(
            "bundestag_char", "127.0.0.1", 9000, False, learning_engine
        )
    assert result.success is True
    assert "Analysis placeholder" in result.stdout
    assert result.learning_info is not None
    assert "Analysis for 'bundestag_char'" in caplog.text


def test_run_analyze_unsupported() -> None:
    result = run_analyze("other", "127.0.0.1", 9000, True, None)
    assert result.success is False
    assert "supports only" in result.stderr


def test_run_analyze_exception_path() -> None:
    def _raise_logger(_name: str) -> Any:
        raise RuntimeError("logger boom")

    result = runtime_runners.run_analyze(
        "bundestag_char",
        "127.0.0.1",
        9000,
        False,
        None,
        logger_factory=_raise_logger,
    )
    assert result.success is False
    assert "Analysis failed" in result.stderr


def test_global_options_invalid_config(tmp_path: Path) -> None:
    ctx = FakeTyperContext()
    missing = tmp_path / "missing.toml"
    messages: list[tuple[str, bool]] = []

    with pytest.raises(typer.Exit) as exc:
        runtime_cli.global_options(
            ctx,
            exp_config=missing,
            context_getter=lambda silent=True: SimpleNamespace(
                invoked_subcommand="prepare", get_help=lambda: ""
            ),
            echo_func=lambda msg, err=False: messages.append((msg, err)),
        )

    assert exc.value.exit_code == 2
    assert messages and messages[-1][1] is True


def test_global_options_no_subcommand(tmp_path: Path) -> None:
    ctx = FakeTyperContext()
    help_called: list[str] = []

    class FakeClickCtx:
        invoked_subcommand = None

        @staticmethod
        def get_help() -> str:
            help_called.append("help")
            return "help"

    messages: list[tuple[str, bool]] = []

    with pytest.raises(typer.Exit) as exc:
        runtime_cli.global_options(
            ctx,
            exp_config=None,
            learning_mode=True,
            verbosity=2,
            context_getter=lambda silent=True: FakeClickCtx(),
            echo_func=lambda msg, err=False: messages.append((msg, err)),
        )

    assert exc.value.exit_code == 0
    assert ctx.obj is not None and ctx.obj["learning_mode"] is True
    assert any("Welcome" in msg for msg, _ in messages)
    assert help_called


def test_global_options_sets_exp_config(tmp_path: Path) -> None:
    ctx = FakeTyperContext()
    config_path = tmp_path / "config.toml"
    config_path.write_text("[section]\nvalue=1\n")

    class FakeClickCtx:
        invoked_subcommand = "prepare"

    runtime_cli.global_options(
        ctx,
        exp_config=config_path,
        learning_mode=False,
        verbosity=1,
        context_getter=lambda silent=True: FakeClickCtx(),
        echo_func=lambda msg, err=False: None,
    )

    assert ctx.obj is not None
    assert ctx.obj["exp_config"] == config_path


def test_main_entry_handles_keyboard_interrupt() -> None:
    outputs: list[tuple[str, bool]] = []

    def _raise_keyboard() -> None:
        raise KeyboardInterrupt

    with pytest.raises(typer.Exit) as exc:
        runtime_cli.main_entry(
            app_runner=_raise_keyboard,
            echo=lambda msg, err=False: outputs.append((msg, err)),
        )

    assert exc.value.exit_code == 1
    assert any("Operation cancelled" in msg for msg, _ in outputs)


def test_main_entry_handles_generic_exception() -> None:
    outputs: list[tuple[str, bool]] = []

    def _raise_runtime() -> None:
        raise RuntimeError("boom")

    with pytest.raises(typer.Exit) as exc:
        runtime_cli.main_entry(
            app_runner=_raise_runtime,
            echo=lambda msg, err=False: outputs.append((msg, err)),
        )

    assert exc.value.exit_code == 1
    assert any("Runtime CLI execution failed" in msg for msg, _ in outputs)
