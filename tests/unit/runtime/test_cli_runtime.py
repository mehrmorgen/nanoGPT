# ruff: noqa: TID251
from __future__ import annotations

import logging
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
import typer

import ml_playground.runtime.cli as runtime_cli
from ml_playground.runtime.cli import (
    CLIDependencies,
    LearningModeEngine,
    VerbosityLevel,
    configure_cli_dependencies,
    default_cli_dependencies,
    extract_exp_config,
    get_cli_dependencies,
    global_device_setup,
    handle_tool_result,
    log_command_status,
    log_directory,
    override_cli_dependencies,
    reset_cli_dependencies,
    run_analyze,
    run_or_exit,
    run_prepare,
    run_prepare_impl,
    run_sample,
    run_sample_impl,
    run_sample_cmd,
    run_train,
    run_train_impl,
    run_train_cmd,
)
from ml_playground.runtime.core.results import LearningInfo, ToolResult


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

    with caplog.at_level(logging.INFO, logger="ml_playground.runtime.cli"):
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
# run_prepare_impl / run_train_impl / run_sample_impl
# ---------------------------------------------------------------------------


def test_run_prepare_impl_success(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, learning_engine: LearningModeEngine
) -> None:
    shared = _make_shared(tmp_path)
    logger = DummyLogger()
    cfg = SimpleNamespace(logger=logger)

    class Pipeline:
        def run(self) -> None:
            logger.info("pipeline run")

    monkeypatch.setattr(runtime_cli, "create_pipeline", lambda *_: Pipeline())

    result = run_prepare_impl("demo", cfg, shared.config_path, shared, learning_engine)
    assert result.success is True
    assert result.learning_info is not None
    assert logger.infos


def test_run_prepare_impl_success_without_learning_engine(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    shared = _make_shared(tmp_path)
    logger = DummyLogger()
    cfg = SimpleNamespace(logger=logger)

    class Pipeline:
        def run(self) -> None:
            logger.info("pipeline run")

    monkeypatch.setattr(runtime_cli, "create_pipeline", lambda *_: Pipeline())

    result = run_prepare_impl("demo", cfg, shared.config_path, shared, None)
    assert result.success is True
    assert result.learning_info is not None
    assert result.learning_info.explanations == []


def test_run_prepare_impl_calls_learning_engine_on_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    shared = _make_shared(tmp_path)
    logger = DummyLogger()
    cfg = SimpleNamespace(logger=logger)

    class Pipeline:
        def run(self) -> None:
            raise RuntimeError("bad pipeline")

    engine = RecordingLearningEngine()
    monkeypatch.setattr(runtime_cli, "create_pipeline", lambda *_: Pipeline())

    result = run_prepare_impl("demo", cfg, shared.config_path, shared, engine)
    assert not result.success
    assert engine.calls and engine.calls[0][0] == "demo"
    assert result.learning_info is not None
    assert result.learning_info.explanations == ["Explain demo"]


def test_run_prepare_impl_failure_logs_and_populates_learning(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, learning_engine: LearningModeEngine
) -> None:
    shared = _make_shared(tmp_path)
    logger = DummyLogger()
    cfg = SimpleNamespace(logger=logger)

    class Pipeline:
        def run(self) -> None:
            raise RuntimeError("bad pipeline")

    monkeypatch.setattr(runtime_cli, "create_pipeline", lambda *_: Pipeline())

    result = run_prepare_impl("demo", cfg, shared.config_path, shared, learning_engine)
    assert result.success is False
    assert "bad pipeline" in result.stderr
    assert result.learning_info is not None
    assert logger.errors


def test_run_sample_impl_missing_runtime(tmp_path: Path) -> None:
    shared = _make_shared(tmp_path)
    cfg = SimpleNamespace(logger=DummyLogger(), runtime=None)

    result = run_sample_impl("demo", cfg, shared.config_path, shared, None)
    assert result.success is False
    assert result.stderr == "Runtime configuration is missing for sampling."
    assert cfg.logger.errors


def test_run_train_impl_success(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, learning_engine: LearningModeEngine
) -> None:
    shared = _make_shared(tmp_path)
    logger = DummyLogger()
    runtime = _make_runtime(device="cuda", dtype="float16", seed=42)
    cfg = SimpleNamespace(logger=logger, runtime=runtime)

    trainer_calls: list[tuple[Any, Any]] = []

    class Trainer:
        def __init__(self, cfg_obj: Any, shared_obj: Any) -> None:
            trainer_calls.append((cfg_obj, shared_obj))

        def run(self) -> None:
            logger.info("trainer run")

    device_calls: list[tuple[str, str, int]] = []

    monkeypatch.setattr(runtime_cli, "CoreTrainer", Trainer)
    monkeypatch.setattr(
        runtime_cli,
        "global_device_setup",
        lambda d, t, s: device_calls.append((d, t, s)),
    )

    result = run_train_impl("demo", cfg, shared.config_path, shared, learning_engine)
    assert result.success is True
    assert trainer_calls and trainer_calls[0][0] is cfg
    assert device_calls == [("cuda", "float16", 42)]
    assert result.learning_info is not None


def test_run_train_impl_success_without_learning_engine(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    shared = _make_shared(tmp_path)
    logger = DummyLogger()
    runtime = _make_runtime()
    cfg = SimpleNamespace(logger=logger, runtime=runtime)

    class Trainer:
        def __init__(self, *_: Any) -> None:
            pass

        def run(self) -> None:
            logger.info("trainer")

    monkeypatch.setattr(runtime_cli, "CoreTrainer", Trainer)
    monkeypatch.setattr(runtime_cli, "global_device_setup", lambda *_: None)

    result = run_train_impl("demo", cfg, shared.config_path, shared, None)
    assert result.success is True
    assert result.learning_info is not None
    assert result.learning_info.explanations == []


def test_run_train_impl_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, learning_engine: LearningModeEngine
) -> None:
    shared = _make_shared(tmp_path)
    logger = DummyLogger()
    cfg = SimpleNamespace(logger=logger, runtime=_make_runtime())

    class Trainer:
        def __init__(self, *_: Any) -> None:
            pass

        def run(self) -> None:
            raise RuntimeError("trainer failed")

    monkeypatch.setattr(runtime_cli, "CoreTrainer", Trainer)
    monkeypatch.setattr(runtime_cli, "global_device_setup", lambda *_: None)

    result = run_train_impl("demo", cfg, shared.config_path, shared, learning_engine)
    assert result.success is False
    assert "trainer failed" in result.stderr
    assert result.learning_info is not None


def test_run_train_impl_calls_learning_engine_on_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    shared = _make_shared(tmp_path)
    logger = DummyLogger()
    cfg = SimpleNamespace(logger=logger, runtime=_make_runtime())

    class Trainer:
        def __init__(self, *_: Any) -> None:
            pass

        def run(self) -> None:
            raise RuntimeError("boom")

    engine = RecordingLearningEngine()
    monkeypatch.setattr(runtime_cli, "CoreTrainer", Trainer)
    monkeypatch.setattr(runtime_cli, "global_device_setup", lambda *_: None)

    result = run_train_impl("demo", cfg, shared.config_path, shared, engine)
    assert not result.success
    assert engine.calls and engine.calls[0][0] == "demo"
    assert result.learning_info is not None
    assert result.learning_info.explanations == ["Explain demo"]


def test_run_sample_impl_success(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, learning_engine: LearningModeEngine
) -> None:
    shared = _make_shared(tmp_path)
    logger = DummyLogger()
    cfg = SimpleNamespace(logger=logger, runtime=_make_runtime())

    class Sampler:
        def __init__(self, *_: Any) -> None:
            pass

        def run(self) -> None:
            logger.info("sampler run")

    monkeypatch.setattr(runtime_cli, "Sampler", Sampler)
    monkeypatch.setattr(runtime_cli, "global_device_setup", lambda *_: None)

    result = run_sample_impl("demo", cfg, shared.config_path, shared, learning_engine)
    assert result.success is True
    assert result.learning_info is not None


def test_run_sample_impl_success_without_learning_engine(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    shared = _make_shared(tmp_path)
    logger = DummyLogger()
    cfg = SimpleNamespace(logger=logger, runtime=_make_runtime())

    class Sampler:
        def __init__(self, *_: Any) -> None:
            pass

        def run(self) -> None:
            logger.info("sampler")

    monkeypatch.setattr(runtime_cli, "Sampler", Sampler)
    monkeypatch.setattr(runtime_cli, "global_device_setup", lambda *_: None)

    result = run_sample_impl("demo", cfg, shared.config_path, shared, None)
    assert result.success is True
    assert result.learning_info is not None
    assert result.learning_info.explanations == []


def test_run_sample_impl_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, learning_engine: LearningModeEngine
) -> None:
    shared = _make_shared(tmp_path)
    logger = DummyLogger()
    cfg = SimpleNamespace(logger=logger, runtime=_make_runtime())

    class Sampler:
        def __init__(self, *_: Any) -> None:
            pass

        def run(self) -> None:
            raise RuntimeError("sampler fail")

    monkeypatch.setattr(runtime_cli, "Sampler", Sampler)
    monkeypatch.setattr(runtime_cli, "global_device_setup", lambda *_: None)

    result = run_sample_impl("demo", cfg, shared.config_path, shared, learning_engine)
    assert result.success is False
    assert "sampler fail" in result.stderr
    assert result.learning_info is not None


def test_run_sample_impl_calls_learning_engine_on_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    shared = _make_shared(tmp_path)
    logger = DummyLogger()
    cfg = SimpleNamespace(logger=logger, runtime=_make_runtime())

    class Sampler:
        def __init__(self, *_: Any) -> None:
            pass

        def run(self) -> None:
            raise RuntimeError("boom")

    engine = RecordingLearningEngine()
    monkeypatch.setattr(runtime_cli, "Sampler", Sampler)
    monkeypatch.setattr(runtime_cli, "global_device_setup", lambda *_: None)

    result = run_sample_impl("demo", cfg, shared.config_path, shared, engine)
    assert not result.success
    assert engine.calls and engine.calls[0][0] == "demo"
    assert result.learning_info is not None
    assert result.learning_info.explanations == ["Explain demo"]


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


def test_run_prepare_wrapper_calls_handle(monkeypatch: pytest.MonkeyPatch) -> None:
    result = _tool_success("prepare", "demo")
    called: list[tuple[ToolResult, bool]] = []
    monkeypatch.setattr(runtime_cli, "run_prepare_impl", lambda *args, **_: result)
    monkeypatch.setattr(
        runtime_cli, "handle_tool_result", lambda res, lm: called.append((res, lm))
    )

    cfg = SimpleNamespace()
    shared = SimpleNamespace()
    out = run_prepare("demo", cfg, Path("cfg.toml"), shared, None, learning_mode=True)
    assert out is result
    assert called == [(result, True)]


def test_run_train_wrapper_calls_handle(monkeypatch: pytest.MonkeyPatch) -> None:
    result = _tool_success("train", "demo")
    called: list[tuple[ToolResult, bool]] = []
    monkeypatch.setattr(runtime_cli, "run_train_impl", lambda *args, **_: result)
    monkeypatch.setattr(
        runtime_cli, "handle_tool_result", lambda res, lm: called.append((res, lm))
    )

    cfg = SimpleNamespace()
    shared = SimpleNamespace()
    out = run_train("demo", cfg, Path("cfg.toml"), shared, None, learning_mode=False)
    assert out is result
    assert called == [(result, False)]


def test_run_sample_wrapper_calls_handle(monkeypatch: pytest.MonkeyPatch) -> None:
    result = _tool_success("sample", "demo")
    called: list[tuple[ToolResult, bool]] = []
    monkeypatch.setattr(runtime_cli, "run_sample_impl", lambda *args, **_: result)
    monkeypatch.setattr(
        runtime_cli, "handle_tool_result", lambda res, lm: called.append((res, lm))
    )

    cfg = SimpleNamespace()
    shared = SimpleNamespace()
    out = run_sample("demo", cfg, Path("cfg.toml"), shared, None, learning_mode=True)
    assert out is result
    assert called == [(result, True)]


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


def test_run_train_cmd_invokes_dependencies(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    shared = _make_shared(tmp_path)
    calls: dict[str, list[Any]] = {}
    deps = _make_cli_dependencies(shared, calls)

    captured: list[tuple[ToolResult, bool]] = []
    monkeypatch.setattr(
        runtime_cli, "handle_tool_result", lambda res, lm: captured.append((res, lm))
    )

    run_train_cmd("demo", None, deps, learning_engine=None, learning_mode=True)

    assert calls["load"]
    assert calls["ensure_train"]
    assert calls["run_train"]
    assert captured and captured[0][1] is True


def test_run_sample_cmd_invokes_dependencies(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    shared = _make_shared(tmp_path)
    calls: dict[str, list[Any]] = {}
    deps = _make_cli_dependencies(shared, calls)

    captured: list[tuple[ToolResult, bool]] = []
    monkeypatch.setattr(
        runtime_cli, "handle_tool_result", lambda res, lm: captured.append((res, lm))
    )

    run_sample_cmd("demo", None, deps, learning_engine=None, learning_mode=False)

    assert calls["load"]
    assert calls["ensure_sample"]
    assert calls["run_sample"]
    assert captured and captured[0][1] is False


def test_prepare_command_flow(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    shared = _make_shared(tmp_path)
    calls: dict[str, list[Any]] = {}
    deps = _make_cli_dependencies(shared, calls)

    monkeypatch.setattr(runtime_cli, "get_cli_dependencies", lambda: deps)
    monkeypatch.setattr(runtime_cli, "run_or_exit", lambda func, **_: func())

    captured: list[tuple[ToolResult, bool]] = []
    monkeypatch.setattr(
        runtime_cli, "handle_tool_result", lambda res, lm: captured.append((res, lm))
    )

    ctx = SimpleNamespace(
        obj={"learning_mode": True, "verbosity": VerbosityLevel.COMPREHENSIVE}
    )
    runtime_cli.prepare(ctx, "demo")

    assert calls["run_prepare"]
    assert captured and captured[0][1] is True


def test_train_command_flow(monkeypatch: pytest.MonkeyPatch) -> None:
    invoked: list[tuple[str, Path | None, bool]] = []
    monkeypatch.setattr(runtime_cli, "run_or_exit", lambda func, **_: func())
    monkeypatch.setattr(
        runtime_cli,
        "run_train_cmd",
        lambda name, exp_cfg, deps, engine, learning_mode: invoked.append(
            (name, exp_cfg, learning_mode)
        ),
    )

    ctx = SimpleNamespace(obj={"learning_mode": False})
    runtime_cli.train(ctx, "demo")

    assert invoked == [("demo", None, False)]


def test_sample_command_flow(monkeypatch: pytest.MonkeyPatch) -> None:
    invoked: list[tuple[str, Path | None, bool]] = []
    monkeypatch.setattr(runtime_cli, "run_or_exit", lambda func, **_: func())
    monkeypatch.setattr(
        runtime_cli,
        "run_sample_cmd",
        lambda name, exp_cfg, deps, engine, learning_mode: invoked.append(
            (name, exp_cfg, learning_mode)
        ),
    )

    ctx = SimpleNamespace(obj={"learning_mode": False})
    runtime_cli.sample(ctx, "demo")

    assert invoked == [("demo", None, False)]


def test_analyze_command_flow(monkeypatch: pytest.MonkeyPatch) -> None:
    result = _tool_success("analyze", "demo")
    outputs: list[tuple[ToolResult, bool]] = []
    monkeypatch.setattr(runtime_cli, "run_analyze", lambda *args, **kwargs: result)
    monkeypatch.setattr(
        runtime_cli, "handle_tool_result", lambda res, lm: outputs.append((res, lm))
    )

    ctx = SimpleNamespace(
        obj={"learning_mode": True, "verbosity": VerbosityLevel.MINIMAL}
    )
    runtime_cli.analyze(ctx, "demo", host="0.0.0.0", port=1234, open_browser=False)

    assert outputs == [(result, True)]


# ---------------------------------------------------------------------------
# Dependency configuration helpers
# ---------------------------------------------------------------------------


def test_configure_and_reset_dependencies(tmp_path: Path) -> None:
    factory_calls = []

    def factory() -> CLIDependencies:
        factory_calls.append("factory")
        return _make_cli_dependencies(_make_shared(tmp_path), {})

    configure_cli_dependencies(factory)
    first = get_cli_dependencies()
    reset_cli_dependencies()
    second = get_cli_dependencies()
    assert first is not second
    assert factory_calls

    configure_cli_dependencies(default_cli_dependencies)


def test_override_cli_dependencies_restores(tmp_path: Path) -> None:
    _original = get_cli_dependencies()
    deps = _make_cli_dependencies(_make_shared(tmp_path), {})

    with override_cli_dependencies(deps):
        assert get_cli_dependencies() is deps
    assert get_cli_dependencies() is not deps
    assert get_cli_dependencies() is not None


# ---------------------------------------------------------------------------
# extract_exp_config and run_analyze helpers
# ---------------------------------------------------------------------------


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


def test_run_analyze_exception_path(monkeypatch: pytest.MonkeyPatch) -> None:
    original = runtime_cli.logging.getLogger

    def _raise_logger(name: str | None = None, *args: Any, **kwargs: Any) -> Any:
        if name == runtime_cli.__name__:
            raise RuntimeError("logger boom")
        if name is None:
            return original(*args, **kwargs)
        return original(name, *args, **kwargs)

    monkeypatch.setattr(runtime_cli.logging, "getLogger", _raise_logger)

    result = run_analyze("bundestag_char", "127.0.0.1", 9000, False, None)
    assert result.success is False
    assert "Analysis failed" in result.stderr


def test_global_options_invalid_config(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    ctx = FakeTyperContext()
    missing = tmp_path / "missing.toml"
    messages: list[tuple[str, bool]] = []

    monkeypatch.setattr(
        runtime_cli.click,
        "get_current_context",
        lambda silent=True: SimpleNamespace(
            invoked_subcommand="prepare", get_help=lambda: ""
        ),
    )
    monkeypatch.setattr(
        runtime_cli.typer, "echo", lambda msg, err=False: messages.append((msg, err))
    )

    with pytest.raises(typer.Exit) as exc:
        runtime_cli.global_options(ctx, exp_config=missing)

    assert exc.value.exit_code == 2
    assert messages and messages[-1][1] is True


def test_global_options_no_subcommand(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    ctx = FakeTyperContext()
    help_called: list[str] = []

    class FakeClickCtx:
        invoked_subcommand = None

        @staticmethod
        def get_help() -> str:
            help_called.append("help")
            return "help"

    messages: list[tuple[str, bool]] = []

    monkeypatch.setattr(
        runtime_cli.click, "get_current_context", lambda silent=True: FakeClickCtx()
    )
    monkeypatch.setattr(
        runtime_cli.typer, "echo", lambda msg, err=False: messages.append((msg, err))
    )

    with pytest.raises(typer.Exit) as exc:
        runtime_cli.global_options(
            ctx, exp_config=None, learning_mode=True, verbosity=2
        )

    assert exc.value.exit_code == 0
    assert ctx.obj is not None and ctx.obj["learning_mode"] is True
    assert any("Welcome" in msg for msg, _ in messages)
    assert help_called


def test_global_options_sets_exp_config(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    ctx = FakeTyperContext()
    config_path = tmp_path / "config.toml"
    config_path.write_text("[section]\nvalue=1\n")

    class FakeClickCtx:
        invoked_subcommand = "prepare"

    monkeypatch.setattr(
        runtime_cli.click, "get_current_context", lambda silent=True: FakeClickCtx()
    )

    runtime_cli.global_options(
        ctx, exp_config=config_path, learning_mode=False, verbosity=1
    )

    assert ctx.obj is not None
    assert ctx.obj["exp_config"] == config_path


def test_main_entry_handles_keyboard_interrupt(monkeypatch: pytest.MonkeyPatch) -> None:
    original_app = runtime_cli.app

    class RaisingApp:
        def __call__(self) -> None:
            raise KeyboardInterrupt

    monkeypatch.setattr(runtime_cli, "app", RaisingApp())
    outputs: list[tuple[str, bool]] = []
    monkeypatch.setattr(
        runtime_cli.typer, "echo", lambda msg, err=False: outputs.append((msg, err))
    )

    with pytest.raises(typer.Exit) as exc:
        runtime_cli.main_entry()

    assert exc.value.exit_code == 1
    assert any("Operation cancelled" in msg for msg, _ in outputs)
    monkeypatch.setattr(runtime_cli, "app", original_app)


def test_main_entry_handles_generic_exception(monkeypatch: pytest.MonkeyPatch) -> None:
    original_app = runtime_cli.app

    class RaisingApp:
        def __call__(self) -> None:
            raise RuntimeError("boom")

    monkeypatch.setattr(runtime_cli, "app", RaisingApp())
    outputs: list[tuple[str, bool]] = []
    monkeypatch.setattr(
        runtime_cli.typer, "echo", lambda msg, err=False: outputs.append((msg, err))
    )

    with pytest.raises(typer.Exit) as exc:
        runtime_cli.main_entry()

    assert exc.value.exit_code == 1
    assert any("Runtime CLI execution failed" in msg for msg, _ in outputs)
    monkeypatch.setattr(runtime_cli, "app", original_app)
