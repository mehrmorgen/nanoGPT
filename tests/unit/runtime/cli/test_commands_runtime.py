from __future__ import annotations

import importlib
import logging
import sys
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast

import pytest
import typer
from typer.testing import CliRunner

from ml_playground.configuration.models import (
    PreparerConfig,
    SamplerConfig,
    TrainerConfig,
)
from ml_playground.runtime import helpers
from ml_playground.runtime.cli import commands, device, runners, typer_helpers
from ml_playground.runtime.core import bootstrap
from ml_playground.runtime.core.results import (
    LearningInfo,
    ToolResult,
    LearningModeEngine,
    VerbosityLevel,
)
from ml_playground.runtime.protocols import SharedConfigLike


class _Logger:
    def __init__(self) -> None:
        self.infos: list[str] = []
        self.errors: list[str] = []
        self.warnings: list[str] = []
        self.debugs: list[str] = []

    def info(self, msg: str, *args: object) -> None:
        self.infos.append(msg % args if args else msg)

    def error(self, msg: str, *args: object) -> None:
        self.errors.append(msg % args if args else msg)

    def warning(self, msg: str, *args: object) -> None:
        self.warnings.append(msg % args if args else msg)

    def debug(self, msg: str, *args: object) -> None:
        self.debugs.append(msg % args if args else msg)


def _runtime() -> SimpleNamespace:
    return SimpleNamespace(device="cpu", dtype="float32", seed=0)


def _shared(tmp_path: Path) -> SharedConfigLike:
    return cast(
        SharedConfigLike,
        SimpleNamespace(
            dataset_dir=tmp_path / "data",
            train_out_dir=tmp_path / "train",
            sample_out_dir=tmp_path / "sample",
        ),
    )


def _stub_cli(**kwargs: Any) -> SimpleNamespace:
    def create_pipeline(cfg: Any, shared: Any) -> Any:  # noqa: ARG001
        return SimpleNamespace(run=lambda: None)

    def global_device_setup(device: str, dtype: str, seed: int) -> None:  # noqa: ARG001
        return None

    def log_command_status(tag: str, shared: Any, out_dir: Any, logger: Any) -> None:  # noqa: ARG001
        return None

    def core_trainer(cfg: Any, shared: Any) -> Any:  # noqa: ARG001
        return SimpleNamespace(run=lambda: None)

    def sampler(cfg: Any, shared: Any) -> Any:  # noqa: ARG001
        return SimpleNamespace(run=lambda: None)

    def run_cmd(*args: Any, **kwargs: Any) -> None:  # noqa: ARG001
        return None

    defaults: dict[str, Any] = {
        "__name__": "ml_playground.runtime.cli",
        "create_pipeline": create_pipeline,
        "global_device_setup": global_device_setup,
        "log_command_status": log_command_status,
        "CoreTrainer": core_trainer,
        "Sampler": sampler,
        "logging": logging,
        "run_prepare_cmd": run_cmd,
        "run_train_cmd": run_cmd,
        "run_sample_cmd": run_cmd,
        "get_cli_dependencies": lambda: SimpleNamespace(),
        "run_or_exit": typer_helpers.run_or_exit,
    }
    defaults.update(kwargs)
    return SimpleNamespace(**defaults)


def test_cli_module_imports_when_missing(monkeypatch: Any) -> None:
    monkeypatch.delitem(sys.modules, "ml_playground.runtime.cli", raising=False)
    sentinel = SimpleNamespace(marker="loaded")

    def fake_import(name: str) -> SimpleNamespace:  # noqa: ARG001
        return sentinel

    monkeypatch.setattr(commands.importlib, "import_module", fake_import)
    mod = runners._cli_module()  # pyright: ignore[reportPrivateUsage]
    assert mod is sentinel


def test_commands_cli_module_imports_when_missing(monkeypatch: Any) -> None:
    monkeypatch.delitem(sys.modules, "ml_playground.runtime.cli", raising=False)
    sentinel = SimpleNamespace(marker="loaded-commands")

    def fake_import(name: str) -> SimpleNamespace:  # noqa: ARG001
        return sentinel

    monkeypatch.setattr(commands.importlib, "import_module", fake_import)
    mod = commands._cli_module()  # pyright: ignore[reportPrivateUsage]
    assert mod is sentinel


def test_run_prepare_impl_success(monkeypatch: Any, tmp_path: Path) -> None:
    called: list[str] = []

    def create_pipeline(cfg: PreparerConfig, shared: SharedConfigLike) -> Any:
        return SimpleNamespace(run=lambda: called.append("run"))

    cli_stub = _stub_cli(create_pipeline=create_pipeline)
    monkeypatch.setitem(sys.modules, "ml_playground.runtime.cli", cli_stub)

    logger = _Logger()
    cfg: PreparerConfig = cast(PreparerConfig, SimpleNamespace(logger=logger))
    shared: SharedConfigLike = _shared(tmp_path)

    result = commands.run_prepare_impl("exp", cfg, Path("config"), shared)

    assert result.success is True
    assert "run" in called


def test_run_prepare_impl_failure(monkeypatch: Any, tmp_path: Path) -> None:
    def create_pipeline(cfg: PreparerConfig, shared: SharedConfigLike) -> Any:
        return SimpleNamespace(run=lambda: (_ for _ in ()).throw(RuntimeError("boom")))

    cli_stub = _stub_cli(create_pipeline=create_pipeline)
    monkeypatch.setitem(sys.modules, "ml_playground.runtime.cli", cli_stub)

    logger = _Logger()
    cfg: PreparerConfig = cast(PreparerConfig, SimpleNamespace(logger=logger))
    shared: SharedConfigLike = _shared(tmp_path)

    result = commands.run_prepare_impl("exp", cfg, Path("config"), shared)

    assert result.success is False
    assert "Pipeline preparation failed" in result.stderr


def test_run_prepare_impl_learning(monkeypatch: Any, tmp_path: Path) -> None:
    class DummyEngine(LearningModeEngine):
        def __init__(self) -> None:
            super().__init__(verbosity=VerbosityLevel.MINIMAL)

        def explain_command(self, **_: object) -> LearningInfo:  # type: ignore[override]
            return LearningInfo(
                explanations=["prep"], best_practices=[], related_concepts=[]
            )

    def create_pipeline(
        cfg: PreparerConfig, shared: SharedConfigLike
    ) -> SimpleNamespace:
        return SimpleNamespace(run=lambda: None)

    cli_stub = _stub_cli(create_pipeline=create_pipeline)
    monkeypatch.setitem(sys.modules, "ml_playground.runtime.cli", cli_stub)

    logger = _Logger()
    cfg: PreparerConfig = cast(PreparerConfig, SimpleNamespace(logger=logger))
    shared: SharedConfigLike = _shared(tmp_path)

    result = commands.run_prepare_impl(
        "exp", cfg, Path("config"), shared, DummyEngine()
    )

    assert result.success is True
    assert result.learning_info is not None
    assert result.learning_info.explanations == ["prep"]


def test_run_prepare_impl_failure_with_learning(
    monkeypatch: Any, tmp_path: Path
) -> None:
    class DummyEngine(LearningModeEngine):
        def __init__(self) -> None:
            super().__init__(verbosity=VerbosityLevel.MINIMAL)

        def explain_command(self, **_: object) -> LearningInfo:  # type: ignore[override]
            return LearningInfo(
                explanations=["prep-fail"], best_practices=[], related_concepts=[]
            )

    def create_pipeline(cfg: PreparerConfig, shared: SharedConfigLike) -> Any:
        return SimpleNamespace(run=lambda: (_ for _ in ()).throw(RuntimeError("boom")))

    cli_stub = _stub_cli(create_pipeline=create_pipeline)
    monkeypatch.setitem(sys.modules, "ml_playground.runtime.cli", cli_stub)

    logger = _Logger()
    cfg: PreparerConfig = cast(PreparerConfig, SimpleNamespace(logger=logger))
    shared: SharedConfigLike = _shared(tmp_path)

    result = commands.run_prepare_impl(
        "exp", cfg, Path("config"), shared, DummyEngine()
    )

    assert result.success is False
    assert result.learning_info is not None
    assert result.learning_info.explanations == ["prep-fail"]


def test_run_train_impl_missing_runtime(monkeypatch: Any, tmp_path: Path) -> None:
    cli_stub = _stub_cli()
    monkeypatch.setitem(sys.modules, "ml_playground.runtime.cli", cli_stub)

    logger = _Logger()
    cfg: TrainerConfig = cast(
        TrainerConfig, SimpleNamespace(runtime=None, logger=logger)
    )
    shared: SharedConfigLike = _shared(tmp_path)

    result = commands.run_train_impl("exp", cfg, Path("config"), shared)
    assert result.success is False


def test_run_train_impl_failure_with_learning(monkeypatch: Any, tmp_path: Path) -> None:
    class DummyEngine(LearningModeEngine):
        def __init__(self) -> None:
            super().__init__(verbosity=VerbosityLevel.MINIMAL)

        def explain_command(self, **_: object) -> LearningInfo:  # type: ignore[override]
            return LearningInfo(
                explanations=["train-fail"], best_practices=[], related_concepts=[]
            )

    def trainer_factory(cfg: TrainerConfig, shared: SharedConfigLike) -> Any:  # type: ignore[type-arg]
        return SimpleNamespace(run=lambda: (_ for _ in ()).throw(RuntimeError("fail")))

    cli_stub = _stub_cli(CoreTrainer=trainer_factory)
    monkeypatch.setitem(sys.modules, "ml_playground.runtime.cli", cli_stub)

    logger = _Logger()
    cfg: TrainerConfig = cast(
        TrainerConfig, SimpleNamespace(runtime=_runtime(), logger=logger)
    )
    shared: SharedConfigLike = _shared(tmp_path)

    result = commands.run_train_impl("exp", cfg, Path("config"), shared, DummyEngine())

    assert result.success is False
    assert result.learning_info is not None
    assert result.learning_info.explanations == ["train-fail"]


def test_run_train_impl_learning(monkeypatch: Any, tmp_path: Path) -> None:
    class DummyEngine(LearningModeEngine):
        def __init__(self) -> None:
            super().__init__(verbosity=VerbosityLevel.MINIMAL)

        def explain_command(self, **_: object) -> LearningInfo:  # type: ignore[override]
            return LearningInfo(
                explanations=["train"], best_practices=[], related_concepts=[]
            )

    def trainer_factory(cfg: TrainerConfig, shared: SharedConfigLike) -> Any:  # type: ignore[type-arg]
        return SimpleNamespace(run=lambda: None)

    cli_stub = _stub_cli(CoreTrainer=trainer_factory)
    monkeypatch.setitem(sys.modules, "ml_playground.runtime.cli", cli_stub)

    logger = _Logger()
    cfg: TrainerConfig = cast(
        TrainerConfig, SimpleNamespace(runtime=_runtime(), logger=logger)
    )
    shared: SharedConfigLike = _shared(tmp_path)

    result = commands.run_train_impl("exp", cfg, Path("config"), shared, DummyEngine())

    assert result.success is True
    assert result.learning_info is not None
    assert result.learning_info.explanations == ["train"]


def test_run_train_impl_import_failure_with_learning(monkeypatch: Any) -> None:
    class DummyEngine(LearningModeEngine):
        def __init__(self) -> None:
            super().__init__(verbosity=VerbosityLevel.MINIMAL)

        def explain_command(self, **_: object) -> LearningInfo:  # type: ignore[override]
            return LearningInfo(
                explanations=["train-import"], best_practices=[], related_concepts=[]
            )

    cfg: TrainerConfig = cast(
        TrainerConfig, SimpleNamespace(runtime=_runtime(), logger=_Logger())
    )
    shared: SharedConfigLike = cast(SharedConfigLike, SimpleNamespace())

    def bad_cli_module() -> Any:
        raise RuntimeError("boom")

    monkeypatch.setattr(commands, "_cli_module", bad_cli_module)

    result = commands.run_train_impl("exp", cfg, Path("config"), shared, DummyEngine())

    assert result.success is False
    assert result.learning_info is not None
    assert result.learning_info.explanations == ["train-import"]


def test_run_train_impl_import_failure_without_learning(monkeypatch: Any) -> None:
    cfg: TrainerConfig = cast(
        TrainerConfig, SimpleNamespace(runtime=_runtime(), logger=_Logger())
    )
    shared: SharedConfigLike = cast(SharedConfigLike, SimpleNamespace())

    def bad_cli_module() -> Any:
        raise RuntimeError("boom")

    monkeypatch.setattr(commands, "_cli_module", bad_cli_module)

    result = commands.run_train_impl("exp", cfg, Path("config"), shared)

    assert result.success is False
    assert result.learning_info is not None
    assert result.learning_info.explanations == []


def test_run_sample_impl_missing_runtime(monkeypatch: Any, tmp_path: Path) -> None:
    cli_stub = _stub_cli()
    monkeypatch.setitem(sys.modules, "ml_playground.runtime.cli", cli_stub)

    logger = _Logger()
    cfg: SamplerConfig = cast(
        SamplerConfig, SimpleNamespace(runtime=None, logger=logger)
    )
    shared: SharedConfigLike = _shared(tmp_path)

    result = commands.run_sample_impl("exp", cfg, Path("config"), shared)

    assert result.success is False
    assert "Runtime configuration is missing for sampling." in result.stderr


def test_run_sample_impl_success(monkeypatch: Any, tmp_path: Path) -> None:
    calls: list[str] = []

    def sampler_factory(cfg: SamplerConfig, shared: SharedConfigLike) -> Any:  # type: ignore[type-arg]
        return SimpleNamespace(run=lambda: calls.append("run"))

    def log_command_status(
        tag: str, shared: SharedConfigLike, out_dir: Path | None, logger: Any
    ) -> None:
        calls.append(f"log-{tag}")

    cli_stub = _stub_cli(Sampler=sampler_factory, log_command_status=log_command_status)
    monkeypatch.setitem(sys.modules, "ml_playground.runtime.cli", cli_stub)

    logger = _Logger()
    cfg: SamplerConfig = cast(
        SamplerConfig, SimpleNamespace(runtime=_runtime(), logger=logger)
    )
    shared: SharedConfigLike = _shared(tmp_path)

    result = commands.run_sample_impl("exp", cfg, Path("config"), shared)

    assert result.success is True
    assert "run" in calls
    assert any(call.startswith("log-") for call in calls)


def test_run_sample_impl_failure(monkeypatch: Any, tmp_path: Path) -> None:
    def sampler_factory(cfg: SamplerConfig, shared: SharedConfigLike) -> Any:  # type: ignore[type-arg]
        return SimpleNamespace(run=lambda: (_ for _ in ()).throw(RuntimeError("boom")))

    cli_stub = _stub_cli(Sampler=sampler_factory)
    monkeypatch.setitem(sys.modules, "ml_playground.runtime.cli", cli_stub)

    logger = _Logger()
    cfg: SamplerConfig = cast(
        SamplerConfig, SimpleNamespace(runtime=_runtime(), logger=logger)
    )
    shared: SharedConfigLike = _shared(tmp_path)

    result = commands.run_sample_impl("exp", cfg, Path("config"), shared)

    assert result.success is False
    assert "Sampling failed" in result.stderr


def test_run_sample_impl_failure_with_learning(
    monkeypatch: Any, tmp_path: Path
) -> None:
    class DummyEngine(LearningModeEngine):
        def __init__(self) -> None:
            super().__init__(verbosity=VerbosityLevel.MINIMAL)

        def explain_command(self, **_: object) -> LearningInfo:  # type: ignore[override]
            return LearningInfo(
                explanations=["sample-fail"], best_practices=[], related_concepts=[]
            )

    def sampler_factory(cfg: SamplerConfig, shared: SharedConfigLike) -> Any:  # type: ignore[type-arg]
        return SimpleNamespace(run=lambda: (_ for _ in ()).throw(RuntimeError("boom")))

    cli_stub = _stub_cli(Sampler=sampler_factory)
    monkeypatch.setitem(sys.modules, "ml_playground.runtime.cli", cli_stub)

    logger = _Logger()
    cfg: SamplerConfig = cast(
        SamplerConfig, SimpleNamespace(runtime=_runtime(), logger=logger)
    )
    shared: SharedConfigLike = _shared(tmp_path)

    result = commands.run_sample_impl("exp", cfg, Path("config"), shared, DummyEngine())

    assert result.success is False
    assert result.learning_info is not None
    assert result.learning_info.explanations == ["sample-fail"]


def test_run_sample_impl_import_failure_with_learning(monkeypatch: Any) -> None:
    class DummyEngine(LearningModeEngine):
        def __init__(self) -> None:
            super().__init__(verbosity=VerbosityLevel.MINIMAL)

        def explain_command(self, **_: object) -> LearningInfo:  # type: ignore[override]
            return LearningInfo(
                explanations=["sample-import"], best_practices=[], related_concepts=[]
            )

    cfg: SamplerConfig = cast(
        SamplerConfig, SimpleNamespace(runtime=_runtime(), logger=_Logger())
    )
    shared: SharedConfigLike = cast(SharedConfigLike, SimpleNamespace())

    def bad_cli_module() -> Any:
        raise RuntimeError("boom")

    monkeypatch.setattr(commands, "_cli_module", bad_cli_module)

    result = commands.run_sample_impl("exp", cfg, Path("config"), shared, DummyEngine())

    assert result.success is False
    assert result.learning_info is not None
    assert result.learning_info.explanations == ["sample-import"]


def test_missing_runtime_message_default_branch() -> None:
    missing_runtime_message = commands.__dict__["_missing_runtime_message"]
    assert missing_runtime_message("other") == "Runtime configuration is missing."


def test_log_directory_handles_os_error(monkeypatch: Any, tmp_path: Path) -> None:
    path = tmp_path / "dir"
    path.mkdir()
    logger = _Logger()

    def bad_iterdir(self: Path) -> Any:
        raise OSError("boom")

    monkeypatch.setattr(Path, "iterdir", bad_iterdir)
    commands.log_directory("tag", "dir", path, logger)
    assert any("exists" in msg for msg in logger.infos)


def test_run_sample_impl_learning(monkeypatch: Any, tmp_path: Path) -> None:
    class DummyEngine(LearningModeEngine):
        def __init__(self) -> None:
            super().__init__(verbosity=VerbosityLevel.MINIMAL)

        def explain_command(self, **_: object) -> LearningInfo:  # type: ignore[override]
            return LearningInfo(
                explanations=["sample"], best_practices=[], related_concepts=[]
            )

    def sampler_factory(cfg: SamplerConfig, shared: SharedConfigLike) -> Any:  # type: ignore[type-arg]
        return SimpleNamespace(run=lambda: None)

    cli_stub = _stub_cli(Sampler=sampler_factory)
    monkeypatch.setitem(sys.modules, "ml_playground.runtime.cli", cli_stub)

    logger = _Logger()
    cfg: SamplerConfig = cast(
        SamplerConfig, SimpleNamespace(runtime=_runtime(), logger=logger)
    )
    shared: SharedConfigLike = _shared(tmp_path)

    result = commands.run_sample_impl("exp", cfg, Path("config"), shared, DummyEngine())

    assert result.success is True
    assert result.learning_info is not None
    assert result.learning_info.explanations == ["sample"]


def test_run_analyze_logging_failure(monkeypatch: Any) -> None:
    cli_stub = _stub_cli(logging=None)
    monkeypatch.setitem(sys.modules, "ml_playground.runtime.cli", cli_stub)
    result = commands.run_analyze("bundestag_char", "127.0.0.1", 8050, True, None)
    assert result.success is False
    assert "logging unavailable" in result.stderr


def test_run_analyze_success_logs(
    monkeypatch: Any, caplog: pytest.LogCaptureFixture
) -> None:
    cli_stub = _stub_cli()
    monkeypatch.setitem(sys.modules, "ml_playground.runtime.cli", cli_stub)
    with caplog.at_level(logging.INFO):
        result = commands.run_analyze("bundestag_char", "127.0.0.1", 8050, True, None)
    assert result.success is True
    assert "Analysis placeholder executed" in result.stdout
    assert any(
        "Analysis for 'bundestag_char' not implemented" in line
        for line in caplog.text.splitlines()
    )


def test_run_analyze_learning_info(monkeypatch: Any) -> None:
    class DummyEngine(LearningModeEngine):
        def __init__(self) -> None:
            super().__init__(verbosity=VerbosityLevel.MINIMAL)

        def explain_command(self, **_: object) -> LearningInfo:  # type: ignore[override]
            return LearningInfo(
                explanations=["analyze"], best_practices=[], related_concepts=[]
            )

    cli_stub = _stub_cli()
    monkeypatch.setitem(sys.modules, "ml_playground.runtime.cli", cli_stub)
    result = commands.run_analyze(
        "bundestag_char", "127.0.0.1", 8050, True, DummyEngine()
    )
    assert result.success is True
    assert result.learning_info is not None
    assert result.learning_info.explanations == ["analyze"]


def test_cli_device_delegates_without_error() -> None:
    # Should not raise even when using real runtime.device
    device.global_device_setup("cpu", "float32", 0)


def test_helpers_handle_tool_result_learning_sections(monkeypatch: Any) -> None:
    messages: list[str] = []

    def fake_echo(msg: Any, err: bool = False) -> None:  # noqa: ARG001
        messages.append(str(msg))

    monkeypatch.setattr(helpers.typer, "echo", fake_echo)

    learning_info = LearningInfo(
        explanations=["e1"],
        best_practices=["bp1"],
        related_concepts=["rc1"],
    )
    result = ToolResult.create(
        success=True,
        exit_code=0,
        namespace="ml",
        category="prepare",
        command="cmd",
        stdout="out",
        stderr="err",
        learning_info=learning_info,
    )

    helpers.handle_tool_result(result, learning_mode=True)

    assert any("e1" in m for m in messages)
    assert any("bp1" in m for m in messages)
    assert any("rc1" in m for m in messages)


def test_helpers_handle_tool_result_failure_exits(monkeypatch: Any) -> None:
    def fake_echo(msg: Any, err: bool = False) -> None:  # noqa: ARG001
        return None

    monkeypatch.setattr(helpers.typer, "echo", fake_echo)
    result = ToolResult.create(
        success=False,
        exit_code=5,
        namespace="ml",
        category="prepare",
        command="cmd",
    )
    with pytest.raises(typer.Exit) as excinfo:
        helpers.handle_tool_result(result, learning_mode=False)
    assert excinfo.value.exit_code == 5


def test_helpers_log_directory_with_contents(tmp_path: Path) -> None:
    logger = _Logger()
    sub = tmp_path / "file.txt"
    sub.write_text("x", encoding="utf-8")
    helpers.log_directory("tag", "dir", tmp_path, logger)
    assert any("Contents" in msg for msg in logger.infos)


def test_device_defensive_catch(monkeypatch: Any) -> None:
    class BadDevice:
        def global_device_setup(self, *args: object, **kwargs: object) -> None:  # noqa: ARG001
            raise RuntimeError("fail")

    def fake_import(name: str) -> Any:  # noqa: ARG001
        return BadDevice()

    monkeypatch.setattr(device.importlib, "import_module", fake_import)
    device.global_device_setup("cpu", "float32", 0)  # should not raise


def test_typer_helpers_run_or_exit_file_not_found() -> None:
    with pytest.raises(typer.Exit) as excinfo:
        typer_helpers.run_or_exit(
            lambda: (_ for _ in ()).throw(FileNotFoundError("missing"))
        )
    assert excinfo.value.exit_code == 1


def test_typer_helpers_run_or_exit_click_exit() -> None:
    with pytest.raises(typer.Exit) as excinfo:
        typer_helpers.run_or_exit(lambda: (_ for _ in ()).throw(typer.Exit(3)))
    assert excinfo.value.exit_code == 3


def test_typer_helpers_run_or_exit_keyboard_interrupt(
    caplog: pytest.LogCaptureFixture,
) -> None:
    with caplog.at_level(logging.INFO):
        typer_helpers.run_or_exit(
            lambda: (_ for _ in ()).throw(KeyboardInterrupt),
            keyboard_interrupt_msg="cancel",
        )
    assert "cancel" in caplog.text


def test_bootstrap_guard_raises_when_unconfigured(monkeypatch: Any) -> None:
    monkeypatch.setattr(bootstrap, "_default_factory", None)
    monkeypatch.setattr(bootstrap, "_current", None)
    with pytest.raises(RuntimeError):
        bootstrap.get_runtime_cli_dependencies()


def test_main_global_options_missing_config(tmp_path: Path) -> None:
    runner = CliRunner()
    missing = tmp_path / "nope.toml"
    cli_mod = importlib.import_module("ml_playground.runtime.cli.main")
    result = runner.invoke(cli_mod.app, ["--exp-config", str(missing)])
    assert result.exit_code == 2
    assert "Config file not found" in result.output or "Error" in result.output


def test_main_global_options_handles_bad_ctx(monkeypatch: Any) -> None:
    class BadCtx:
        obj: Any = None

        def ensure_object(self, arg: Any) -> None:  # noqa: ARG001
            raise AttributeError("boom")

    ctx = BadCtx()
    cli_mod = importlib.import_module("ml_playground.runtime.cli.main")
    result = cli_mod.global_options(ctx, None, False, 1)
    assert result is None


def test_main_global_options_invoked_subcommand_none(monkeypatch: Any) -> None:
    cli_mod = importlib.import_module("ml_playground.runtime.cli.main")

    class DummyCtx:
        def __init__(self) -> None:
            self.obj: Any = None

        def ensure_object(self, typ: Any) -> None:  # noqa: ANN401
            if not isinstance(self.obj, typ):
                self.obj = typ()

    class FakeClickCtx:
        invoked_subcommand = None

        def get_help(self) -> str:
            return "help"

    monkeypatch.setattr(
        cli_mod.click, "get_current_context", lambda silent=True: FakeClickCtx()
    )  # noqa: ARG005

    ctx = DummyCtx()
    with pytest.raises(typer.Exit) as excinfo:
        cli_mod.global_options(ctx, None, False, 1)
    assert excinfo.value.exit_code == 2
    assert "learning_mode" not in ctx.obj


def test_main_no_subcommand_shows_help() -> None:
    runner = CliRunner()
    cli_mod = importlib.import_module("ml_playground.runtime.cli.main")
    result = runner.invoke(cli_mod.app, [])
    assert result.exit_code == 2
    assert "Usage:" in result.output


def test_helpers_run_or_exit_runtime_error() -> None:
    with pytest.raises(typer.Exit) as excinfo:
        helpers.run_or_exit(lambda: (_ for _ in ()).throw(RuntimeError("bad")))
    assert excinfo.value.exit_code == 1


def test_commands_run_analyze_exception(monkeypatch: Any) -> None:
    def bad_module() -> Any:
        raise RuntimeError("boom")

    monkeypatch.setattr(commands, "_cli_module", bad_module)
    result = commands.run_analyze("bundestag_char", "127.0.0.1", 8050, True, None)
    assert result.success is False
    assert "Analysis failed" in result.stderr
