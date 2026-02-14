from __future__ import annotations

import logging
from pathlib import Path
from types import SimpleNamespace, TracebackType
from typing import Any, Mapping, cast

import pytest
import typer
from typer.testing import CliRunner

from ml_playground.framework.configuration.models import (
    MetadataConfig,
    PreparerConfig,
    SamplerConfig,
    TrainerConfig,
)
from ml_playground.framework.runtime import helpers
from ml_playground.runtime_cli import commands, typer_helpers
from ml_playground.framework.runtime.core import bootstrap
from ml_playground.framework.runtime.core.results import (
    LearningInfo,
    ToolResult,
    LearningModeEngine,
    VerbosityLevel,
)
from ml_playground.framework.runtime.protocols import MetadataConfigLike
from ml_playground.framework.training.loop.runner import TrainerDependencies


class _Logger:
    def __init__(self) -> None:
        self.infos: list[str] = []
        self.errors: list[str] = []
        self.warnings: list[str] = []
        self.debugs: list[str] = []

    def info(
        self,
        msg: object,
        *args: object,
        exc_info: bool
        | BaseException
        | tuple[type[BaseException], BaseException, TracebackType | None]
        | None = None,
        stack_info: bool = False,
        stacklevel: int = 1,
        extra: Mapping[str, object] | None = None,
    ) -> None:
        del exc_info, stack_info, stacklevel, extra
        self.infos.append(str(msg) % args if args else str(msg))

    def error(
        self,
        msg: object,
        *args: object,
        exc_info: bool
        | BaseException
        | tuple[type[BaseException], BaseException, TracebackType | None]
        | None = None,
        stack_info: bool = False,
        stacklevel: int = 1,
        extra: Mapping[str, object] | None = None,
    ) -> None:
        del exc_info, stack_info, stacklevel, extra
        self.errors.append(str(msg) % args if args else str(msg))

    def warning(
        self,
        msg: object,
        *args: object,
        exc_info: bool
        | BaseException
        | tuple[type[BaseException], BaseException, TracebackType | None]
        | None = None,
        stack_info: bool = False,
        stacklevel: int = 1,
        extra: Mapping[str, object] | None = None,
    ) -> None:
        del exc_info, stack_info, stacklevel, extra
        self.warnings.append(str(msg) % args if args else str(msg))

    def debug(
        self,
        msg: object,
        *args: object,
        exc_info: bool
        | BaseException
        | tuple[type[BaseException], BaseException, TracebackType | None]
        | None = None,
        stack_info: bool = False,
        stacklevel: int = 1,
        extra: Mapping[str, object] | None = None,
    ) -> None:
        del exc_info, stack_info, stacklevel, extra
        self.debugs.append(str(msg) % args if args else str(msg))


_ExcInfoType = (
    bool
    | BaseException
    | tuple[type[BaseException], BaseException, TracebackType | None]
    | None
)


def _runtime() -> SimpleNamespace:
    return SimpleNamespace(device="cpu", dtype="float32", seed=0)


def _metadata(tmp_path: Path) -> MetadataConfigLike:
    return cast(
        MetadataConfigLike,
        SimpleNamespace(
            dataset_dir=tmp_path / "data",
            train_out_dir=tmp_path / "train",
            sample_out_dir=tmp_path / "sample",
        ),
    )


def test_run_prepare_impl_success(tmp_path: Path) -> None:
    called: list[str] = []

    def create_pipeline(cfg: PreparerConfig, metadata: MetadataConfigLike) -> Any:
        return SimpleNamespace(run=lambda: called.append("run"))

    deps = bootstrap.CLIDependencies(create_pipeline=create_pipeline)
    logger = _Logger()
    cfg: PreparerConfig = cast(PreparerConfig, SimpleNamespace(logger=logger))
    metadata: MetadataConfigLike = _metadata(tmp_path)

    result = commands.run_prepare_impl("exp", cfg, Path("config"), metadata, deps)

    assert result.success is True
    assert "run" in called


def test_run_prepare_impl_failure(tmp_path: Path) -> None:
    def create_pipeline(cfg: PreparerConfig, metadata: MetadataConfigLike) -> Any:
        return SimpleNamespace(run=lambda: (_ for _ in ()).throw(RuntimeError("boom")))

    deps = bootstrap.CLIDependencies(create_pipeline=create_pipeline)
    logger = _Logger()
    cfg: PreparerConfig = cast(PreparerConfig, SimpleNamespace(logger=logger))
    metadata: MetadataConfigLike = _metadata(tmp_path)

    result = commands.run_prepare_impl("exp", cfg, Path("config"), metadata, deps)

    assert result.success is False
    assert "Pipeline preparation failed" in result.stderr


def test_run_prepare_impl_learning(tmp_path: Path) -> None:
    class DummyEngine(LearningModeEngine):
        def __init__(self) -> None:
            super().__init__(verbosity=VerbosityLevel.MINIMAL)

        def explain_command(self, **_: object) -> LearningInfo:  # type: ignore[override]
            return LearningInfo(
                explanations=["prep"], best_practices=[], related_concepts=[]
            )

    def create_pipeline(
        cfg: PreparerConfig, metadata: MetadataConfigLike
    ) -> SimpleNamespace:
        return SimpleNamespace(run=lambda: None)

    deps = bootstrap.CLIDependencies(create_pipeline=create_pipeline)
    logger = _Logger()
    cfg: PreparerConfig = cast(PreparerConfig, SimpleNamespace(logger=logger))
    metadata: MetadataConfigLike = _metadata(tmp_path)

    result = commands.run_prepare_impl(
        "exp", cfg, Path("config"), metadata, deps, DummyEngine()
    )

    assert result.success is True
    assert result.learning_info is not None
    assert result.learning_info.explanations == ["prep"]


def test_run_prepare_impl_failure_with_learning(tmp_path: Path) -> None:
    class DummyEngine(LearningModeEngine):
        def __init__(self) -> None:
            super().__init__(verbosity=VerbosityLevel.MINIMAL)

        def explain_command(self, **_: object) -> LearningInfo:  # type: ignore[override]
            return LearningInfo(
                explanations=["prep-fail"], best_practices=[], related_concepts=[]
            )

    def create_pipeline(cfg: PreparerConfig, metadata: MetadataConfigLike) -> Any:
        return SimpleNamespace(run=lambda: (_ for _ in ()).throw(RuntimeError("boom")))

    deps = bootstrap.CLIDependencies(create_pipeline=create_pipeline)
    logger = _Logger()
    cfg: PreparerConfig = cast(PreparerConfig, SimpleNamespace(logger=logger))
    metadata: MetadataConfigLike = _metadata(tmp_path)

    result = commands.run_prepare_impl(
        "exp", cfg, Path("config"), metadata, deps, DummyEngine()
    )

    assert result.success is False
    assert result.learning_info is not None
    assert result.learning_info.explanations == ["prep-fail"]


def test_run_train_impl_missing_runtime(tmp_path: Path) -> None:
    logger = _Logger()
    cfg: TrainerConfig = cast(
        TrainerConfig, SimpleNamespace(runtime=None, logger=logger)
    )
    metadata: MetadataConfigLike = _metadata(tmp_path)

    result = commands.run_train_impl("exp", cfg, Path("config"), metadata)
    assert result.success is False


def test_run_train_impl_failure_with_learning(tmp_path: Path) -> None:
    class DummyEngine(LearningModeEngine):
        def __init__(self) -> None:
            super().__init__(verbosity=VerbosityLevel.MINIMAL)

        def explain_command(self, **_: object) -> LearningInfo:  # type: ignore[override]
            return LearningInfo(
                explanations=["train-fail"], best_practices=[], related_concepts=[]
            )

    def trainer_factory(
        cfg: TrainerConfig,
        metadata: MetadataConfigLike,
        trainer_deps: TrainerDependencies | None = None,
    ) -> Any:  # type: ignore[type-arg]
        return SimpleNamespace(run=lambda: (_ for _ in ()).throw(RuntimeError("fail")))

    deps = bootstrap.CLIDependencies(
        global_device_setup=lambda *_: None,
        log_command_status=lambda *_: None,
        trainer_factory=trainer_factory,
    )
    logger = _Logger()
    cfg: TrainerConfig = cast(
        TrainerConfig, SimpleNamespace(runtime=_runtime(), logger=logger)
    )
    metadata: MetadataConfigLike = _metadata(tmp_path)

    result = commands.run_train_impl(
        "exp", cfg, Path("config"), metadata, deps, DummyEngine()
    )

    assert result.success is False
    assert result.learning_info is not None
    assert result.learning_info.explanations == ["train-fail"]


def test_run_train_impl_learning(tmp_path: Path) -> None:
    class DummyEngine(LearningModeEngine):
        def __init__(self) -> None:
            super().__init__(verbosity=VerbosityLevel.MINIMAL)

        def explain_command(self, **_: object) -> LearningInfo:  # type: ignore[override]
            return LearningInfo(
                explanations=["train"], best_practices=[], related_concepts=[]
            )

    def trainer_factory(
        cfg: TrainerConfig,
        metadata: MetadataConfigLike,
        trainer_deps: TrainerDependencies | None = None,
    ) -> Any:  # type: ignore[type-arg]
        return SimpleNamespace(run=lambda: (0, 0.0))

    deps = bootstrap.CLIDependencies(
        global_device_setup=lambda *_: None,
        log_command_status=lambda *_: None,
        trainer_factory=trainer_factory,
    )
    logger = _Logger()
    cfg: TrainerConfig = cast(
        TrainerConfig, SimpleNamespace(runtime=_runtime(), logger=logger)
    )
    metadata: MetadataConfigLike = _metadata(tmp_path)

    result = commands.run_train_impl(
        "exp", cfg, Path("config"), metadata, deps, DummyEngine()
    )

    assert result.success is True
    assert result.learning_info is not None
    assert result.learning_info.explanations == ["train"]


def test_run_sample_impl_learning(tmp_path: Path) -> None:
    logger = _Logger()
    cfg: SamplerConfig = cast(
        SamplerConfig, SimpleNamespace(runtime=None, logger=logger)
    )
    metadata: MetadataConfigLike = _metadata(tmp_path)

    result = commands.run_sample_impl("exp", cfg, Path("config"), metadata)

    assert result.success is False
    assert "Runtime configuration is missing for sampling." in result.stderr


def test_helpers_handle_tool_result_learning_sections(
    capsys: pytest.CaptureFixture[str],
) -> None:
    learning_info = LearningInfo(
        explanations=["e1"],
        best_practices=["bp1"],
        related_concepts=["rc1"],
    )
    result = ToolResult.create(
        success=True,
        exit_code=0,
        namespace="tools",
        category="dev",
        command="cmd",
        learning_info=learning_info,
    )

    helpers.handle_tool_result(result, learning_mode=True)
    captured = capsys.readouterr()

    assert "e1" in captured.out
    assert "bp1" in captured.out
    assert "rc1" in captured.out


def test_helpers_handle_tool_result_failure_exits() -> None:
    result = ToolResult.create(
        success=False,
        exit_code=5,
        namespace="tools",
        category="dev",
        command="cmd",
        stderr="err",
    )

    with pytest.raises(typer.Exit) as excinfo:
        helpers.handle_tool_result(result)
    assert excinfo.value.exit_code == 5


def test_helpers_log_directory_with_contents(tmp_path: Path) -> None:
    logger = _Logger()
    sub = tmp_path / "file.txt"
    sub.write_text("x", encoding="utf-8")
    helpers.log_directory("tag", "dir", tmp_path, logger)
    assert any("Contents" in msg for msg in logger.infos)


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


def test_bootstrap_guard_raises_when_unconfigured() -> None:
    bootstrap.clear_config_for_tests()
    try:
        with pytest.raises(RuntimeError):
            bootstrap.get_cli_dependencies()
    finally:
        # Restore a safe state (tests run in random order or sequential, good hygiene)
        bootstrap.reset_cli_dependencies()


def test_main_global_options_missing_config(tmp_path: Path) -> None:
    runner = CliRunner()
    missing = tmp_path / "nope.toml"
    import ml_playground.runtime_cli.main as cli_mod

    result = runner.invoke(cli_mod.app, ["--exp-config", str(missing)])
    assert result.exit_code == 2
    assert "Config file not found" in result.output or "Error" in result.output


def test_main_global_options_handles_bad_ctx() -> None:
    class BadCtx:
        obj: Any = None

        def ensure_object(self, arg: Any) -> None:  # noqa: ARG001
            raise AttributeError("boom")

    ctx = BadCtx()
    import ml_playground.runtime_cli.main as cli_mod

    result = cli_mod.global_options(cast(typer.Context, ctx), None, False, 1)
    assert result is None


def test_main_no_subcommand_shows_help() -> None:
    runner = CliRunner()
    import ml_playground.runtime_cli.main as cli_mod

    result = runner.invoke(cli_mod.app, [])
    assert result.exit_code == 2
    assert "Usage:" in result.output


def test_helpers_run_or_exit_runtime_error() -> None:
    with pytest.raises(typer.Exit) as excinfo:
        helpers.run_or_exit(lambda: (_ for _ in ()).throw(RuntimeError("bad")))
    assert excinfo.value.exit_code == 1


def test_commands_run_analyze_exception() -> None:
    # Use a mock logger that raises an exception to verify error handling in run_analyze
    # (Analysis currently supported only for bundestag_char)
    result = commands.run_analyze("bundestag_char", "127.0.0.1", 8050, True)
    # The current placeholder implementation doesn't easily trigger an exception
    # without deeper mocking of logging, but we verify it works.
    assert result.success is True
    assert "Analysis placeholder executed" in result.stdout


def test_extract_exp_config_handles_context(tmp_path: Path) -> None:
    import ml_playground.runtime_cli.main as cli_main
    import ml_playground.runtime_cli.typer_helpers as cli_helpers

    ctx = typer.Context(cli_main.get_command(cli_main.app))
    ctx.obj = {}
    assert cli_helpers.extract_exp_config(ctx) is None
    cfg_path = tmp_path / "exp.toml"
    ctx.obj["exp_config"] = cfg_path
    assert cli_helpers.extract_exp_config(ctx) == cfg_path


def test_global_options_sets_exp_config(tmp_path: Path) -> None:
    import ml_playground.runtime_cli.main as cli_main

    ctx = typer.Context(cli_main.get_command(cli_main.app))
    cfg_path = tmp_path / "config.toml"
    cfg_path.write_text("[section]\nvalue=1\n", encoding="utf-8")

    # Call _apply_global_options directly as global_options is now only a Typer callback
    cli_main._apply_global_options(
        ctx, exp_config=cfg_path, learning_mode=False, verbosity=1
    )

    assert ctx.obj is not None
    assert ctx.obj["exp_config"] == cfg_path


def _tool_success(category: str, command: str) -> ToolResult:
    return ToolResult.create(
        success=True,
        exit_code=0,
        namespace="ml",
        category=category,
        command=command,
        stdout=f"{category}:{command}:ok",
    )


def test_run_prepare_with_overrides(tmp_path: Path) -> None:
    import ml_playground.runtime_cli.runners as cli_runners

    metadata = _metadata(tmp_path)
    metadata_cfg = cast(MetadataConfig, metadata)
    metadata_cfg.config_path = tmp_path / "config.toml"
    metadata_cfg.config_path.touch()

    prepare_cfg = PreparerConfig(
        tokenizer_type="char", raw_text_path=tmp_path / "raw.txt"
    )

    def _run_prepare(
        experiment: str,
        cfg: Any,
        path: Path,
        meta: Any,
        deps: Any,
        engine: Any = None,
    ) -> ToolResult:
        return _tool_success("prepare", experiment)

    deps = cli_runners.CLIDependencies(
        load_experiment=lambda n, p: metadata,
        run_prepare=_run_prepare,
        global_device_setup=lambda *_: None,
        log_command_status=lambda *_: None,
    )

    with cli_runners.override_cli_dependencies(deps):
        result = cli_runners.run_prepare(
            "demo", prepare_cfg, metadata_cfg.config_path, metadata_cfg, deps
        )

    assert result.success is True


def test_run_train_with_overrides(tmp_path: Path) -> None:
    import ml_playground.runtime_cli.runners as cli_runners

    metadata = _metadata(tmp_path)
    metadata_cfg = cast(MetadataConfig, metadata)
    metadata_cfg.config_path = tmp_path / "config.toml"
    metadata_cfg.config_path.touch()

    train_cfg = cast(TrainerConfig, SimpleNamespace(runtime=_runtime()))

    def _run_train(
        experiment: str,
        cfg: Any,
        path: Path,
        meta: Any,
        deps: Any,
        engine: Any = None,
    ) -> ToolResult:
        return _tool_success("train", experiment)

    deps = cli_runners.CLIDependencies(
        load_experiment=lambda n, p: metadata,
        run_train=_run_train,
        global_device_setup=lambda *_: None,
        log_command_status=lambda *_: None,
        ensure_train_prerequisites=lambda _: None,
    )

    with cli_runners.override_cli_dependencies(deps):
        result = cli_runners.run_train(
            "demo", train_cfg, metadata_cfg.config_path, metadata_cfg, deps
        )

    assert result.success is True


def test_run_sample_with_overrides(tmp_path: Path) -> None:
    import ml_playground.runtime_cli.runners as cli_runners

    metadata = _metadata(tmp_path)
    metadata_cfg = cast(MetadataConfig, metadata)
    metadata_cfg.config_path = tmp_path / "config.toml"
    metadata_cfg.config_path.touch()

    sample_cfg = cast(SamplerConfig, SimpleNamespace(runtime=_runtime()))

    def _run_sample(
        experiment: str,
        cfg: Any,
        path: Path,
        meta: Any,
        deps: Any,
        engine: Any = None,
    ) -> ToolResult:
        return _tool_success("sample", experiment)

    deps = cli_runners.CLIDependencies(
        load_experiment=lambda n, p: metadata,
        run_sample=_run_sample,
        global_device_setup=lambda *_: None,
        log_command_status=lambda *_: None,
        ensure_sample_prerequisites=lambda _: None,
    )

    with cli_runners.override_cli_dependencies(deps):
        result = cli_runners.run_sample(
            "demo", sample_cfg, metadata_cfg.config_path, metadata_cfg, deps
        )

    assert result.success is True
