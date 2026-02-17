import logging
import typer
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast, TYPE_CHECKING, Generator

import torch
import pytest
from typer.testing import CliRunner

import ml_playground.runtime_cli.commands as cli_commands
import ml_playground.runtime_cli.device as cli_device
import ml_playground.runtime_cli.main as cli_main
import ml_playground.runtime_cli.runners as cli_runners
import ml_playground.runtime_cli.typer_helpers as typer_helpers
from ml_playground.framework.experiment_registry import registry
from ml_playground.runtime_cli.main import app as cli_app
from ml_playground.runtime_cli.runners import CLIDependencies
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
from ml_playground.framework.runtime.core.results import ToolResult

if TYPE_CHECKING:
    pass


def _normalize_cli_path(path: Path) -> Path:
    if path.is_absolute():
        try:
            return Path("/") / path.relative_to("/private")
        except ValueError:
            return path
    return path


def _ok_result(category: str, command: str) -> ToolResult:
    return ToolResult.create(
        success=True,
        exit_code=0,
        namespace="ml",
        category=category,
        command=command,
    )


class FakeLogger:
    def __init__(self) -> None:
        self.messages: list[str] = []

    def info(self, msg: str, *args: object, **kwargs: object) -> None:
        self.messages.append(msg % args if args else msg)

    def error(self, msg: str, *args: object, **kwargs: object) -> None:
        self.messages.append(msg % args if args else msg)

    def warning(self, msg: str, *args: object, **kwargs: object) -> None:
        self.messages.append(msg % args if args else msg)

    def debug(self, msg: str, *args: object, **kwargs: object) -> None:
        self.messages.append(msg % args if args else msg)


class FakeExperiment:
    def __init__(
        self,
        prepare: PreparerConfig | None,
        training: TrainerConfig | None,
        sampling: SamplerConfig | None,
        metadata: MetadataConfig,
    ) -> None:
        self.prepare = prepare
        self.training = training
        self.sampling = sampling
        self.metadata = metadata


def test_cli_prepare_invokes_overridden_dependency(tmp_path: Path) -> None:
    runner = CliRunner()
    calls: list[str] = []

    def _load_experiment(name: str, exp_config: Path | None) -> ExperimentConfig:
        assert name == "demo"
        assert exp_config is None
        # Construct actual config types to satisfy strict type checks
        metadata = MetadataConfig(
            experiment="demo",
            project_home=tmp_path,
            dataset_dir=tmp_path / "dataset",
            config_path=tmp_path / "config.toml",
            train_out_dir=tmp_path / "train",
            sample_out_dir=tmp_path / "sample",
        )
        runtime = RuntimeConfig(out_dir=tmp_path / "train")
        model = ModelConfig()
        data = DataConfig()
        optim = OptimConfig()
        schedule = LRSchedule()
        sample_params = SampleConfig()

        return ExperimentConfig(
            prepare=PreparerConfig(logger=cast(Any, FakeLogger())),
            training=TrainerConfig(
                model=model,
                data=data,
                optim=optim,
                schedule=schedule,
                runtime=runtime,
                logger=cast(Any, FakeLogger()),
            ),
            sampling=SamplerConfig(
                runtime=runtime,
                sample=sample_params,
                logger=cast(Any, FakeLogger()),
            ),
            metadata=metadata,
        )

    def _run_prepare(
        experiment: str,
        prepare_cfg: PreparerConfig,
        config_path: Path,
        metadata_cfg: MetadataConfig,
        _deps: CLIDependencies,
        _engine: object | None = None,
    ) -> ToolResult:
        assert experiment == "demo"
        # CLI normalizes paths, so we normalize the expected path too
        assert config_path == _normalize_cli_path(metadata_cfg.config_path)
        calls.append("prepare")
        return _ok_result("prepare", experiment)

    deps = CLIDependencies(
        load_experiment=_load_experiment,
        ensure_train_prerequisites=lambda _: None,
        ensure_sample_prerequisites=lambda _: None,
        run_prepare=_run_prepare,
        run_train=lambda *_: _ok_result("train", "demo"),
        run_sample=lambda *_: _ok_result("sample", "demo"),
        handle_tool_result=cli_commands.handle_tool_result,
    )

    result = runner.invoke(cli_app, ["prepare", "demo"], obj={"cli_deps": deps})

    assert result.exit_code == 0
    assert calls == ["prepare"]


@pytest.mark.parametrize(  # type: ignore[reportAny]
    "exc_type, exit_code", [(FileNotFoundError, 7), (ValueError, 5)]
)
def test_run_or_exit_maps_known_exceptions(
    exc_type: type[Exception], exit_code: int
) -> None:
    def _raise() -> None:
        raise exc_type("boom")

    with pytest.raises(cli_main.typer.Exit) as excinfo:
        typer_helpers.run_or_exit(_raise, exception_exit_code=exit_code)

    assert excinfo.value.exit_code == exit_code


def test_cli_train_missing_runtime(tmp_path: Path) -> None:
    runner = CliRunner()
    metadata = SimpleNamespace(
        experiment="demo",
        config_path=tmp_path / "config.toml",
        project_home=tmp_path,
        dataset_dir=tmp_path / "dataset",
        train_out_dir=tmp_path / "train",
        sample_out_dir=tmp_path / "sample",
    )

    def _load_experiment(name: str, exp_config: Path | None) -> Any:
        return SimpleNamespace(
            prepare=SimpleNamespace(),
            training=SimpleNamespace(runtime=None, logger=FakeLogger()),
            metadata=metadata,
        )

    deps = CLIDependencies(
        load_experiment=cast(Any, _load_experiment),
        ensure_train_prerequisites=lambda _: None,
        ensure_sample_prerequisites=lambda _: None,
        run_prepare=lambda *_: None,
        run_train=cli_commands.run_train_impl,
        run_sample=lambda *_: None,
        handle_tool_result=cli_commands.handle_tool_result,
    )

    result = runner.invoke(cli_app, ["train", "demo"], obj={"cli_deps": deps})

    assert result.exit_code == 1


def test_cli_sample_missing_runtime(tmp_path: Path) -> None:
    runner = CliRunner()
    metadata = SimpleNamespace(
        experiment="demo",
        config_path=tmp_path / "config.toml",
        project_home=tmp_path,
        dataset_dir=tmp_path / "dataset",
        train_out_dir=tmp_path / "train",
        sample_out_dir=tmp_path / "sample",
    )

    def _load_experiment(name: str, exp_config: Path | None) -> Any:
        return SimpleNamespace(
            prepare=SimpleNamespace(),
            training=SimpleNamespace(),
            sampling=SimpleNamespace(runtime=None, logger=FakeLogger()),
            metadata=metadata,
        )

    deps = CLIDependencies(
        load_experiment=cast(Any, _load_experiment),
        ensure_train_prerequisites=lambda _: None,
        ensure_sample_prerequisites=lambda _: None,
        run_prepare=lambda *_: None,
        run_train=lambda *_: None,
        run_sample=cli_commands.run_sample_impl,
        handle_tool_result=cli_commands.handle_tool_result,
    )

    result = runner.invoke(cli_app, ["sample", "demo"], obj={"cli_deps": deps})

    assert result.exit_code == 1


def test_extract_exp_config_invalid_obj() -> None:
    class MockCtx:
        def __init__(self) -> None:
            self.obj = "not-a-dict"

    assert typer_helpers.extract_exp_config(cast(Any, MockCtx())) is None


def test_extract_exp_config_missing_key() -> None:
    class MockCtx:
        def __init__(self) -> None:
            self.obj = {}

    assert typer_helpers.extract_exp_config(cast(Any, MockCtx())) is None


def test_extract_exp_config_wrong_type() -> None:
    class MockCtx:
        def __init__(self) -> None:
            self.obj = {"exp_config": "not-a-path"}

    assert typer_helpers.extract_exp_config(cast(Any, MockCtx())) is None


def test_log_dir_missing(caplog: Any) -> None:
    logger = logging.getLogger("test_logger")
    with caplog.at_level(logging.INFO):
        cli_commands.log_directory("TAG", "my_dir", Path("/non/existent/path"), logger)
    assert "[TAG] my_dir (missing): /non/existent/path" in caplog.text


def test_log_dir_not_path(caplog: Any) -> None:
    logger = logging.getLogger("test_logger")
    with caplog.at_level(logging.INFO):
        cli_commands.log_directory("TAG", "my_dir", "not-a-path", logger)
    assert caplog.text == ""


def test_cli_train_impl_success(tmp_path: Path) -> None:
    metadata = SimpleNamespace(
        experiment="demo",
        dataset_dir=tmp_path / "dataset",
        config_path=tmp_path / "config.toml",
        train_out_dir=tmp_path / "train",
    )
    # Ensure directories exist for logging
    metadata.train_out_dir.mkdir(parents=True, exist_ok=True)
    metadata.dataset_dir.mkdir(parents=True, exist_ok=True)

    runtime = SimpleNamespace(device="cpu", dtype="float32", seed=42)
    train_cfg = SimpleNamespace(runtime=runtime, logger=FakeLogger())

    # We need to mock CoreTrainer to avoid actual training

    class MockTrainer:
        def __init__(self, cfg: Any, metadata: Any) -> None:
            pass

        def run(self) -> None:
            pass

    mock_deps = CLIDependencies(
        global_device_setup=lambda *_: None,
        log_command_status=lambda *_: None,
        trainer_factory=lambda *_: None,
    )
    # run_train_impl calls global_device_setup and log_command_status
    cli_commands.run_train_impl(
        "demo", cast(Any, train_cfg), Path("unused"), cast(Any, metadata), mock_deps
    )


def test_cli_sample_impl_success(tmp_path: Path) -> None:
    metadata = SimpleNamespace(
        experiment="demo",
        dataset_dir=tmp_path / "dataset",
        config_path=tmp_path / "config.toml",
        sample_out_dir=tmp_path / "sample",
    )
    metadata.sample_out_dir.mkdir(parents=True, exist_ok=True)
    metadata.dataset_dir.mkdir(parents=True, exist_ok=True)

    runtime = SimpleNamespace(device="cpu", dtype="float32", seed=42)
    sample_cfg = SimpleNamespace(
        runtime=runtime, logger=FakeLogger(), sample=SimpleNamespace(num_samples=1)
    )

    class MockSampler:
        def __init__(self, cfg: Any, metadata: Any) -> None:
            pass

        def run(self) -> None:
            pass

    mock_deps = CLIDependencies(
        global_device_setup=lambda *_: None,
        log_command_status=lambda *_: None,
        sampler_factory=lambda *_: None,
    )
    cli_commands.run_sample_impl(
        "demo", cast(Any, sample_cfg), Path("unused"), cast(Any, metadata), mock_deps
    )


def test_global_options_config_not_found() -> None:
    class MockCtx:
        def __init__(self) -> None:
            self.obj: dict[str, Any] = {}

        def ensure_object(self, _type: Any) -> None:
            pass

    with pytest.raises(cli_main.typer.Exit) as exc:
        # Use a non-existent path
        cli_main.global_options(
            cast(Any, MockCtx()), exp_config=Path("/non/existent/config.toml")
        )
    assert exc.value.exit_code == 2


def test_run_or_exit_keyboard_interrupt(caplog: Any) -> None:
    def _interrupt() -> None:
        raise KeyboardInterrupt()

    # Should return None without raising
    with caplog.at_level(logging.INFO):
        typer_helpers.run_or_exit(_interrupt, keyboard_interrupt_msg="interrupted")
    assert "interrupted" in caplog.text


def test_run_or_exit_other_exception() -> None:
    def _error() -> None:
        raise RuntimeError("oops")

    with pytest.raises(cli_main.typer.Exit) as exc:
        typer_helpers.run_or_exit(_error, exception_exit_code=3)
    assert exc.value.exit_code == 3


def test_global_options_logging_setup(caplog: Any) -> None:
    # Test that logging is configured if no handlers exist
    # Since we can't easily remove all handlers from the root logger in a running process without side effects,
    # we just verify it runs without error.
    class MockCtx:
        def __init__(self) -> None:
            self.obj: dict[str, Any] = {}

        def ensure_object(self, _type: Any) -> None:
            pass

    cli_main.global_options(cast(Any, MockCtx()), exp_config=None)


def test_extract_exp_config_unexpected_type(caplog: Any) -> None:
    class MockCtx:
        def __init__(self) -> None:
            self.obj = {"exp_config": 123}  # Not a Path

    with caplog.at_level(logging.DEBUG):
        assert typer_helpers.extract_exp_config(cast(Any, MockCtx())) is None
    assert "Unexpected exp_config value type" in caplog.text


def test_log_dir_permission_error(tmp_path: Path, caplog: Any) -> None:
    logger = logging.getLogger("test_logger")
    test_dir = tmp_path / "perm_dir"
    test_dir.mkdir()

    class FakePath(type(Path())):
        def exists(self, *, follow_symlinks: bool = True) -> bool:
            _ = follow_symlinks
            return True

        def iterdir(self) -> Generator[Path, None, None]:
            raise PermissionError("denied")

        @property
        def name(self) -> str:
            return "perm_dir"

    with caplog.at_level(logging.INFO):
        cli_commands.log_directory(
            "TAG", "my_dir", cast(Path, FakePath(test_dir)), logger
        )
    assert "[TAG] my_dir (exists):" in caplog.text


def test_log_command_status_exception_swallow(caplog: Any) -> None:
    # Test that log_command_status swallows exceptions
    logger = logging.getLogger("test_logger")
    # Passing None for metadata when log_command_status expects MetadataConfig
    # will trigger an exception when it tries to getattr(metadata, "dataset_dir")
    cli_commands.log_command_status("TAG", cast(Any, None), Path("/path"), logger)
    # Should complete without raising


def test_global_device_setup_torch_errors() -> None:
    # Test that _global_device_setup swallows torch-related errors
    orig_manual_seed = torch.manual_seed

    def _fail_seed(s: int) -> Any:
        raise RuntimeError("torch fail")

    try:
        torch.manual_seed = _fail_seed  # type: ignore
        cli_device.global_device_setup("cpu", "float32", 42)
        # Should not raise
    finally:
        torch.manual_seed = orig_manual_seed


def test_run_analyze_unsupported() -> None:
    result = cli_commands.run_analyze("not_bundestag", "127.0.0.1", 8050, True)
    assert result.success is False
    assert result.exit_code == 1
    assert "No TensorBoard event files found" in (result.stderr or "")


def test_run_prepare_internal(tmp_path: Path) -> None:
    metadata = SimpleNamespace(
        experiment="demo",
        dataset_dir=tmp_path / "dataset",
        config_path=tmp_path / "config.toml",
    )
    prepare_cfg = PreparerConfig(
        tokenizer_type="char", logger=cast(Any, FakeLogger()), extras={}
    )

    class MockPipeline:
        def __init__(self) -> None:
            self.calls: list[str] = []

        def run(self) -> None:
            self.calls.append("run")

    pipeline_instance = MockPipeline()

    mock_deps = CLIDependencies(
        create_pipeline=lambda _cfg, _metadata: pipeline_instance,
    )
    result = cli_commands.run_prepare_impl(
        "demo",
        cast(Any, prepare_cfg),
        Path("unused"),
        cast(Any, metadata),
        mock_deps,
    )
    assert result.success is True
    assert pipeline_instance.calls == ["run"]


def test_run_train_internal_missing_runtime() -> None:
    train_cfg = SimpleNamespace(runtime=None, logger=FakeLogger())
    result = cli_commands.run_train_impl(
        "demo",
        cast(Any, train_cfg),
        Path("unused"),
        cast(Any, None),
        CLIDependencies(
            global_device_setup=lambda *_: None,
            log_command_status=lambda *_: None,
        ),
    )
    assert result.success is False
    assert result.exit_code == 1


def test_run_sample_internal_missing_runtime() -> None:
    sample_cfg = SimpleNamespace(runtime=None, logger=FakeLogger())
    result = cli_commands.run_sample_impl(
        "demo",
        cast(Any, sample_cfg),
        Path("unused"),
        cast(Any, None),
        CLIDependencies(global_device_setup=lambda *_: None),
    )
    assert result.success is False
    assert result.exit_code == 1


def test_global_device_setup_cuda_path() -> None:
    # Test the branch where _cuda_available is True
    orig_manual_seed = torch.manual_seed
    orig_cuda_manual_seed = torch.cuda.manual_seed
    # We don't mock allow_tf32 directly to avoid deprecation warnings or implementation issues
    # instead we just mock the seeds which is the primary side effect.

    try:
        torch.manual_seed = lambda _: None  # type: ignore
        torch.cuda.manual_seed = lambda _: None  # type: ignore

        # Filter the deprecation warning from torch regarding TF32
        import warnings

        with warnings.catch_warnings():
            warnings.filterwarnings("ignore", category=UserWarning, message=".*TF32.*")
            cli_device.global_device_setup(
                "cuda", "float16", 42, cuda_is_available=lambda: True
            )
    finally:
        torch.manual_seed = orig_manual_seed
        torch.cuda.manual_seed = orig_cuda_manual_seed


def test_main_startup() -> None:
    # Test main function startup (lines 578-581)
    # standlone_mode=False returns the exit code instead of exiting
    orig_registry_load = registry.load_preparers
    orig_get_command = cli_main.get_command

    class MockCmd:
        def main(self, args: Any, standalone_mode: bool) -> int:
            return 0

    try:
        registry.load_preparers = lambda: None  # type: ignore
        cli_main.get_command = lambda _app: MockCmd()  # type: ignore
        result = cli_main.main(["--help"])
        assert result == 0
    finally:
        registry.load_preparers = orig_registry_load
        cli_main.get_command = orig_get_command


def test_cli_train_cmd_full(tmp_path: Path) -> None:
    # Test run_train_cmd wiring
    metadata = SimpleNamespace(
        experiment="demo",
        dataset_dir=tmp_path / "dataset",
        config_path=tmp_path / "config.toml",
        train_out_dir=tmp_path / "train",
    )
    metadata.train_out_dir.mkdir(parents=True, exist_ok=True)

    runtime = RuntimeConfig(out_dir=tmp_path / "train", ckpt_last_filename="last.pt")
    train_cfg = TrainerConfig(
        model=ModelConfig(),
        data=DataConfig(),
        optim=OptimConfig(),
        schedule=LRSchedule(),
        runtime=runtime,
        logger=cast(Any, FakeLogger()),
    )
    exp = FakeExperiment(
        prepare=None,
        training=train_cfg,
        sampling=None,
        metadata=cast(MetadataConfig, metadata),
    )

    deps = CLIDependencies(
        load_experiment=lambda _name, _path: cast(Any, exp),
        ensure_train_prerequisites=lambda _exp: None,
        ensure_sample_prerequisites=lambda _exp: None,
        run_prepare=lambda *_: None,
        run_train=lambda *_: _ok_result("train", "demo"),
        run_sample=lambda *_: _ok_result("sample", "demo"),
    )

    cli_runners.run_train_cmd("demo", None, deps=deps)


def test_cli_sample_cmd_full(tmp_path: Path) -> None:
    # Test run_sample_cmd wiring
    metadata = SimpleNamespace(
        experiment="demo",
        dataset_dir=tmp_path / "dataset",
        config_path=tmp_path / "config.toml",
        train_out_dir=tmp_path / "train",
        sample_out_dir=tmp_path / "sample",
    )
    metadata.sample_out_dir.mkdir(parents=True, exist_ok=True)
    metadata.train_out_dir.mkdir(parents=True, exist_ok=True)

    runtime = RuntimeConfig(out_dir=tmp_path / "sample", ckpt_last_filename="last.pt")
    sample_cfg = SamplerConfig(
        runtime=runtime,
        sample=SampleConfig(),
        logger=cast(Any, FakeLogger()),
    )
    exp = FakeExperiment(
        prepare=None,
        training=None,
        sampling=sample_cfg,
        metadata=cast(MetadataConfig, metadata),
    )

    deps = CLIDependencies(
        load_experiment=lambda _name, _path: cast(Any, exp),
        ensure_train_prerequisites=lambda _exp: None,
        ensure_sample_prerequisites=lambda _exp: None,
        run_prepare=lambda *_: None,
        run_train=lambda *_: _ok_result("train", "demo"),
        run_sample=lambda *_: _ok_result("sample", "demo"),
    )

    cli_runners.run_sample_cmd("demo", None, deps=deps)


def test_run_analyze_success() -> None:
    # Test success path for bundestag_char
    calls: list[tuple[str | None, int, bool]] = []

    def _fake_analyze_runner(
        host: str | None, port: int, open_browser: bool, _logger: Any
    ) -> None:
        calls.append((host, port, open_browser))

    result = cli_commands.run_analyze(
        "bundestag_char",
        "1.2.3.4",
        8888,
        True,
        analyze_runner=_fake_analyze_runner,
    )
    assert result.success is True
    assert result.exit_code == 0
    assert result.stdout == "Analysis completed for bundestag_char"
    assert calls == [("1.2.3.4", 8888, True)]


def test_cli_analyze_command_success(tmp_path: Path) -> None:
    runner = CliRunner()

    def _run_analyze(
        experiment: str,
        host: str,
        port: int,
        open_browser: bool,
        learning_engine: Any = None,
        metadata: Any = None,
        exp_config_path: Path | None = None,
    ) -> ToolResult:
        _ = host, port, open_browser, learning_engine, metadata, exp_config_path
        return _ok_result("analyze", experiment)

    metadata = SimpleNamespace(
        experiment="bundestag_char",
        dataset_dir=tmp_path / "dataset",
        config_path=tmp_path / "config.toml",
        train_out_dir=tmp_path / "train",
        sample_out_dir=tmp_path / "sample",
    )
    exp = SimpleNamespace(prepare=None, training=None, sampling=None, metadata=metadata)

    deps = CLIDependencies(
        load_experiment=lambda _name, _path: cast(Any, exp),
        run_analyze=_run_analyze,
        handle_tool_result=cli_commands.handle_tool_result,
    )

    result = runner.invoke(
        cli_app,
        ["analyze", "bundestag_char", "--host", "1.2.3.4", "--port", "8888"],
        obj={"cli_deps": deps},
    )

    assert result.exit_code == 0


def test_cli_analyze_command_learning_mode(tmp_path: Path) -> None:
    runner = CliRunner()
    from ml_playground.framework.runtime.core.results import LearningInfo

    def _run_analyze(
        experiment: str,
        host: str,
        port: int,
        open_browser: bool,
        learning_engine: Any = None,
        metadata: Any = None,
        exp_config_path: Path | None = None,
    ) -> ToolResult:
        _ = host, port, open_browser, learning_engine, metadata, exp_config_path
        info = LearningInfo(
            explanations=["expl"], best_practices=[], related_concepts=[]
        )
        return ToolResult.create(
            success=True,
            exit_code=0,
            namespace="ml",
            category="analyze",
            command=experiment,
            learning_info=info,
        )

    metadata = SimpleNamespace(
        experiment="bundestag_char",
        dataset_dir=tmp_path / "dataset",
        config_path=tmp_path / "config.toml",
        train_out_dir=tmp_path / "train",
        sample_out_dir=tmp_path / "sample",
    )
    exp = SimpleNamespace(prepare=None, training=None, sampling=None, metadata=metadata)

    deps = CLIDependencies(
        load_experiment=lambda _name, _path: cast(Any, exp),
        run_analyze=_run_analyze,
        handle_tool_result=cli_commands.handle_tool_result,
    )

    result = runner.invoke(
        cli_app,
        ["--learning-mode", "analyze", "bundestag_char"],
        obj={"cli_deps": deps},
    )
    assert result.exit_code == 0
    assert "Learning Mode" in result.stdout


def test_deps_from_ctx_fallback() -> None:
    # Test _deps_from_ctx with no deps in ctx
    ctx = typer.Context(cli_main.get_command(cli_main.app))
    ctx.obj = {}
    # We must ensure bootstrapper is configured or provide a fake
    from ml_playground.framework.runtime.core import bootstrap

    original_deps = bootstrap._current
    try:
        bootstrap._current = CLIDependencies(
            load_experiment=lambda *_: None,
            ensure_train_prerequisites=lambda *_: None,
            ensure_sample_prerequisites=lambda *_: None,
            run_prepare=lambda *_: None,
            run_train=lambda *_: None,
            run_sample=lambda *_: None,
        )
        deps = cli_commands._deps_from_ctx(ctx)
        assert isinstance(deps, CLIDependencies)
    finally:
        bootstrap._current = original_deps


def test_app_apply_global_options_missing_config(tmp_path: Path) -> None:
    _apply_global_options = getattr(cli_main, "_apply_global_options")

    ctx = typer.Context(cli_main.get_command(cli_main.app))
    missing = tmp_path / "missing.toml"
    with pytest.raises(typer.Exit) as exc:
        _apply_global_options(ctx, exp_config=missing, learning_mode=False, verbosity=1)
    assert exc.value.exit_code == 2


def test_app_apply_global_options_not_a_file(tmp_path: Path) -> None:
    _apply_global_options = getattr(cli_main, "_apply_global_options")

    ctx = typer.Context(cli_main.get_command(cli_main.app))
    directory = tmp_path / "dir"
    directory.mkdir()
    with pytest.raises(typer.Exit) as exc:
        _apply_global_options(
            ctx, exp_config=directory, learning_mode=False, verbosity=1
        )
    assert exc.value.exit_code == 2


def test_app_apply_global_options_verbosity_enum(tmp_path: Path) -> None:
    _apply_global_options = getattr(cli_main, "_apply_global_options")
    from ml_playground.framework.runtime.core.results import VerbosityLevel

    ctx = typer.Context(cli_main.get_command(cli_main.app))
    ctx.obj = {}
    _apply_global_options(
        ctx,
        exp_config=None,
        learning_mode=True,
        verbosity=VerbosityLevel.COMPREHENSIVE,
    )
    assert ctx.obj["verbosity"] == VerbosityLevel.COMPREHENSIVE


def test_coerce_metadata_config_invalid() -> None:
    # Test _coerce_metadata_config with missing attributes
    class BadMetadata:
        pass

    res = cli_commands._coerce_metadata_config(BadMetadata())
    assert res is None


def test_run_analyze_failure() -> None:
    # Test missing event-data path in run_analyze
    result = cli_commands.run_analyze("invalid", "localhost", 0, False)
    assert result.success is False
    assert "No TensorBoard event files found" in (result.stderr or "")


def test_log_command_status_failure(caplog: Any) -> None:
    # Trigger exception in log_command_status
    logger = logging.getLogger("test_logger")
    with caplog.at_level(logging.WARNING):
        # Passing None for metadata where it expects MetadataConfig will cause error on getattr
        cli_commands.log_command_status("FAIL", cast(Any, None), None, logger)
    assert "Failed to log artifacts" in caplog.text


def test_run_train_impl_missing_deps() -> None:
    # Test runtime error for missing dependencies
    train_cfg = TrainerConfig(
        model=ModelConfig(),
        data=DataConfig(),
        optim=OptimConfig(),
        schedule=LRSchedule(),
        runtime=RuntimeConfig(out_dir=Path("/tmp")),
        logger=logging.getLogger("test"),
    )
    # deps with non-callable
    deps = CLIDependencies(global_device_setup=None, log_command_status=None)  # type: ignore
    res = cli_commands.run_train_impl(
        "demo", train_cfg, Path("/tmp/cfg.toml"), cast(Any, {}), deps
    )
    assert res.success is False
    assert "CLI dependencies not provided" in res.stderr
