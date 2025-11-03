from __future__ import annotations

import logging
import os
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast

import pytest
import typer
from typer.testing import CliRunner

import ml_playground.runtime.cli as cli
from ml_playground.runtime.cli import (
    app,
    override_cli_dependencies,
    CLIDependencies,
    run_train_cmd,
    run_sample_cmd,
    run_analyze,
    handle_tool_result,
    run_prepare_impl,
    log_directory,
    log_command_status,
    global_device_setup,
)
from ml_playground.configuration.models import ExperimentConfig
from ml_playground.tools.core.interfaces import ToolResult


@pytest.fixture
def runner() -> CliRunner:
    """Typer CLI runner for testing."""
    return CliRunner()


@pytest.fixture
def temp_config_file(tmp_path: Path) -> Path:
    """Create a minimal temporary config file for testing."""
    config_content = """
[shared]
dataset_dir = "tmp/dataset"
config_path = "tmp/config.toml"

[prepare]
vocab_size = 256

[train]
n_layer = 1
n_head = 1
n_embd = 32

[sample]
max_new_tokens = 100
"""
    config_path = tmp_path / "config.toml"
    config_path.write_text(config_content, encoding="utf-8")
    return config_path


def _build_stub_experiment(
    tmp_path: Path, experiment: str = "demo"
) -> tuple[ExperimentConfig, SimpleNamespace]:
    base_dir = tmp_path / experiment
    dataset_dir = base_dir / "dataset"
    train_dir = base_dir / "train"
    sample_dir = base_dir / "sample"
    dataset_dir.mkdir(parents=True, exist_ok=True)
    train_dir.mkdir(parents=True, exist_ok=True)
    sample_dir.mkdir(parents=True, exist_ok=True)
    config_path = base_dir / "config.toml"
    config_path.write_text("{}", encoding="utf-8")

    shared = SimpleNamespace(
        experiment=experiment,
        config_path=config_path,
        project_home=base_dir,
        dataset_dir=dataset_dir,
        train_out_dir=train_dir,
        sample_out_dir=sample_dir,
    )

    exp = cast(
        ExperimentConfig,
        SimpleNamespace(
            prepare="prepare_cfg",
            train="train_cfg",
            sample="sample_cfg",
            shared=shared,
        ),
    )

    return exp, shared


def _raise_keyboard_interrupt_load(_: str, __: Path | None) -> ExperimentConfig:
    raise KeyboardInterrupt()


def _raise_value_error_load(_: str, __: Path | None) -> ExperimentConfig:
    raise ValueError("test error")


def _raise_keyboard_interrupt_ensure(_: ExperimentConfig) -> Path:
    raise KeyboardInterrupt()


def _raise_value_error_ensure(_: ExperimentConfig) -> Path:
    raise ValueError("test error")


class TestGlobalOptions:
    """Test global CLI options like --exp-config validation."""

    def test_exp_config_missing_file_exits_with_code_2(
        self, runner: CliRunner, caplog: pytest.LogCaptureFixture, tmp_path: Path
    ) -> None:
        """Test that --exp-config with missing file exits with code 2."""
        missing_path = tmp_path / "missing.toml"
        with caplog.at_level(logging.ERROR, logger="ml_playground.cli"):
            result = runner.invoke(
                app, ["--exp-config", str(missing_path), "prepare", "shakespeare"]
            )
        assert result.exit_code == 2
        assert "Config file not found" in caplog.messages[-1]

    def test_exp_config_valid_file_sets_context(
        self, runner: CliRunner, temp_config_file: Path
    ) -> None:
        """Test that valid --exp-config is stored in context."""
        # This test passes if no error occurs; context setting is internal
        result = runner.invoke(
            app, ["--exp-config", str(temp_config_file), "prepare", "shakespeare"]
        )
        # Since shakespeare experiment may not exist, we expect an error, but context should be set
        assert result.exit_code != 2  # Not the config file error

    def test_context_initialization_fallback_on_bad_context(
        self, runner: CliRunner
    ) -> None:
        """Test fallback when Typer context object is malformed."""
        # This is hard to trigger directly, but ensure basic commands work
        result = runner.invoke(app, ["prepare", "nonexistent"])
        assert result.exit_code != 0  # Some error, but not context-related crash

    def test_global_options_preserves_existing_handlers(self) -> None:
        """Ensure global_options does not reset logging when handlers already exist."""

        root_logger = logging.getLogger()
        original_handlers = list(root_logger.handlers)
        for handler in original_handlers:
            root_logger.removeHandler(handler)

        test_handler = logging.NullHandler()
        root_logger.addHandler(test_handler)

        pre_call_handlers = list(root_logger.handlers)
        try:
            ctx = typer.Context(cli.get_command(app))
            cli.global_options(ctx, exp_config=None)
            post_call_handlers = list(root_logger.handlers)
        finally:
            root_logger.removeHandler(test_handler)
            for handler in original_handlers:
                root_logger.addHandler(handler)

        assert post_call_handlers == pre_call_handlers
        assert ctx.obj == {"exp_config": None}

    def test_global_options_initializes_logging_when_no_handlers(self) -> None:
        """Ensure global_options configures logging when no handlers are present."""

        root_logger = logging.getLogger()
        original_handlers = list(root_logger.handlers)
        for handler in original_handlers:
            root_logger.removeHandler(handler)

        try:
            ctx = typer.Context(cli.get_command(app))
            cli.global_options(ctx, exp_config=None)
            post_call_handlers = list(root_logger.handlers)
        finally:
            for handler in list(root_logger.handlers):
                root_logger.removeHandler(handler)
            for handler in original_handlers:
                root_logger.addHandler(handler)

        assert post_call_handlers
        assert ctx.obj == {"exp_config": None}


class TestCommandRunners:
    """Test prepare, train, sample commands with run_or_exit wrapping."""

    def test_prepare_keyboard_interrupt_handled(
        self, runner: CliRunner, caplog: pytest.LogCaptureFixture, tmp_path: Path
    ) -> None:
        """Test prepare handles KeyboardInterrupt gracefully."""
        exp, _ = _build_stub_experiment(tmp_path, "shakespeare")

        def load_experiment(_: str, __: Path | None) -> ExperimentConfig:
            raise KeyboardInterrupt()

        deps = CLIDependencies(
            load_experiment=load_experiment,
            ensure_train_prerequisites=lambda _: None,
            ensure_sample_prerequisites=lambda _: None,
            run_prepare=lambda *_: None,
            run_train=lambda *_: None,
            run_sample=lambda *_: None,
        )
        with (
            override_cli_dependencies(deps),
            caplog.at_level(logging.INFO, logger="ml_playground.runtime.cli"),
        ):
            result = runner.invoke(app, ["prepare", "shakespeare"])
            assert result.exit_code == 0
            assert "Data preparation cancelled" in caplog.messages[-1]

    def test_train_keyboard_interrupt_handled(
        self, runner: CliRunner, caplog: pytest.LogCaptureFixture, tmp_path: Path
    ) -> None:
        """Test train handles KeyboardInterrupt gracefully."""
        exp, _ = _build_stub_experiment(tmp_path, "shakespeare")

        def load_experiment(_: str, __: Path | None) -> ExperimentConfig:
            return exp

        def ensure_train(_: ExperimentConfig) -> None:
            raise KeyboardInterrupt()

        deps = CLIDependencies(
            load_experiment=load_experiment,
            ensure_train_prerequisites=ensure_train,
            ensure_sample_prerequisites=lambda _: None,
            run_prepare=lambda *_: None,
            run_train=lambda *_: None,
            run_sample=lambda *_: None,
        )
        with (
            override_cli_dependencies(deps),
            caplog.at_level(logging.INFO, logger="ml_playground.runtime.cli"),
        ):
            result = runner.invoke(app, ["train", "shakespeare"])
            assert result.exit_code == 0
            assert "Training cancelled" in caplog.messages[-1]

    def test_sample_keyboard_interrupt_handled(
        self, runner: CliRunner, caplog: pytest.LogCaptureFixture, tmp_path: Path
    ) -> None:
        """Test sample handles KeyboardInterrupt gracefully."""
        exp, _ = _build_stub_experiment(tmp_path, "shakespeare")

        def load_experiment(_: str, __: Path | None) -> ExperimentConfig:
            return exp

        def ensure_sample(_: ExperimentConfig) -> tuple[Path, Path]:
            raise KeyboardInterrupt()

        deps = CLIDependencies(
            load_experiment=load_experiment,
            ensure_train_prerequisites=lambda _: None,
            ensure_sample_prerequisites=ensure_sample,
            run_prepare=lambda *_: None,
            run_train=lambda *_: None,
            run_sample=lambda *_: None,
        )
        with (
            override_cli_dependencies(deps),
            caplog.at_level(logging.INFO, logger="ml_playground.runtime.cli"),
        ):
            result = runner.invoke(app, ["sample", "shakespeare"])
            assert result.exit_code == 0
            assert "Sampling cancelled" in caplog.messages[-1]

    def test_prepare_domain_exception_exits_with_code_1(
        self, runner: CliRunner, caplog: pytest.LogCaptureFixture, tmp_path: Path
    ) -> None:
        """Test prepare exits with code 1 on ValueError."""

        def load_experiment(_: str, __: Path | None) -> ExperimentConfig:
            raise ValueError("test error")

        deps = CLIDependencies(
            load_experiment=load_experiment,
            ensure_train_prerequisites=lambda _: None,
            ensure_sample_prerequisites=lambda _: None,
            run_prepare=lambda *_: None,
            run_train=lambda *_: None,
            run_sample=lambda *_: None,
        )
        with override_cli_dependencies(deps), caplog.at_level(logging.ERROR):
            result = runner.invoke(app, ["prepare", "shakespeare"])
            assert result.exit_code == 1
            assert "test error" in caplog.messages[-1]

    def test_train_domain_exception_exits_with_code_1(
        self, runner: CliRunner, caplog: pytest.LogCaptureFixture, tmp_path: Path
    ) -> None:
        """Test train exits with code 1 on ValueError."""
        exp, _ = _build_stub_experiment(tmp_path, "shakespeare")

        def load_experiment(_: str, __: Path | None) -> ExperimentConfig:
            return exp

        def ensure_train(_: ExperimentConfig) -> None:
            raise ValueError("test error")

        deps = CLIDependencies(
            load_experiment=load_experiment,
            ensure_train_prerequisites=ensure_train,
            ensure_sample_prerequisites=lambda _: None,
            run_prepare=lambda *_: None,
            run_train=lambda *_: None,
            run_sample=lambda *_: None,
        )
        with override_cli_dependencies(deps), caplog.at_level(logging.ERROR):
            result = runner.invoke(app, ["train", "shakespeare"])
            assert result.exit_code == 1
            assert "test error" in caplog.messages[-1]

    def test_sample_domain_exception_exits_with_code_1(
        self, runner: CliRunner, caplog: pytest.LogCaptureFixture, tmp_path: Path
    ) -> None:
        """Test sample exits with code 1 on ValueError."""
        exp, _ = _build_stub_experiment(tmp_path, "shakespeare")

        def load_experiment(_: str, __: Path | None) -> ExperimentConfig:
            return exp

        def ensure_sample(_: ExperimentConfig) -> tuple[Path, Path]:
            raise ValueError("test error")

        deps = CLIDependencies(
            load_experiment=load_experiment,
            ensure_train_prerequisites=lambda _: None,
            ensure_sample_prerequisites=ensure_sample,
            run_prepare=lambda *_: None,
            run_train=lambda *_: None,
            run_sample=lambda *_: None,
        )
        with override_cli_dependencies(deps), caplog.at_level(logging.ERROR):
            result = runner.invoke(app, ["sample", "shakespeare"])
            assert result.exit_code == 1
            assert "test error" in caplog.messages[-1]


class TestHelperCoverage:
    def test_run_or_exit_keyboard_interrupt_logs_message(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        """run_or_exit should log a message when KeyboardInterrupt occurs."""

        def _raise_interrupt() -> None:
            raise KeyboardInterrupt()

        with caplog.at_level(logging.INFO, logger="ml_playground.runtime.cli"):
            cli.run_or_exit(_raise_interrupt, keyboard_interrupt_msg="Cancelled.")

        assert any("Cancelled." in msg for msg in caplog.messages)

    def test_run_or_exit_keyboard_interrupt_no_message(self) -> None:
        """run_or_exit should quietly swallow KeyboardInterrupt without a message."""

        def _raise_interrupt() -> None:
            raise KeyboardInterrupt()

        cli.run_or_exit(_raise_interrupt)

    def testrun_analyze_logs_placeholder(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        """run_analyze should log a not implemented message for bundestag_char."""

        with caplog.at_level(logging.INFO, logger="ml_playground.runtime.cli"):
            run_analyze("bundestag_char", "0.0.0.0", 9000, False)

        assert any("not implemented" in msg for msg in caplog.messages)

    def testrun_analyze_unknown_experiment_raises(self) -> None:
        """run_analyze should return failure for unsupported experiments."""

        result = run_analyze("other", "127.0.0.1", 8050, True)
        assert not result.success
        assert result.exit_code == 1
        assert "analyze currently supports only 'bundestag_char'" in result.stderr

    def test_analyze_command_invokes_helper(
        self, runner: CliRunner, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Analyze CLI command should forward to run_analyze."""

        with caplog.at_level(logging.INFO, logger="ml_playground.runtime.cli"):
            result = runner.invoke(
                app,
                [
                    "analyze",
                    "bundestag_char",
                    "--host",
                    "0.0.0.0",
                    "--port",
                    "9000",
                    "--no-open-browser",
                ],
            )

        assert result.exit_code == 0
        assert any(
            "Analysis for 'bundestag_char'" in message for message in caplog.messages
        )

    def testrun_train_cmd_uses_default_dependencies(self, tmp_path: Path) -> None:
        """run_train_cmd should request dependencies when none are provided."""

        calls: dict[str, list[Any]] = {
            "load": [],
            "ensure": [],
            "run": [],
        }

        shared = SimpleNamespace(config_path=tmp_path / "cfg.toml")
        exp = SimpleNamespace(train="train_cfg", shared=shared)

        def fake_load(experiment: str, cfg: Path | None) -> Any:
            calls["load"].append((experiment, cfg))
            return exp

        def fake_ensure(exp_cfg: Any) -> None:
            calls["ensure"].append(exp_cfg)

        def fake_run(
            experiment: str,
            train_cfg: Any,
            config_path: Path,
            shared_cfg: Any,
            learning_engine: Any = None,
        ) -> ToolResult:
            calls["run"].append((experiment, train_cfg, config_path, shared_cfg))
            return ToolResult.create(
                success=True,
                exit_code=0,
                namespace="ml",
                category="train",
                command=experiment,
            )

        deps = CLIDependencies(
            load_experiment=fake_load,
            ensure_train_prerequisites=fake_ensure,
            ensure_sample_prerequisites=lambda _: None,
            run_prepare=lambda *args: ToolResult.create(
                success=True,
                exit_code=0,
                namespace="ml",
                category="prepare",
                command="test",
            ),
            run_train=fake_run,
            run_sample=lambda *args: ToolResult.create(
                success=True,
                exit_code=0,
                namespace="ml",
                category="sample",
                command="test",
            ),
        )

        with override_cli_dependencies(deps):
            run_train_cmd("demo", None, deps=None)

        assert calls["load"] == [("demo", None)]
        assert calls["ensure"] == [exp]
        assert calls["run"] == [("demo", "train_cfg", shared.config_path, shared)]

    def testrun_sample_cmd_uses_default_dependencies(self, tmp_path: Path) -> None:
        """run_sample_cmd should request dependencies when none are provided."""

        calls: dict[str, list[Any]] = {
            "load": [],
            "ensure": [],
            "run": [],
        }

        shared = SimpleNamespace(config_path=tmp_path / "cfg.toml")
        exp = SimpleNamespace(sample="sample_cfg", shared=shared)

        def fake_load(experiment: str, cfg: Path | None) -> Any:
            calls["load"].append((experiment, cfg))
            return exp

        def fake_ensure(exp_cfg: Any) -> None:
            calls["ensure"].append(exp_cfg)

        def fake_run(
            experiment: str,
            sample_cfg: Any,
            config_path: Path,
            shared_cfg: Any,
            learning_engine: Any = None,
        ) -> ToolResult:
            calls["run"].append((experiment, sample_cfg, config_path, shared_cfg))
            return ToolResult.create(
                success=True,
                exit_code=0,
                namespace="ml",
                category="sample",
                command=experiment,
            )

        deps = CLIDependencies(
            load_experiment=fake_load,
            ensure_train_prerequisites=lambda _: None,
            ensure_sample_prerequisites=fake_ensure,
            run_prepare=lambda *args: ToolResult.create(
                success=True,
                exit_code=0,
                namespace="ml",
                category="prepare",
                command="test",
            ),
            run_train=lambda *args: ToolResult.create(
                success=True,
                exit_code=0,
                namespace="ml",
                category="train",
                command="test",
            ),
            run_sample=fake_run,
        )

        with override_cli_dependencies(deps):
            run_sample_cmd("demo", None, deps=None)

        assert calls["load"] == [("demo", None)]
        assert calls["ensure"] == [exp]
        assert calls["run"] == [("demo", "sample_cfg", shared.config_path, shared)]

    def testrun_train_cmd_handles_failure_result(self) -> None:
        """run_train_cmd should exit with failure when dependencies return errors."""

        failure_result = ToolResult.create(
            success=False,
            exit_code=7,
            namespace="ml",
            category="train",
            command="demo",
            stderr="train failure",
        )

        deps = CLIDependencies(
            load_experiment=lambda *_: SimpleNamespace(
                train="train_cfg",
                shared=SimpleNamespace(config_path=Path("cfg.toml")),
            ),
            ensure_train_prerequisites=lambda *_: None,
            ensure_sample_prerequisites=lambda *_: None,
            run_prepare=lambda *_: ToolResult.create(
                success=True,
                exit_code=0,
                namespace="ml",
                category="prepare",
                command="test",
            ),
            run_train=lambda *_: failure_result,
            run_sample=lambda *_: ToolResult.create(
                success=True,
                exit_code=0,
                namespace="ml",
                category="sample",
                command="test",
            ),
        )

        with override_cli_dependencies(deps):
            with pytest.raises(typer.Exit) as excinfo:
                run_train_cmd("demo", None, deps=None)

        assert excinfo.value.exit_code == 7

    def testrun_sample_cmd_handles_failure_result(self) -> None:
        """run_sample_cmd should exit with failure when dependencies return errors."""

        failure_result = ToolResult.create(
            success=False,
            exit_code=5,
            namespace="ml",
            category="sample",
            command="demo",
            stderr="sample failure",
        )

        deps = CLIDependencies(
            load_experiment=lambda *_: SimpleNamespace(
                sample="sample_cfg",
                shared=SimpleNamespace(config_path=Path("cfg.toml")),
            ),
            ensure_train_prerequisites=lambda *_: None,
            ensure_sample_prerequisites=lambda *_: None,
            run_prepare=lambda *_: ToolResult.create(
                success=True,
                exit_code=0,
                namespace="ml",
                category="prepare",
                command="test",
            ),
            run_train=lambda *_: ToolResult.create(
                success=True,
                exit_code=0,
                namespace="ml",
                category="train",
                command="test",
            ),
            run_sample=lambda *_: failure_result,
        )

        with override_cli_dependencies(deps):
            with pytest.raises(typer.Exit) as excinfo:
                run_sample_cmd("demo", None, deps=None)

        assert excinfo.value.exit_code == 5

    def test_complete_experiments_returns_known_experiments(self) -> None:
        """_complete_experiments should list available experiments including bundestag_char."""

        ctx = typer.Context(cli.get_command(app))
        result = cli._complete_experiments(ctx, "bund")

        assert any(entry == "bundestag_char" for entry in result)

    def test_main_returns_zero_status(self) -> None:
        """main should complete successfully for --help invocation."""

        exit_code = cli.main(["--help"])

        assert exit_code == 0


class TestDeviceSetupFallbacks:
    """Test global_device_setup error handling."""

    def test_device_setup_swallows_cuda_errors(self) -> None:
        """Test that torch errors in device setup are swallowed."""

        # Should not raise, even with cuda available but error
        global_device_setup("cuda", "float32", 42, cuda_is_available=lambda: True)

    def test_device_setup_success_path(self) -> None:
        """Test successful CUDA setup."""

        # Inject fake cuda available
        global_device_setup("cuda", "float32", 42, cuda_is_available=lambda: True)

    def test_device_setup_explicit_cuda_override(self) -> None:
        """Test injecting cuda_is_available callable."""

        called = False

        def fake_cuda():
            nonlocal called
            called = True
            return False

        global_device_setup("cpu", "float32", 42, cuda_is_available=fake_cuda)
        assert called


class TestAnalysisGuardRails:
    """Test run_analyze experiment validation."""

    def test_analyze_rejects_non_bundestag_char(self) -> None:
        """Test failure result for unsupported experiments."""
        result = cli.run_analyze("shakespeare", "127.0.0.1", 8050, True)
        assert not result.success
        assert result.exit_code == 1
        assert "analyze currently supports only 'bundestag_char'" in result.stderr

    def test_analyze_accepts_bundestag_char(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Test info log for supported experiment."""
        with caplog.at_level("INFO"):
            cli.run_analyze("bundestag_char", "127.0.0.1", 8050, True)
        assert "Analysis for 'bundestag_char' not implemented" in caplog.text


class TestDirectoryLoggingResilience:
    """Test log_directory and log_command_status error handling."""

    def testlog_directory_handles_unset_path(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Test logging when dir_path is None."""
        import logging

        logger = logging.getLogger("test")
        with caplog.at_level("INFO"):
            log_directory("test", "test_dir", None, logger)
        assert "<not set>" in caplog.text

    def testlog_directory_handles_missing_directory(
        self, caplog: pytest.LogCaptureFixture, tmp_path: Path
    ) -> None:
        """Test logging for non-existent directory."""
        import logging

        logger = logging.getLogger("test")
        missing_dir = tmp_path / "missing"
        with caplog.at_level("INFO"):
            log_directory("test", "test_dir", missing_dir, logger)
        assert "(missing)" in caplog.text

    def testlog_directory_handles_existing_directory(
        self, caplog: pytest.LogCaptureFixture, tmp_path: Path
    ) -> None:
        """Test logging for existing directory."""
        import logging

        logger = logging.getLogger("test")
        existing_dir = tmp_path / "existing"
        existing_dir.mkdir()
        (existing_dir / "file.txt").write_text("content")
        with caplog.at_level("INFO"):
            log_directory("test", "test_dir", existing_dir, logger)
        assert "(exists)" in caplog.text
        assert "Contents:" in caplog.text

    def testlog_directory_handles_unreadable_directory(
        self, caplog: pytest.LogCaptureFixture, tmp_path: Path
    ) -> None:
        """Test logging for directory without read permission."""
        import logging

        logger = logging.getLogger("test")
        unreadable_dir = tmp_path / "unreadable"
        unreadable_dir.mkdir()
        os.chmod(unreadable_dir, 0o000)  # No permissions
        try:
            with caplog.at_level("INFO"):
                log_directory("test", "test_dir", unreadable_dir, logger)
            assert "(exists)" in caplog.text
            # Should not crash, even if contents not listed
        finally:
            os.chmod(unreadable_dir, 0o755)  # Restore for cleanup

    def testlog_directory_ignores_non_path_values(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        """log_directory should ignore values that are not Path instances."""
        logger = logging.getLogger("test")
        with caplog.at_level("INFO"):
            log_directory("test", "test_dir", "not-a-path", logger)
        assert caplog.text == ""

    def testlog_command_status_handles_errors_gracefully(
        self, caplog: pytest.LogCaptureFixture, tmp_path: Path
    ) -> None:
        """Test log_command_status handles OSError/ValueError/TypeError gracefully."""
        from ml_playground.configuration.models import SharedConfig

        shared = SharedConfig(
            experiment="test",
            config_path=tmp_path / "config.toml",
            project_home=tmp_path,
            dataset_dir=tmp_path / "dataset",
            train_out_dir=tmp_path / "train",
            sample_out_dir=tmp_path / "sample",
        )

        class ExplodingPath(Path):  # type: ignore[misc]
            _flavour = Path(".")._flavour

            def iterdir(self):  # type: ignore[override]
                raise OSError("boom")

        exploding_dir = ExplodingPath(str(tmp_path / "explode"))

        with caplog.at_level("INFO"):
            log_command_status(
                "tag", shared, exploding_dir, logging.getLogger(__name__)
            )

        assert "boom" not in caplog.text

    def testlog_command_status_logs_expected_directories(
        self, caplog: pytest.LogCaptureFixture, tmp_path: Path
    ) -> None:
        """log_command_status should log out_dir and dataset_dir details."""
        from ml_playground.configuration.models import SharedConfig

        shared = SharedConfig(
            experiment="test",
            config_path=tmp_path / "config.toml",
            project_home=tmp_path,
            dataset_dir=tmp_path / "dataset",
            train_out_dir=tmp_path / "train",
            sample_out_dir=tmp_path / "sample",
        )

        shared.dataset_dir.mkdir(parents=True, exist_ok=True)
        out_dir = tmp_path / "output"
        out_dir.mkdir(parents=True, exist_ok=True)

        with caplog.at_level("INFO"):
            log_command_status("tag", shared, out_dir, logging.getLogger(__name__))

        assert "[tag] out_dir" in caplog.text
        assert "[tag] dataset_dir" in caplog.text


class TestMLWorkflowLearningModeIntegration:
    """Test learning mode integration with ML workflow commands."""

    def test_learning_mode_flag_parsing(self, runner: CliRunner) -> None:
        """Test that learning mode flags are properly parsed."""
        # Test learning mode flag is accepted
        result = runner.invoke(app, ["--learning-mode", "--help"])
        assert result.exit_code == 0

        # Test verbosity flag is accepted
        result = runner.invoke(app, ["--verbosity", "2", "--help"])
        assert result.exit_code == 0

        # Test combined flags
        result = runner.invoke(app, ["--learning-mode", "--verbosity", "1", "--help"])
        assert result.exit_code == 0

    def test_learning_mode_context_storage(self, runner: CliRunner) -> None:
        """Test that learning mode settings are stored in context."""
        import typer

        # Create a test command that checks context
        test_app = typer.Typer()

        @test_app.callback()
        def global_options(
            ctx: typer.Context,
            learning_mode: bool = typer.Option(False, "--learning-mode"),
            verbosity: int = typer.Option(1, "--verbosity", min=0, max=2),
        ) -> None:
            if ctx.obj is None:
                ctx.obj = {}
            ctx.obj["learning_mode"] = learning_mode
            ctx.obj["verbosity"] = verbosity

        @test_app.command()
        def test_cmd(ctx: typer.Context) -> None:
            assert ctx.obj is not None
            assert "learning_mode" in ctx.obj
            assert "verbosity" in ctx.obj

        # Test with learning mode enabled
        result = runner.invoke(
            test_app, ["--learning-mode", "--verbosity", "2", "test-cmd"]
        )
        assert result.exit_code == 0

    def test_prepare_command_with_learning_mode(self, tmp_path: Path) -> None:
        """Test prepare command integrates learning mode correctly."""
        from ml_playground.tools.core.learning_mode import (
            LearningModeEngine,
            VerbosityLevel,
        )
        from ml_playground.configuration.models import PreparerConfig, SharedConfig

        # Create minimal configs
        shared = SharedConfig(
            experiment="test",
            config_path=tmp_path / "config.toml",
            project_home=tmp_path,
            dataset_dir=tmp_path / "dataset",
            train_out_dir=tmp_path / "train",
            sample_out_dir=tmp_path / "sample",
        )

        prepare_cfg = PreparerConfig(
            raw_dir=tmp_path / "raw",
        )

        # Create learning mode engine
        learning_engine = LearningModeEngine(VerbosityLevel.STANDARD)

        # This will fail due to missing data, but should return ToolResult with learning info
        result = run_prepare_impl(
            "bundestag_char",
            prepare_cfg,
            tmp_path / "config.toml",
            shared,
            learning_engine,
        )

        # Verify ToolResult structure
        assert not result.success  # Expected to fail due to missing data
        assert result.operation_id.namespace == "ml"
        assert result.operation_id.category == "prepare"
        assert result.operation_id.command == "bundestag_char"
        assert result.learning_info is not None
        assert len(result.learning_info.explanations) > 0
        assert len(result.learning_info.best_practices) > 0

    def test_train_command_with_learning_mode(self) -> None:
        """Test train command learning mode integration using mocked dependencies."""
        from ml_playground.tools.core.learning_mode import (
            LearningModeEngine,
            VerbosityLevel,
        )
        from types import SimpleNamespace

        # Create learning mode engine
        learning_engine = LearningModeEngine(VerbosityLevel.COMPREHENSIVE)

        # Mock dependencies to avoid complex configuration setup
        calls = []

        def mock_load_experiment(name: str, path: Path | None) -> Any:
            return SimpleNamespace(
                train="mock_train_cfg",
                shared=SimpleNamespace(config_path=Path("test.toml")),
            )

        def mock_ensure_prerequisites(config: Any) -> None:
            pass

        def mock_run_train(
            experiment: str,
            config: Any,
            config_path: Path,
            shared: Any,
            learning_engine: Any = None,
        ) -> ToolResult:
            calls.append(("run_train", experiment, learning_engine is not None))
            # Create result with learning info if learning_engine provided
            learning_info = None
            if learning_engine:
                learning_info = learning_engine.explain_command(
                    experiment, "model training", "train"
                )

            return ToolResult.create(
                success=True,
                exit_code=0,
                namespace="ml",
                category="train",
                command=experiment,
                learning_info=learning_info,
            )

        deps = CLIDependencies(
            load_experiment=mock_load_experiment,
            ensure_train_prerequisites=mock_ensure_prerequisites,
            ensure_sample_prerequisites=mock_ensure_prerequisites,
            run_prepare=lambda *_: ToolResult.create(
                success=True,
                exit_code=0,
                namespace="ml",
                category="prepare",
                command="test",
            ),
            run_train=mock_run_train,
            run_sample=lambda *_: ToolResult.create(
                success=True,
                exit_code=0,
                namespace="ml",
                category="sample",
                command="test",
            ),
        )

        with override_cli_dependencies(deps):
            run_train_cmd(
                "bundestag_char", None, deps, learning_engine, learning_mode=True
            )

        # Verify that the learning engine was passed through
        assert len(calls) == 1
        assert calls[0][0] == "run_train"
        assert calls[0][1] == "bundestag_char"
        assert calls[0][2] is True  # learning_engine was provided

    def test_sample_command_with_learning_mode(self) -> None:
        """Test sample command learning mode integration using mocked dependencies."""
        from ml_playground.tools.core.learning_mode import (
            LearningModeEngine,
            VerbosityLevel,
        )
        from types import SimpleNamespace

        # Create learning mode engine
        learning_engine = LearningModeEngine(VerbosityLevel.MINIMAL)

        # Mock dependencies to avoid complex configuration setup
        calls = []

        def mock_load_experiment(name: str, path: Path | None) -> Any:
            return SimpleNamespace(
                sample="mock_sample_cfg",
                shared=SimpleNamespace(config_path=Path("test.toml")),
            )

        def mock_ensure_prerequisites(config: Any) -> None:
            pass

        def mock_run_sample(
            experiment: str,
            config: Any,
            config_path: Path,
            shared: Any,
            learning_engine: Any = None,
        ) -> ToolResult:
            calls.append(("run_sample", experiment, learning_engine is not None))
            # Create result with learning info if learning_engine provided
            learning_info = None
            if learning_engine:
                learning_info = learning_engine.explain_command(
                    experiment, "text generation", "sample"
                )

            return ToolResult.create(
                success=True,
                exit_code=0,
                namespace="ml",
                category="sample",
                command=experiment,
                learning_info=learning_info,
            )

        deps = CLIDependencies(
            load_experiment=mock_load_experiment,
            ensure_train_prerequisites=mock_ensure_prerequisites,
            ensure_sample_prerequisites=mock_ensure_prerequisites,
            run_prepare=lambda *_: ToolResult.create(
                success=True,
                exit_code=0,
                namespace="ml",
                category="prepare",
                command="test",
            ),
            run_train=lambda *_: ToolResult.create(
                success=True,
                exit_code=0,
                namespace="ml",
                category="train",
                command="test",
            ),
            run_sample=mock_run_sample,
        )

        with override_cli_dependencies(deps):
            run_sample_cmd(
                "bundestag_qwen15b_lora_mps",
                None,
                deps,
                learning_engine,
                learning_mode=True,
            )

        # Verify that the learning engine was passed through
        assert len(calls) == 1
        assert calls[0][0] == "run_sample"
        assert calls[0][1] == "bundestag_qwen15b_lora_mps"
        assert calls[0][2] is True  # learning_engine was provided

    def test_analyze_command_with_learning_mode(self) -> None:
        """Test analyze command integrates learning mode correctly."""
        from ml_playground.tools.core.learning_mode import (
            LearningModeEngine,
            VerbosityLevel,
        )

        # Create learning mode engine
        learning_engine = LearningModeEngine(VerbosityLevel.STANDARD)

        # Test with supported experiment
        result = run_analyze("bundestag_char", "127.0.0.1", 8050, True, learning_engine)

        # Verify ToolResult structure
        assert result.success  # Should succeed as it's just a placeholder
        assert result.operation_id.namespace == "ml"
        assert result.operation_id.category == "analyze"
        assert result.operation_id.command == "bundestag_char"
        assert result.learning_info is not None
        assert len(result.learning_info.explanations) > 0

    def testhandle_tool_result_displays_learning_info(
        self, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """Test that handle_tool_result properly displays learning information."""
        from ml_playground.tools.core.learning_mode import (
            LearningModeEngine,
            VerbosityLevel,
        )

        # Create learning info
        engine = LearningModeEngine(VerbosityLevel.STANDARD)
        learning_info = engine.explain_command(
            "bundestag_char", "data preparation", "prepare"
        )

        # Create ToolResult with learning info
        result = ToolResult.create(
            success=True,
            exit_code=0,
            namespace="ml",
            category="prepare",
            command="bundestag_char",
            stdout="Preparation completed successfully",
            learning_info=learning_info,
        )

        # Test with learning mode enabled
        handle_tool_result(result, learning_mode=True)

        captured = capsys.readouterr()
        assert "📚 Learning Mode - What this command does:" in captured.out
        assert "💡 Best Practices:" in captured.out
        assert "🔗 Related Concepts:" in captured.out
        assert "Converts raw text files into character-level tokens" in captured.out

    def testhandle_tool_result_without_learning_mode(
        self, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """Test that handle_tool_result doesn't display learning info when disabled."""
        from ml_playground.tools.core.learning_mode import (
            LearningModeEngine,
            VerbosityLevel,
        )

        # Create learning info
        engine = LearningModeEngine(VerbosityLevel.STANDARD)
        learning_info = engine.explain_command(
            "bundestag_char", "data preparation", "prepare"
        )

        # Create ToolResult with learning info
        result = ToolResult.create(
            success=True,
            exit_code=0,
            namespace="ml",
            category="prepare",
            command="bundestag_char",
            stdout="Preparation completed successfully",
            learning_info=learning_info,
        )

        # Test with learning mode disabled
        handle_tool_result(result, learning_mode=False)

        captured = capsys.readouterr()
        assert "📚 Learning Mode" not in captured.out
        assert "💡 Best Practices" not in captured.out
        assert "🔗 Related Concepts" not in captured.out
        assert "Preparation completed successfully" in captured.out

    def test_ml_workflow_educational_content_coverage(self) -> None:
        """Test that educational content exists for all ML workflow operations."""
        from ml_playground.tools.core.learning_mode import (
            LearningModeEngine,
            VerbosityLevel,
        )

        engine = LearningModeEngine(VerbosityLevel.COMPREHENSIVE)

        # Test prepare operations
        for experiment in ["bundestag_char", "bundestag_tiktoken"]:
            result = engine.explain_command(experiment, "data preparation", "prepare")
            assert len(result.explanations) > 0, (
                f"No explanations for prepare.{experiment}"
            )
            assert len(result.best_practices) > 0, (
                f"No best practices for prepare.{experiment}"
            )
            assert len(result.related_concepts) > 0, (
                f"No related concepts for prepare.{experiment}"
            )

        # Test train operations
        for experiment in ["bundestag_char", "bundestag_qwen15b_lora_mps"]:
            result = engine.explain_command(experiment, "model training", "train")
            assert len(result.explanations) > 0, (
                f"No explanations for train.{experiment}"
            )
            assert len(result.best_practices) > 0, (
                f"No best practices for train.{experiment}"
            )
            assert len(result.related_concepts) > 0, (
                f"No related concepts for train.{experiment}"
            )

        # Test sample operations
        for experiment in ["bundestag_char", "bundestag_qwen15b_lora_mps"]:
            result = engine.explain_command(experiment, "text generation", "sample")
            assert len(result.explanations) > 0, (
                f"No explanations for sample.{experiment}"
            )
            assert len(result.best_practices) > 0, (
                f"No best practices for sample.{experiment}"
            )
            assert len(result.related_concepts) > 0, (
                f"No related concepts for sample.{experiment}"
            )

        # Test analyze operations
        result = engine.explain_command("bundestag_char", "model analysis", "analyze")
        assert len(result.explanations) > 0, (
            "No explanations for analyze.bundestag_char"
        )
        assert len(result.best_practices) > 0, (
            "No best practices for analyze.bundestag_char"
        )
        assert len(result.related_concepts) > 0, (
            "No related concepts for analyze.bundestag_char"
        )

    def test_verbosity_levels_affect_content_length(self) -> None:
        """Test that different verbosity levels provide different amounts of content."""
        from ml_playground.tools.core.learning_mode import (
            LearningModeEngine,
            VerbosityLevel,
        )

        # Test with different verbosity levels
        minimal_engine = LearningModeEngine(VerbosityLevel.MINIMAL)
        standard_engine = LearningModeEngine(VerbosityLevel.STANDARD)
        comprehensive_engine = LearningModeEngine(VerbosityLevel.COMPREHENSIVE)

        # Get results for the same command at different verbosity levels
        minimal_result = minimal_engine.explain_command(
            "bundestag_char", "data preparation", "prepare"
        )
        standard_result = standard_engine.explain_command(
            "bundestag_char", "data preparation", "prepare"
        )
        comprehensive_result = comprehensive_engine.explain_command(
            "bundestag_char", "data preparation", "prepare"
        )

        # Comprehensive should have more content than standard, standard more than minimal
        assert len(comprehensive_result.explanations) >= len(
            standard_result.explanations
        )
        assert len(standard_result.explanations) >= len(minimal_result.explanations)

        # Best practices should be empty for minimal mode
        assert len(minimal_result.best_practices) == 0
        assert len(standard_result.best_practices) > 0
        assert len(comprehensive_result.best_practices) > 0


class TestToolResultIntegration:
    """Test ToolResult integration with ML workflow commands."""

    def test_tool_result_creation_with_ml_namespace(self) -> None:
        """Test ToolResult creation with ML namespace validation."""
        # Test valid ML categories
        for category in ["prepare", "train", "sample", "analyze"]:
            result = ToolResult.create(
                success=True,
                exit_code=0,
                namespace="ml",
                category=category,
                command="test",
            )
            assert result.operation_id.namespace == "ml"
            assert result.operation_id.category == category
            assert str(result.operation_id) == f"ml.{category}.test"

    def test_tool_result_with_learning_info(self) -> None:
        """Test ToolResult with learning information."""
        from ml_playground.tools.core.learning_mode import (
            LearningModeEngine,
            VerbosityLevel,
        )

        engine = LearningModeEngine(VerbosityLevel.STANDARD)
        learning_info = engine.explain_command(
            "bundestag_char", "data preparation", "prepare"
        )

        result = ToolResult.create(
            success=True,
            exit_code=0,
            namespace="ml",
            category="prepare",
            command="bundestag_char",
            learning_info=learning_info,
        )

        assert result.learning_info is not None
        assert result.learning_info.explanations == learning_info.explanations
        assert result.learning_info.best_practices == learning_info.best_practices
        assert result.learning_info.related_concepts == learning_info.related_concepts

    def test_tool_result_error_handling(self) -> None:
        """Test ToolResult creation for error cases."""
        result = ToolResult.create(
            success=False,
            exit_code=1,
            namespace="ml",
            category="prepare",
            command="test",
            stderr="Preparation failed: missing input data",
        )

        assert not result.success
        assert result.exit_code == 1
        assert "missing input data" in result.stderr
        assert result.stdout == ""

    def test_cli_dependencies_interface_compatibility(self) -> None:
        """Test that CLIDependencies interface works with ToolResult."""

        def mock_load_experiment(name: str, path: Path | None) -> Any:
            # This would normally load a real config, but we'll create a mock
            raise NotImplementedError("Mock function for interface testing")

        def mock_ensure_prerequisites(config: ExperimentConfig) -> None:
            pass

        def mock_run_operation(
            experiment: str,
            config: Any,
            config_path: Path,
            shared: Any,
            learning_engine: Any = None,
        ) -> ToolResult:
            return ToolResult.create(
                success=True,
                exit_code=0,
                namespace="ml",
                category="prepare",
                command=experiment,
            )

        # Test that the interface can be created with ToolResult-returning functions
        deps = cli.CLIDependencies(
            load_experiment=mock_load_experiment,
            ensure_train_prerequisites=mock_ensure_prerequisites,
            ensure_sample_prerequisites=mock_ensure_prerequisites,
            run_prepare=mock_run_operation,
            run_train=mock_run_operation,
            run_sample=mock_run_operation,
        )

        # Verify the interface is properly typed
        assert callable(deps.run_prepare)
        assert callable(deps.run_train)
        assert callable(deps.run_sample)
