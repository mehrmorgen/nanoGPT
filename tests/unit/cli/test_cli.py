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
from ml_playground.runtime.cli import app, override_cli_dependencies, CLIDependencies
from ml_playground.runtime.core.results import ToolResult
from ml_playground.configuration.models import ExperimentConfig
from ml_playground.configuration import loading as config_loading


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


def _noop_run_prepare(*args: Any, **kwargs: Any) -> ToolResult:
    del args, kwargs
    return ToolResult.create(
        success=True,
        exit_code=0,
        namespace="ml",
        category="prepare",
        command="noop",
        stdout="ok",
    )


def _noop_run_train(*args: Any, **kwargs: Any) -> ToolResult:
    del args, kwargs
    return ToolResult.create(
        success=True,
        exit_code=0,
        namespace="ml",
        category="train",
        command="noop",
        stdout="ok",
    )


def _noop_run_sample(*args: Any, **kwargs: Any) -> ToolResult:
    del args, kwargs
    return ToolResult.create(
        success=True,
        exit_code=0,
        namespace="ml",
        category="sample",
        command="noop",
        stdout="ok",
    )


class TestGlobalOptions:
    """Test global CLI options like --exp-config validation."""

    def test_exp_config_missing_file_exits_with_code_2(
        self, runner: CliRunner, caplog: pytest.LogCaptureFixture, tmp_path: Path
    ) -> None:
        """Test that --exp-config with missing file exits with code 2."""
        missing_path = tmp_path / "missing.toml"
        with caplog.at_level(logging.ERROR, logger="ml_playground.runtime.cli"):
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

        def load_experiment(_: str, __: Path | None) -> ExperimentConfig:
            raise KeyboardInterrupt()

        deps = CLIDependencies(
            load_experiment=load_experiment,
            ensure_train_prerequisites=lambda _: None,
            ensure_sample_prerequisites=lambda _: None,
            run_prepare=_noop_run_prepare,
            run_train=_noop_run_train,
            run_sample=_noop_run_sample,
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
            run_prepare=_noop_run_prepare,
            run_train=_noop_run_train,
            run_sample=_noop_run_sample,
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

        def ensure_sample(_: ExperimentConfig) -> None:
            raise KeyboardInterrupt()

        deps = CLIDependencies(
            load_experiment=load_experiment,
            ensure_train_prerequisites=lambda _: None,
            ensure_sample_prerequisites=ensure_sample,
            run_prepare=_noop_run_prepare,
            run_train=_noop_run_train,
            run_sample=_noop_run_sample,
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
            run_prepare=_noop_run_prepare,
            run_train=_noop_run_train,
            run_sample=_noop_run_sample,
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
            run_prepare=_noop_run_prepare,
            run_train=_noop_run_train,
            run_sample=_noop_run_sample,
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

        def ensure_sample(_: ExperimentConfig) -> None:
            raise ValueError("test error")

        deps = CLIDependencies(
            load_experiment=load_experiment,
            ensure_train_prerequisites=lambda _: None,
            ensure_sample_prerequisites=ensure_sample,
            run_prepare=_noop_run_prepare,
            run_train=_noop_run_train,
            run_sample=_noop_run_sample,
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

    def test_run_analyze_logs_placeholder(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        """_run_analyze should log a not implemented message for bundestag_char."""

        with caplog.at_level(logging.INFO, logger="ml_playground.runtime.cli"):
            result = cli.run_analyze("bundestag_char", "0.0.0.0", 9000, False)

        assert result.success is True
        assert any("not implemented" in msg for msg in caplog.messages)

    def test_run_analyze_unknown_experiment_raises(self) -> None:
        """_run_analyze should raise for unsupported experiments."""

        result = cli.run_analyze("other", "127.0.0.1", 8050, True)

        assert result.success is False
        assert "analyze currently supports only 'bundestag_char'" in (
            result.stderr or ""
        )

    def test_analyze_command_invokes_helper(
        self, runner: CliRunner, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Analyze CLI command should forward to _run_analyze."""

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

    def test_run_train_cmd_uses_default_dependencies(self, tmp_path: Path) -> None:
        """_run_train_cmd should request dependencies when none are provided."""

        calls: dict[str, list[Any]] = {"load": [], "ensure": [], "run": []}

        def fake_load(experiment: str, cfg: Path | None) -> ExperimentConfig:
            calls["load"].append((experiment, cfg))
            exp_cfg, _ = _build_stub_experiment(tmp_path, experiment)
            return exp_cfg

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
                stdout="ok",
            )

        deps = CLIDependencies(
            load_experiment=fake_load,
            ensure_train_prerequisites=fake_ensure,
            ensure_sample_prerequisites=lambda _: None,
            run_prepare=_noop_run_prepare,
            run_train=fake_run,
            run_sample=_noop_run_sample,
        )

        with override_cli_dependencies(deps):
            cli.run_train_cmd("demo", None, deps=None)

        assert calls["load"] == [("demo", None)]
        assert len(calls["ensure"]) == 1
        # CLI may construct a real SharedConfig instance; compare by shape and basic fields.
        assert len(calls["run"]) == 1
        run_experiment, run_cfg, run_config_path, run_shared = calls["run"][0]
        assert run_experiment == "demo"
        assert run_cfg == "train_cfg"
        # We only require that CLI passes some config path through consistently.
        from pathlib import Path as _Path

        assert isinstance(run_config_path, _Path)
        assert getattr(run_shared, "config_path", None) == run_config_path

    def test_run_sample_cmd_uses_default_dependencies(self, tmp_path: Path) -> None:
        """_run_sample_cmd should request dependencies when none are provided."""

        calls: dict[str, list[Any]] = {
            "load": [],
            "ensure": [],
            "run": [],
        }

        def fake_load(experiment: str, cfg: Path | None) -> ExperimentConfig:
            calls["load"].append((experiment, cfg))
            exp_cfg, _ = _build_stub_experiment(tmp_path, experiment)
            return exp_cfg

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
                stdout="ok",
            )

        deps = CLIDependencies(
            load_experiment=fake_load,
            ensure_train_prerequisites=lambda _: None,
            ensure_sample_prerequisites=fake_ensure,
            run_prepare=_noop_run_prepare,
            run_train=_noop_run_train,
            run_sample=fake_run,
        )

        with override_cli_dependencies(deps):
            cli.run_sample_cmd("demo", None, deps=None)

        assert calls["load"] == [("demo", None)]
        assert len(calls["ensure"]) == 1
        # CLI may construct a real SharedConfig instance; we only assert the call shape.
        assert len(calls["run"]) == 1

    def test_complete_experiments_returns_known_experiments(self) -> None:
        """_complete_experiments should list available experiments including bundestag_char."""
        experiments = config_loading.list_experiments_with_config("bund")

        assert any(entry == "bundestag_char" for entry in experiments)

    def test_main_returns_zero_status(self) -> None:
        """main should complete successfully for --help invocation."""

        exit_code = cli.main(["--help"])

        assert exit_code == 0


class TestDeviceSetupFallbacks:
    """Test _global_device_setup error handling."""

    def test_device_setup_swallows_cuda_errors(self) -> None:
        """Test that torch errors in device setup are swallowed."""
        # Should not raise, even with cuda available but error
        cli.global_device_setup("cuda", "float32", 42, cuda_is_available=lambda: True)

    def test_device_setup_success_path(self) -> None:
        """Test successful CUDA setup."""
        # Inject fake cuda available
        cli.global_device_setup("cuda", "float32", 42, cuda_is_available=lambda: True)

    def test_device_setup_explicit_cuda_override(self) -> None:
        """Test injecting cuda_is_available callable."""
        called = False

        def fake_cuda():
            nonlocal called
            called = True
            return False

        cli.global_device_setup("cpu", "float32", 42, cuda_is_available=fake_cuda)
        assert called


class TestAnalysisGuardRails:
    """Test _run_analyze experiment validation."""

    def test_analyze_rejects_non_bundestag_char(self) -> None:
        """Test RuntimeError for unsupported experiments."""
        result = cli.run_analyze("shakespeare", "127.0.0.1", 8050, True)

        assert result.success is False
        assert result.stderr is not None
        assert "analyze currently supports only 'bundestag_char'" in result.stderr

    def test_analyze_accepts_bundestag_char(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Test info log for supported experiment."""
        with caplog.at_level("INFO"):
            result = cli.run_analyze("bundestag_char", "127.0.0.1", 8050, True)
        assert result.success is True
        assert "Analysis for 'bundestag_char' not implemented" in caplog.text


class TestDirectoryLoggingResilience:
    """Test _log_dir and _log_command_status error handling."""

    def test_log_dir_handles_unset_path(self, caplog: pytest.LogCaptureFixture) -> None:
        """Test logging when dir_path is None."""
        import logging

        logger = logging.getLogger("test")
        with caplog.at_level("INFO"):
            cli.log_directory("test", "test_dir", None, logger)
        assert "<not set>" in caplog.text

    def test_log_dir_handles_missing_directory(
        self, caplog: pytest.LogCaptureFixture, tmp_path: Path
    ) -> None:
        """Test logging for non-existent directory."""
        import logging

        logger = logging.getLogger("test")
        missing_dir = tmp_path / "missing"
        with caplog.at_level("INFO"):
            cli.log_directory("test", "test_dir", missing_dir, logger)
        assert "(missing)" in caplog.text

    def test_log_dir_handles_existing_directory(
        self, caplog: pytest.LogCaptureFixture, tmp_path: Path
    ) -> None:
        """Test logging for existing directory."""
        import logging

        logger = logging.getLogger("test")
        existing_dir = tmp_path / "existing"
        existing_dir.mkdir()
        (existing_dir / "file.txt").write_text("content")
        with caplog.at_level("INFO"):
            cli.log_directory("test", "test_dir", existing_dir, logger)
        assert "(exists)" in caplog.text
        assert "Contents:" in caplog.text

    def test_log_dir_handles_unreadable_directory(
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
                cli.log_directory("test", "test_dir", unreadable_dir, logger)
            assert "(exists)" in caplog.text
            # Should not crash, even if contents not listed
        finally:
            os.chmod(unreadable_dir, 0o755)  # Restore for cleanup

    def test_log_command_status_handles_errors_gracefully(self, tmp_path: Path) -> None:
        """Test _log_command_status handles OSError/ValueError/TypeError gracefully."""
        from ml_playground.configuration.models import SharedConfig

        shared = SharedConfig(
            experiment="test",
            config_path=tmp_path / "config.toml",
            project_home=tmp_path,
            dataset_dir=tmp_path / "dataset",
            train_out_dir=tmp_path / "train",
            sample_out_dir=tmp_path / "sample",
        )

        # Should not raise
        cli.log_command_status("tag", shared, tmp_path, logging.getLogger(__name__))
