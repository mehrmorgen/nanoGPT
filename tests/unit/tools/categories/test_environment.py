"""Unit tests for environment tools category."""

from __future__ import annotations

from pathlib import Path

import pytest

from ml_playground.tools import environment as environment_module
from ml_playground.tools.core import config as config_module
from ml_playground.tools.core.config import ToolsConfig
from ml_playground.tools.core.errors import ToolExecutionError
from ml_playground.tools.core.interfaces import OperationId
from tests.unit.tools.fakes import (
    FakeSubprocessRunner,
    create_success_result,
    create_failure_result,
)


@pytest.fixture
def config() -> ToolsConfig:
    """Create test configuration."""
    return ToolsConfig(
        environment=config_module.EnvironmentToolsConfig(
            timeout=300,
        )
    )


@pytest.fixture
def root_path(tmp_path: Path) -> Path:
    """Create temporary root path."""
    return tmp_path


@pytest.fixture
def subprocess_runner() -> FakeSubprocessRunner:
    """Create fake subprocess runner."""
    return FakeSubprocessRunner()


@pytest.fixture
def environment_tools(
    config: ToolsConfig, root_path: Path, subprocess_runner: FakeSubprocessRunner
) -> environment_module.EnvironmentTools:
    """Create environment tools instance with fake dependencies."""
    return environment_module.EnvironmentTools(config, root_path, subprocess_runner)


class TestEnvironmentToolsInit:
    """Test EnvironmentTools initialization."""

    def test_init(
        self,
        environment_tools: environment_module.EnvironmentTools,
        config: ToolsConfig,
        root_path: Path,
    ) -> None:
        """Test initialization."""
        assert environment_tools.config == config
        assert environment_tools.root_path == root_path
        assert environment_tools.cache_dir == root_path / ".cache"
        assert environment_tools.venv_path == root_path / ".venv"
        assert environment_tools.pkg_name == "ml_playground"
        assert environment_tools.category == "env"


class TestSetup:
    """Test environment setup functionality."""

    def test_setup_success(
        self,
        environment_tools: environment_module.EnvironmentTools,
        subprocess_runner: FakeSubprocessRunner,
    ) -> None:
        """Test successful environment setup."""
        operation_id = OperationId(namespace="tools", category="env", command="setup")
        # Mock both venv and sync commands
        venv_result = create_success_result(operation_id, "Created virtual environment")
        sync_result = create_success_result(operation_id, "Synchronized dependencies")
        subprocess_runner.set_results([venv_result, sync_result])

        result = environment_tools.setup([])

        assert result.success is True
        assert str(result.operation_id) == "tools.env.setup"

        # Check that both venv and sync commands were called
        assert len(subprocess_runner.calls) == 2
        venv_command = subprocess_runner.calls[0]["command"]
        sync_command = subprocess_runner.calls[1]["command"]
        assert "venv" in venv_command
        assert "sync" in sync_command
        assert "--all-groups" in sync_command

    def test_setup_clear_removes_existing_env(
        self,
        environment_tools: environment_module.EnvironmentTools,
        subprocess_runner: FakeSubprocessRunner,
        root_path: Path,
    ) -> None:
        """Ensure `clear=True` removes an existing virtual environment."""
        venv_path = environment_tools.venv_path
        venv_path.mkdir(parents=True, exist_ok=True)
        (venv_path / "placeholder.txt").write_text("keep me", encoding="utf-8")

        operation_id = OperationId(namespace="tools", category="env", command="setup")
        subprocess_runner.set_results(
            [
                create_success_result(operation_id, "venv ok"),
                create_success_result(operation_id, "sync ok"),
            ]
        )

        result = environment_tools.setup([], clear=True)

        assert result.success is True
        assert "Removed existing virtual environment" in result.stdout
        assert not venv_path.exists()

    def test_setup_git_hook_failure_records_warning(
        self,
        environment_tools: environment_module.EnvironmentTools,
        subprocess_runner: FakeSubprocessRunner,
        root_path: Path,
    ) -> None:
        """Git hook failure should not abort setup but emit a warning."""
        git_dir = root_path / ".git"
        git_dir.mkdir()
        hooks_file = git_dir / "hooks"
        hooks_file.write_text("not a directory", encoding="utf-8")

        operation_id = OperationId(namespace="tools", category="env", command="setup")
        subprocess_runner.set_results(
            [
                create_success_result(operation_id, "venv ok"),
                create_success_result(operation_id, "sync ok"),
            ]
        )

        result = environment_tools.setup([])

        assert result.success is True
        assert "Warning: Git hooks setup failed" in result.stdout

    def test_setup_propogates_failure_from_uv_command(
        self,
        environment_tools: environment_module.EnvironmentTools,
        subprocess_runner: FakeSubprocessRunner,
    ) -> None:
        """If the initial uv venv command fails, setup should stop and bubble the failure."""

        operation_id = OperationId(namespace="tools", category="env", command="setup")
        subprocess_runner.set_results(
            [
                create_failure_result(operation_id, exit_code=2, stderr="uv failed"),
                create_success_result(operation_id, "sync ok"),
            ]
        )

        result = environment_tools.setup([])

        assert result.success is False
        assert result.exit_code == 2
        assert len(subprocess_runner.calls) == 1
        assert "uv failed" in (result.stderr or "")


class TestSync:
    """Test dependency synchronization functionality."""

    def test_sync_success(
        self,
        environment_tools: environment_module.EnvironmentTools,
        subprocess_runner: FakeSubprocessRunner,
    ) -> None:
        """Test successful sync execution."""
        operation_id = OperationId(namespace="tools", category="env", command="sync")
        expected_result = create_success_result(
            operation_id, "Synchronized dependencies"
        )
        subprocess_runner.set_results([expected_result])

        result = environment_tools.sync([])

        assert result.success is True
        assert str(result.operation_id) == "tools.env.sync"

        # Check basic sync command
        assert len(subprocess_runner.calls) == 1
        command = subprocess_runner.calls[0]["command"]
        assert "sync" in command

    def test_sync_with_groups(
        self,
        environment_tools: environment_module.EnvironmentTools,
        subprocess_runner: FakeSubprocessRunner,
    ) -> None:
        """Test sync with specific groups."""
        operation_id = OperationId(namespace="tools", category="env", command="sync")
        expected_result = create_success_result(operation_id)
        subprocess_runner.set_results([expected_result])

        result = environment_tools.sync([], groups=["dev", "test"])

        assert result.success is True

        # Check groups are included
        assert len(subprocess_runner.calls) == 1
        command = subprocess_runner.calls[0]["command"]
        assert "--group" in command
        assert "dev" in command
        assert "test" in command

    def test_sync_all_groups(
        self,
        environment_tools: environment_module.EnvironmentTools,
        subprocess_runner: FakeSubprocessRunner,
    ) -> None:
        """Test sync with all groups."""
        operation_id = OperationId(namespace="tools", category="env", command="sync")
        expected_result = create_success_result(operation_id)
        subprocess_runner.set_results([expected_result])

        result = environment_tools.sync([], all_groups=True)

        assert result.success is True

        # Check all-groups flag
        assert len(subprocess_runner.calls) == 1
        command = subprocess_runner.calls[0]["command"]
        assert "--all-groups" in command


class TestVerify:
    """Test package verification functionality."""

    def test_verify_success(
        self,
        environment_tools: environment_module.EnvironmentTools,
        subprocess_runner: FakeSubprocessRunner,
    ) -> None:
        """Test successful package verification."""
        operation_id = OperationId(namespace="tools", category="env", command="verify")
        expected_result = create_success_result(
            operation_id, "✓ ml_playground import OK"
        )
        subprocess_runner.set_results([expected_result])

        result = environment_tools.verify([])

        assert result.success is True
        assert str(result.operation_id) == "tools.env.verify"

        # Check import command
        assert len(subprocess_runner.calls) == 1
        command = subprocess_runner.calls[0]["command"]
        assert "python" in command
        assert "-c" in command
        assert "import ml_playground" in " ".join(command)

    def test_verify_failure(
        self,
        environment_tools: environment_module.EnvironmentTools,
        subprocess_runner: FakeSubprocessRunner,
    ) -> None:
        """Test failed package verification."""
        operation_id = OperationId(namespace="tools", category="env", command="verify")
        expected_result = create_failure_result(
            operation_id, 1, "", "ImportError: No module named 'ml_playground'"
        )
        subprocess_runner.set_results([expected_result])

        result = environment_tools.verify([])

        assert result.success is False
        assert result.exit_code == 1


class TestClean:
    """Test cleanup functionality."""

    def test_clean_success(
        self, environment_tools: environment_module.EnvironmentTools
    ) -> None:
        """Test successful cleanup."""
        result = environment_tools.clean([])

        # The clean method should always succeed
        assert result.success is True
        assert str(result.operation_id) == "tools.env.clean"
        # Should contain information about cleanup
        assert "Cache" in result.stdout or "Cleaned" in result.stdout

    def test_clean_removes_cache_and_pycache(
        self, environment_tools: environment_module.EnvironmentTools
    ) -> None:
        """Ensure cache directories and __pycache__ folders are removed."""
        cache_pytest = environment_tools.cache_dir / "pytest"
        cache_pytest.mkdir(parents=True, exist_ok=True)
        (cache_pytest / "state.bin").write_bytes(b"binary")

        pycache_dir = environment_tools.root_path / "src" / "__pycache__"
        pycache_dir.mkdir(parents=True, exist_ok=True)
        (pycache_dir / "module.cpython-311.pyc").write_bytes(b"code")

        result = environment_tools.clean([])

        assert pycache_dir.exists() is False
        assert "Removed" in result.stdout
        assert "Cleaned" in result.stdout


class TestInfo:
    """Test environment info functionality."""

    def test_info_success(
        self,
        environment_tools: environment_module.EnvironmentTools,
        subprocess_runner: FakeSubprocessRunner,
    ) -> None:
        """Test successful info display."""
        operation_id = OperationId(namespace="tools", category="env", command="info")
        expected_result = create_success_result(operation_id, "OK")
        subprocess_runner.set_results([expected_result])

        result = environment_tools.info([])

        assert result.success is True
        assert str(result.operation_id) == "tools.env.info"
        assert "Project root:" in result.stdout
        assert "Package name: ml_playground" in result.stdout
        assert "Virtual environment:" in result.stdout

    def test_info_reports_import_failure(
        self,
        environment_tools: environment_module.EnvironmentTools,
        subprocess_runner: FakeSubprocessRunner,
    ) -> None:
        """If import command fails, info output notes the failure."""
        operation_id = OperationId(namespace="tools", category="env", command="info")
        subprocess_runner.set_results(
            [create_failure_result(operation_id, stderr="Import failed")]
        )

        result = environment_tools.info([])

        assert "import failed" not in result.stderr
        assert "Package import: ✗" in result.stdout

    def test_info_catches_runner_exceptions(
        self, config: ToolsConfig, root_path: Path
    ) -> None:
        """Exceptions from the subprocess runner should be converted into a warning line."""

        class RaisingRunner(FakeSubprocessRunner):
            def run_uv_command(self, *args, **kwargs):  # type: ignore[override]
                raise RuntimeError("runner blew up")

        runner = RaisingRunner()
        tools = environment_module.EnvironmentTools(config, root_path, runner)

        result = tools.info([])

        assert result.success is True
        assert "Could not test ml_playground import" in result.stdout


class TestAIGuidelines:
    """Test AI guidelines functionality."""

    def test_ai_guidelines_success(
        self,
        environment_tools: environment_module.EnvironmentTools,
    ) -> None:
        """Test successful AI guidelines setup."""
        result = environment_tools.ai_guidelines([], tool="windsurf", dry_run=True)

        assert result.success is True
        assert str(result.operation_id) == "tools.env.ai-guidelines"
        # Should produce informative output
        assert "[dry-run]" in result.stdout or "done." in result.stdout

    def test_ai_guidelines_empty_tool(
        self, environment_tools: environment_module.EnvironmentTools
    ) -> None:
        """Test AI guidelines with empty tool name."""
        with pytest.raises(ToolExecutionError) as exc_info:
            environment_tools.ai_guidelines([], tool="", dry_run=False)

        assert "Missing tool name" in str(exc_info.value)

    def test_ai_guidelines_dry_run_includes_flag(
        self,
        environment_tools: environment_module.EnvironmentTools,
    ) -> None:
        """Dry run should append the --dry-run flag."""
        result = environment_tools.ai_guidelines([], tool="windsurf", dry_run=True)
        assert result.success is True
        assert "[dry-run]" in result.stdout


class TestTensorboard:
    """Test tensorboard helper."""

    def test_tensorboard_success(
        self,
        environment_tools: environment_module.EnvironmentTools,
        subprocess_runner: FakeSubprocessRunner,
        root_path: Path,
    ) -> None:
        logdir = root_path / "logs"
        logdir.mkdir()

        operation_id = OperationId(
            namespace="tools", category="env", command="tensorboard"
        )
        subprocess_runner.set_results([create_success_result(operation_id, "tb")])

        result = environment_tools.tensorboard([], logdir)

        assert result.success is True
        command = subprocess_runner.calls[0]["command"]
        assert command[0] == "uv"
        assert command[1] == "run"
        assert "tensorboard" in command
        assert "--logdir" in command
        assert logdir.as_posix() in command

    def test_tensorboard_missing_dir_raises(
        self, environment_tools: environment_module.EnvironmentTools
    ) -> None:
        with pytest.raises(ToolExecutionError):
            environment_tools.tensorboard([], environment_tools.root_path / "missing")

    def test_tensorboard_non_directory_raises(
        self,
        environment_tools: environment_module.EnvironmentTools,
        root_path: Path,
    ) -> None:
        file_path = root_path / "log.txt"
        file_path.write_text("not a directory", encoding="utf-8")

        with pytest.raises(ToolExecutionError):
            environment_tools.tensorboard([], file_path)


class TestGgufHelp:
    """Test GGUF help behavior."""

    def test_gguf_help_adjusts_exit_code(
        self,
        environment_tools: environment_module.EnvironmentTools,
        subprocess_runner: FakeSubprocessRunner,
    ) -> None:
        operation_id = OperationId(
            namespace="tools", category="env", command="gguf-help"
        )
        failure = create_failure_result(
            operation_id,
            exit_code=2,
            stdout="usage: convert-hf-to-gguf",
            stderr="",
        )
        subprocess_runner.set_results([failure])

        result = environment_tools.gguf_help([])

        assert result.success is True
        assert result.exit_code == 0
        assert "GGUF converter help displayed" in result.stderr

    def test_gguf_help_returns_original_failure_when_help_missing(
        self,
        environment_tools: environment_module.EnvironmentTools,
        subprocess_runner: FakeSubprocessRunner,
    ) -> None:
        operation_id = OperationId(
            namespace="tools", category="env", command="gguf-help"
        )
        failure = create_failure_result(
            operation_id,
            exit_code=5,
            stdout="",
            stderr="no help available",
        )
        subprocess_runner.set_results([failure])

        result = environment_tools.gguf_help([])

        assert result.success is False
        assert result.exit_code == 5
        assert result.stderr == "no help available"
