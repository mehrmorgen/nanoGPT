"""Unit tests for environment tools category."""

from __future__ import annotations

from pathlib import Path

import pytest

from ml_playground.tools.categories import environment as environment_module
from ml_playground.tools.core import config as config_module
from ml_playground.tools.core.config import ToolsConfig
from ml_playground.tools.core.errors import ToolExecutionError
from ml_playground.tools.core.interfaces import OperationId
from tests.unit.tools.fakes import FakeSubprocessRunner, create_success_result, create_failure_result


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
    config: ToolsConfig, 
    root_path: Path, 
    subprocess_runner: FakeSubprocessRunner
) -> environment_module.EnvironmentTools:
    """Create environment tools instance with fake dependencies."""
    return environment_module.EnvironmentTools(config, root_path, subprocess_runner)


class TestEnvironmentToolsInit:
    """Test EnvironmentTools initialization."""
    
    def test_init(
        self, 
        environment_tools: environment_module.EnvironmentTools, 
        config: ToolsConfig, 
        root_path: Path
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
        subprocess_runner: FakeSubprocessRunner
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


class TestSync:
    """Test dependency synchronization functionality."""
    
    def test_sync_success(
        self, 
        environment_tools: environment_module.EnvironmentTools, 
        subprocess_runner: FakeSubprocessRunner
    ) -> None:
        """Test successful sync execution."""
        operation_id = OperationId(namespace="tools", category="env", command="sync")
        expected_result = create_success_result(operation_id, "Synchronized dependencies")
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
        subprocess_runner: FakeSubprocessRunner
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
        subprocess_runner: FakeSubprocessRunner
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
        subprocess_runner: FakeSubprocessRunner
    ) -> None:
        """Test successful package verification."""
        operation_id = OperationId(namespace="tools", category="env", command="verify")
        expected_result = create_success_result(operation_id, "✓ ml_playground import OK")
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
        subprocess_runner: FakeSubprocessRunner
    ) -> None:
        """Test failed package verification."""
        operation_id = OperationId(namespace="tools", category="env", command="verify")
        expected_result = create_failure_result(operation_id, 1, "", "ImportError: No module named 'ml_playground'")
        subprocess_runner.set_results([expected_result])
        
        result = environment_tools.verify([])
        
        assert result.success is False
        assert result.exit_code == 1


class TestClean:
    """Test cleanup functionality."""
    
    def test_clean_success(
        self, 
        environment_tools: environment_module.EnvironmentTools
    ) -> None:
        """Test successful cleanup."""
        result = environment_tools.clean([])
        
        # The clean method should always succeed
        assert result.success is True
        assert str(result.operation_id) == "tools.env.clean"
        # Should contain information about cleanup
        assert "Cache" in result.stdout or "Cleaned" in result.stdout


class TestInfo:
    """Test environment info functionality."""
    
    def test_info_success(
        self, 
        environment_tools: environment_module.EnvironmentTools, 
        subprocess_runner: FakeSubprocessRunner
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


class TestAIGuidelines:
    """Test AI guidelines functionality."""
    
    def test_ai_guidelines_success(
        self, 
        environment_tools: environment_module.EnvironmentTools, 
        subprocess_runner: FakeSubprocessRunner
    ) -> None:
        """Test successful AI guidelines setup."""
        operation_id = OperationId(namespace="tools", category="env", command="ai-guidelines")
        expected_result = create_success_result(operation_id, "AI guidelines set up for ruff")
        subprocess_runner.set_results([expected_result])
        
        result = environment_tools.ai_guidelines([], tool="ruff", dry_run=False)
        
        assert result.success is True
        assert str(result.operation_id) == "tools.env.ai-guidelines"
        
        # Check command construction
        assert len(subprocess_runner.calls) == 1
        command = subprocess_runner.calls[0]["command"]
        assert "python" in command
        assert "tools/setup_ai_guidelines.py" in command
        assert "ruff" in command
    
    def test_ai_guidelines_empty_tool(
        self, 
        environment_tools: environment_module.EnvironmentTools
    ) -> None:
        """Test AI guidelines with empty tool name."""
        with pytest.raises(ToolExecutionError) as exc_info:
            environment_tools.ai_guidelines([], tool="", dry_run=False)
        
        assert "Missing tool name" in str(exc_info.value)