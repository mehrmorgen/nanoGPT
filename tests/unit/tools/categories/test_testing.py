"""Unit tests for testing tools category - refactored without mocks."""

from __future__ import annotations

from pathlib import Path

import pytest

from ml_playground.tools.categories import testing as testing_module
from ml_playground.tools.core import config as config_module
from ml_playground.tools.core.config import ToolsConfig
from ml_playground.tools.core.interfaces import OperationId
from tests.unit.tools.fakes import FakeSubprocessRunner, create_success_result, create_failure_result


@pytest.fixture
def config() -> ToolsConfig:
    """Create test configuration."""
    return ToolsConfig(
        testing=config_module.TestToolsConfig(
            timeout=300,
            coverage_threshold=80.0,
            parallel_workers=2,
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
def testing_tools(
    config: ToolsConfig, 
    root_path: Path, 
    subprocess_runner: FakeSubprocessRunner
) -> testing_module.TestingTools:
    """Create testing tools instance with fake dependencies."""
    return testing_module.TestingTools(config, root_path, subprocess_runner)


class TestTestingToolsInit:
    """Test TestingTools initialization."""
    
    def test_init(
        self, 
        testing_tools: testing_module.TestingTools, 
        config: ToolsConfig, 
        root_path: Path
    ) -> None:
        """Test initialization."""
        assert testing_tools.config == config
        assert testing_tools.root_path == root_path
        assert testing_tools.cache_dir == root_path / ".cache"
        assert testing_tools.category == "test"


class TestUnitTests:
    """Test unit test execution."""
    
    def test_unit_success(
        self, 
        testing_tools: testing_module.TestingTools, 
        subprocess_runner: FakeSubprocessRunner
    ) -> None:
        """Test successful unit test execution."""
        operation_id = OperationId(namespace="tools", category="test", command="unit")
        expected_result = create_success_result(operation_id, "test output")
        subprocess_runner.set_results([expected_result])
        
        result = testing_tools.unit(["--verbose"])
        
        assert result.success is True
        assert result.exit_code == 0
        assert str(result.operation_id) == "tools.test.unit"
        
        # Check call arguments
        assert len(subprocess_runner.calls) == 1
        call = subprocess_runner.calls[0]
        command = call["command"]
        assert "tests/unit" in command
        assert "--verbose" in command
    
    def test_unit_failure(
        self, 
        testing_tools: testing_module.TestingTools, 
        subprocess_runner: FakeSubprocessRunner
    ) -> None:
        """Test failed unit test execution."""
        operation_id = OperationId(namespace="tools", category="test", command="unit")
        expected_result = create_failure_result(operation_id, 1, "", "test failed")
        subprocess_runner.set_results([expected_result])
        
        result = testing_tools.unit([])
        
        assert result.success is False
        assert result.exit_code == 1


class TestIntegrationTests:
    """Test integration test execution."""
    
    def test_integration_success(
        self, 
        testing_tools: testing_module.TestingTools, 
        subprocess_runner: FakeSubprocessRunner
    ) -> None:
        """Test successful integration test execution."""
        operation_id = OperationId(namespace="tools", category="test", command="integration")
        expected_result = create_success_result(operation_id, "integration test output")
        subprocess_runner.set_results([expected_result])
        
        result = testing_tools.integration([])
        
        assert result.success is True
        assert str(result.operation_id) == "tools.test.integration"
        
        # Check that integration-specific args are included
        assert len(subprocess_runner.calls) == 1
        command = subprocess_runner.calls[0]["command"]
        assert "-m" in command
        assert "integration" in command
        assert "--no-cov" in command


class TestE2ETests:
    """Test end-to-end test execution."""
    
    def test_e2e_success(
        self, 
        testing_tools: testing_module.TestingTools, 
        subprocess_runner: FakeSubprocessRunner
    ) -> None:
        """Test successful e2e test execution."""
        operation_id = OperationId(namespace="tools", category="test", command="e2e")
        expected_result = create_success_result(operation_id, "e2e test output")
        subprocess_runner.set_results([expected_result])
        
        result = testing_tools.e2e([])
        
        assert result.success is True
        assert str(result.operation_id) == "tools.test.e2e"
        
        # Check that e2e path is included
        assert len(subprocess_runner.calls) == 1
        command = subprocess_runner.calls[0]["command"]
        assert "tests/e2e" in command


class TestAcceptanceTests:
    """Test acceptance test execution."""
    
    def test_acceptance_success(
        self, 
        testing_tools: testing_module.TestingTools, 
        subprocess_runner: FakeSubprocessRunner
    ) -> None:
        """Test successful acceptance test execution."""
        operation_id = OperationId(namespace="tools", category="test", command="acceptance")
        expected_result = create_success_result(operation_id, "acceptance test output")
        subprocess_runner.set_results([expected_result])
        
        result = testing_tools.acceptance([])
        
        assert result.success is True
        assert str(result.operation_id) == "tools.test.acceptance"
        
        # Check that acceptance path is included
        assert len(subprocess_runner.calls) == 1
        command = subprocess_runner.calls[0]["command"]
        assert "tests/acceptance" in command


class TestPropertyTests:
    """Test property-based test execution."""
    
    def test_property_success(
        self, 
        testing_tools: testing_module.TestingTools, 
        subprocess_runner: FakeSubprocessRunner
    ) -> None:
        """Test successful property test execution."""
        operation_id = OperationId(namespace="tools", category="test", command="property")
        expected_result = create_success_result(operation_id, "property test output")
        subprocess_runner.set_results([expected_result])
        
        result = testing_tools.property_tests([])
        
        assert result.success is True
        assert str(result.operation_id) == "tools.test.property"
        
        # Check that property path is included
        assert len(subprocess_runner.calls) == 1
        command = subprocess_runner.calls[0]["command"]
        assert "tests/property" in command


class TestAllTests:
    """Test all test execution."""
    
    def test_all_success(
        self, 
        testing_tools: testing_module.TestingTools, 
        subprocess_runner: FakeSubprocessRunner
    ) -> None:
        """Test successful all test execution."""
        operation_id = OperationId(namespace="tools", category="test", command="all")
        expected_result = create_success_result(operation_id, "all test output")
        subprocess_runner.set_results([expected_result])
        
        result = testing_tools.all_tests([])
        
        assert result.success is True
        assert str(result.operation_id) == "tools.test.all"
        
        # Check that tests path is included
        assert len(subprocess_runner.calls) == 1
        command = subprocess_runner.calls[0]["command"]
        assert "tests" in command


class TestCoverageTest:
    """Test coverage test execution."""
    
    def test_coverage_test_success(
        self, 
        testing_tools: testing_module.TestingTools, 
        subprocess_runner: FakeSubprocessRunner
    ) -> None:
        """Test successful coverage test execution."""
        operation_id = OperationId(namespace="tools", category="test", command="coverage-test")
        expected_result = create_success_result(operation_id, "coverage test output")
        subprocess_runner.set_results([expected_result])
        
        result = testing_tools.coverage_test([])
        
        assert result.success is True
        assert str(result.operation_id) == "tools.test.coverage-test"
        
        # Check coverage command construction
        assert len(subprocess_runner.calls) == 1
        command = subprocess_runner.calls[0]["command"]
        assert "coverage" in command
        assert "run" in command
        assert "tests/unit" in command
        assert "tests/property" in command


class TestClean:
    """Test cleaning test artifacts."""
    
    def test_clean_success(
        self, 
        testing_tools: testing_module.TestingTools
    ) -> None:
        """Test successful cleaning."""
        result = testing_tools.clean([])
        
        # The clean method should always succeed, even if no artifacts exist
        assert result.success is True
        # Should contain either "Cleaned" or "No artifacts to clean"
        assert "Cleaned" in result.stdout or "No artifacts to clean" in result.stdout