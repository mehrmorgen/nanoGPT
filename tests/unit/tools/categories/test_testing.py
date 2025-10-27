"""Unit tests for testing tools category."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import Mock, patch

import pytest

from ml_playground.tools.categories import testing as testing_module
from ml_playground.tools.core import config as config_module
from ml_playground.tools.core.config import ToolsConfig
from ml_playground.tools.core.errors import ToolExecutionError
from ml_playground.tools.core.interfaces import OperationId, ToolResult


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
def testing_tools(config: ToolsConfig, root_path: Path) -> testing_module.TestingTools:
    """Create testing tools instance."""
    return testing_module.TestingTools(config, root_path)


class TestTestingToolsInit:
    """Test TestingTools initialization."""
    
    def test_init(self, testing_tools: testing_module.TestingTools, config: ToolsConfig, root_path: Path) -> None:
        """Test initialization."""
        assert testing_tools.config == config
        assert testing_tools.root_path == root_path
        assert testing_tools.cache_dir == root_path / ".cache"
        assert testing_tools.category == "test"


class TestUnitTests:
    """Test unit test execution."""
    
    def test_unit_success(self, testing_tools: testing_module.TestingTools) -> None:
        """Test successful unit test execution."""
        with patch("ml_playground.tools.utils.subprocess_utils.run_pytest_command") as mock_run:
            mock_result = ToolResult(
                success=True,
                exit_code=0,
                stdout="test output",
                stderr="",
                operation_id=OperationId(namespace="tools", category="test", command="unit"),
            )
            mock_run.return_value = mock_result
            
            result = testing_tools.unit(["--verbose"])
            
            assert result.success is True
            assert result.exit_code == 0
            assert str(result.operation_id) == "tools.test.unit"
            
            # Check call arguments
            mock_run.assert_called_once()
            call_args = mock_run.call_args
            assert "tests/unit" in call_args[0][0]
            assert "--verbose" in call_args[0][0]
    
    def test_unit_failure(self, testing_tools: testing_module.TestingTools) -> None:
        """Test failed unit test execution."""
        with patch("ml_playground.tools.utils.subprocess_utils.run_pytest_command") as mock_run:
            mock_result = ToolResult(
                success=False,
                exit_code=1,
                stdout="",
                stderr="test failed",
                operation_id=OperationId(namespace="tools", category="test", command="unit"),
            )
            mock_run.return_value = mock_result
            
            result = testing_tools.unit([])
            
            assert result.success is False
            assert result.exit_code == 1


class TestIntegrationTests:
    """Test integration test execution."""
    
    def test_integration_success(self, testing_tools: testing_module.TestingTools) -> None:
        """Test successful integration test execution."""
        with patch("ml_playground.tools.utils.subprocess_utils.run_pytest_command") as mock_run:
            mock_result = ToolResult(
                success=True,
                exit_code=0,
                stdout="integration test output",
                stderr="",
                operation_id=OperationId(namespace="tools", category="test", command="integration"),
            )
            mock_run.return_value = mock_result
            
            result = testing_tools.integration([])
            
            assert result.success is True
            assert str(result.operation_id) == "tools.test.integration"
            
            # Check that integration-specific args are included
            call_args = mock_run.call_args[0][0]
            assert "-m" in call_args
            assert "integration" in call_args
            assert "--no-cov" in call_args


class TestE2ETests:
    """Test end-to-end test execution."""
    
    def test_e2e_success(self, testing_tools: testing_module.TestingTools) -> None:
        """Test successful e2e test execution."""
        with patch("ml_playground.tools.utils.subprocess_utils.run_pytest_command") as mock_run:
            mock_result = ToolResult(
                success=True,
                exit_code=0,
                stdout="e2e test output",
                stderr="",
                operation_id=OperationId(namespace="tools", category="test", command="e2e"),
            )
            mock_run.return_value = mock_result
            
            result = testing_tools.e2e([])
            
            assert result.success is True
            assert str(result.operation_id) == "tools.test.e2e"
            
            # Check that e2e path is included
            call_args = mock_run.call_args[0][0]
            assert "tests/e2e" in call_args


class TestAcceptanceTests:
    """Test acceptance test execution."""
    
    def test_acceptance_success(self, testing_tools: testing_module.TestingTools) -> None:
        """Test successful acceptance test execution."""
        with patch("ml_playground.tools.utils.subprocess_utils.run_pytest_command") as mock_run:
            mock_result = ToolResult(
                success=True,
                exit_code=0,
                stdout="acceptance test output",
                stderr="",
                operation_id=OperationId(namespace="tools", category="test", command="acceptance"),
            )
            mock_run.return_value = mock_result
            
            result = testing_tools.acceptance([])
            
            assert result.success is True
            assert str(result.operation_id) == "tools.test.acceptance"
            
            # Check that acceptance path is included
            call_args = mock_run.call_args[0][0]
            assert "tests/acceptance" in call_args


class TestPropertyTests:
    """Test property-based test execution."""
    
    def test_property_success(self, testing_tools: testing_module.TestingTools) -> None:
        """Test successful property test execution."""
        with patch("ml_playground.tools.utils.subprocess_utils.run_pytest_command") as mock_run:
            mock_result = ToolResult(
                success=True,
                exit_code=0,
                stdout="property test output",
                stderr="",
                operation_id=OperationId(namespace="tools", category="test", command="property"),
            )
            mock_run.return_value = mock_result
            
            result = testing_tools.property_tests([])
            
            assert result.success is True
            assert str(result.operation_id) == "tools.test.property"
            
            # Check that property path is included
            call_args = mock_run.call_args[0][0]
            assert "tests/property" in call_args


class TestAllTests:
    """Test all test execution."""
    
    def test_all_success(self, testing_tools: testing_module.TestingTools) -> None:
        """Test successful all test execution."""
        with patch("ml_playground.tools.utils.subprocess_utils.run_pytest_command") as mock_run:
            mock_result = ToolResult(
                success=True,
                exit_code=0,
                stdout="all test output",
                stderr="",
                operation_id=OperationId(namespace="tools", category="test", command="all"),
            )
            mock_run.return_value = mock_result
            
            result = testing_tools.all_tests([])
            
            assert result.success is True
            assert str(result.operation_id) == "tools.test.all"
            
            # Check that tests path is included
            call_args = mock_run.call_args[0][0]
            assert "tests" in call_args


class TestCoverageTest:
    """Test coverage test execution."""
    
    def test_coverage_test_success(self, testing_tools: testing_module.TestingTools) -> None:
        """Test successful coverage test execution."""
        with patch("ml_playground.tools.utils.subprocess_utils.run_uv_command") as mock_run:
            mock_result = ToolResult(
                success=True,
                exit_code=0,
                stdout="coverage test output",
                stderr="",
                operation_id=OperationId(namespace="tools", category="test", command="coverage-test"),
            )
            mock_run.return_value = mock_result
            
            # Mock coverage file operations
            with patch.object(Path, "exists", return_value=False):
                with patch.object(Path, "unlink"):
                    with patch.object(Path, "glob", return_value=[]):
                        result = testing_tools.coverage_test([])
            
            assert result.success is True
            assert str(result.operation_id) == "tools.test.coverage-test"
            
            # Check coverage command construction
            call_args = mock_run.call_args[0][0]
            assert "coverage" in call_args
            assert "run" in call_args
            assert "tests/unit" in call_args
            assert "tests/property" in call_args


class TestCoverageReport:
    """Test coverage report generation."""
    
    def test_coverage_report_success(self, testing_tools: testing_module.TestingTools) -> None:
        """Test successful coverage report generation."""
        coverage_file = testing_tools._coverage_file()
        
        with patch.object(coverage_file, "exists", return_value=True):
            with patch.object(coverage_file, "stat") as mock_stat:
                mock_stat.return_value.st_size = 1000  # Non-empty file
                
                with patch("ml_playground.tools.utils.subprocess_utils.run_uv_command") as mock_run:
                    mock_result = ToolResult(
                        success=True,
                        exit_code=0,
                        stdout="",
                        stderr="",
                        operation_id=OperationId(namespace="tools", category="test", command="coverage-report"),
                    )
                    mock_run.return_value = mock_result
                    
                    result = testing_tools.coverage_report([], fail_under=80.0, verbose=False)
                    
                    assert result.success is True
                    assert "Generated terminal report" in result.stdout
    
    def test_coverage_report_missing_file(self, testing_tools: testing_module.TestingTools) -> None:
        """Test coverage report with missing coverage file."""
        coverage_file = testing_tools._coverage_file()
        
        with patch.object(coverage_file, "exists", return_value=False):
            with pytest.raises(ToolExecutionError) as exc_info:
                testing_tools.coverage_report([])
            
            assert "Coverage data file not found" in str(exc_info.value)
    
    def test_coverage_report_empty_file_in_ci(self, testing_tools: testing_module.TestingTools) -> None:
        """Test coverage report with empty file in CI."""
        coverage_file = testing_tools._coverage_file()
        
        with patch.object(coverage_file, "exists", return_value=True):
            with patch.object(coverage_file, "stat") as mock_stat:
                mock_stat.return_value.st_size = 0  # Empty file
                
                with patch.dict("os.environ", {"CI": "true"}):
                    with pytest.raises(ToolExecutionError) as exc_info:
                        testing_tools.coverage_report([])
                    
                    assert "Coverage data file is empty" in str(exc_info.value)


class TestCoverageThreshold:
    """Test coverage threshold checking."""
    
    def test_coverage_threshold_success(self, testing_tools: testing_module.TestingTools) -> None:
        """Test successful coverage threshold check."""
        coverage_file = testing_tools._coverage_file()
        json_path = coverage_file.parent / "coverage.json"
        
        # Mock coverage data
        coverage_data = {
            "totals": {
                "num_statements": 100,
                "covered_lines": 90,
                "num_branches": 50,
                "covered_branches": 45,
            }
        }
        
        with patch.object(coverage_file, "exists", return_value=True):
            with patch("ml_playground.tools.utils.subprocess_utils.run_uv_command") as mock_run:
                mock_result = ToolResult(
                    success=True,
                    exit_code=0,
                    stdout="",
                    stderr="",
                    operation_id=OperationId(namespace="tools", category="test", command="coverage-threshold"),
                )
                mock_run.return_value = mock_result
                
                with patch.object(json_path, "open") as mock_open:
                    mock_open.return_value.__enter__.return_value = Mock()
                    with patch("json.load", return_value=coverage_data):
                        result = testing_tools.coverage_threshold(
                            [], 
                            line_threshold=80.0, 
                            branch_threshold=80.0,
                            verbose=True
                        )
                
                assert result.success is True
                assert "Coverage totals: lines=90.00% branches=90.00%" in result.stdout
    
    def test_coverage_threshold_failure(self, testing_tools: testing_module.TestingTools) -> None:
        """Test failed coverage threshold check."""
        coverage_file = testing_tools._coverage_file()
        json_path = coverage_file.parent / "coverage.json"
        
        # Mock coverage data with low coverage
        coverage_data = {
            "totals": {
                "num_statements": 100,
                "covered_lines": 70,  # 70% coverage
                "num_branches": 50,
                "covered_branches": 35,  # 70% coverage
            }
        }
        
        with patch.object(coverage_file, "exists", return_value=True):
            with patch("ml_playground.tools.utils.subprocess_utils.run_uv_command") as mock_run:
                mock_result = ToolResult(
                    success=True,
                    exit_code=0,
                    stdout="",
                    stderr="",
                    operation_id=OperationId(namespace="tools", category="test", command="coverage-threshold"),
                )
                mock_run.return_value = mock_result
                
                with patch.object(json_path, "open") as mock_open:
                    mock_open.return_value.__enter__.return_value = Mock()
                    with patch("json.load", return_value=coverage_data):
                        result = testing_tools.coverage_threshold(
                            [], 
                            line_threshold=80.0, 
                            branch_threshold=80.0
                        )
                
                assert result.success is False
                assert result.exit_code == 1
                assert "Line coverage 70.00% < 80.00%" in result.stderr
                assert "Branch coverage 70.00% < 80.00%" in result.stderr


class TestClean:
    """Test cleaning test artifacts."""
    
    def test_clean_success(self, testing_tools: testing_module.TestingTools) -> None:
        """Test successful cleaning."""
        # Create some mock paths
        pytest_cache = testing_tools.root_path / ".pytest_cache"
        htmlcov = testing_tools.root_path / "htmlcov"
        
        with patch.object(pytest_cache, "exists", return_value=True):
            with patch.object(htmlcov, "exists", return_value=True):
                with patch.object(pytest_cache, "is_dir", return_value=True):
                    with patch.object(htmlcov, "is_dir", return_value=True):
                        with patch("shutil.rmtree") as mock_rmtree:
                            result = testing_tools.clean([])
        
        assert result.success is True
        assert "Cleaned 4 paths" in result.stdout  # 4 paths: pytest_cache, htmlcov, coverage, hypothesis
    
    def test_clean_no_artifacts(self, testing_tools: testing_module.TestingTools) -> None:
        """Test cleaning with no artifacts."""
        # Mock all paths as non-existent
        with patch.object(Path, "exists", return_value=False):
            result = testing_tools.clean([])
        
        assert result.success is True
        assert "No artifacts to clean" in result.stdout