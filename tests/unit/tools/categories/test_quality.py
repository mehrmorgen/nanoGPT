"""Unit tests for quality tools category."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

import pytest

from ml_playground.tools.categories import quality as quality_module
from ml_playground.tools.core import config as config_module
from ml_playground.tools.core.config import ToolsConfig
from ml_playground.tools.core.interfaces import OperationId, ToolResult


@pytest.fixture
def config() -> ToolsConfig:
    """Create test configuration."""
    return ToolsConfig(
        quality=config_module.QualityToolsConfig(
            timeout=120,
            ruff_config_path=Path("pyproject.toml"),
        )
    )


@pytest.fixture
def root_path(tmp_path: Path) -> Path:
    """Create temporary root path."""
    return tmp_path


@pytest.fixture
def quality_tools(config: ToolsConfig, root_path: Path) -> quality_module.QualityTools:
    """Create quality tools instance."""
    return quality_module.QualityTools(config, root_path)


class TestQualityToolsInit:
    """Test QualityTools initialization."""
    
    def test_init(self, quality_tools: quality_module.QualityTools, config: ToolsConfig, root_path: Path) -> None:
        """Test initialization."""
        assert quality_tools.config == config
        assert quality_tools.root_path == root_path
        assert quality_tools.pkg_path == root_path / "src" / "ml_playground"
        assert quality_tools.category == "quality"


class TestLint:
    """Test lint functionality."""
    
    def test_lint_success(self, quality_tools: quality_module.QualityTools) -> None:
        """Test successful lint execution."""
        with patch("ml_playground.tools.utils.subprocess_utils.run_uv_command") as mock_run:
            mock_result = ToolResult(
                success=True,
                exit_code=0,
                stdout="All checks passed",
                stderr="",
                operation_id=OperationId(namespace="tools", category="quality", command="lint"),
            )
            mock_run.return_value = mock_result
            
            result = quality_tools.lint([])
            
            assert result.success is True
            assert result.exit_code == 0
            assert str(result.operation_id) == "tools.quality.lint"
            
            # Check default command construction
            call_args = mock_run.call_args[0][0]
            assert call_args == ["ruff", "check", "."]
    
    def test_lint_with_custom_args(self, quality_tools: quality_module.QualityTools) -> None:
        """Test lint with custom arguments."""
        with patch("ml_playground.tools.utils.subprocess_utils.run_uv_command") as mock_run:
            mock_result = ToolResult(
                success=True,
                exit_code=0,
                stdout="",
                stderr="",
                operation_id=OperationId(namespace="tools", category="quality", command="lint"),
            )
            mock_run.return_value = mock_result
            
            result = quality_tools.lint(["check", "--fix", "src/"])
            
            assert result.success is True
            
            # Check custom command construction
            call_args = mock_run.call_args[0][0]
            assert call_args == ["ruff", "check", "--fix", "src/"]
    
    def test_lint_failure(self, quality_tools: quality_module.QualityTools) -> None:
        """Test failed lint execution."""
        with patch("ml_playground.tools.utils.subprocess_utils.run_uv_command") as mock_run:
            mock_result = ToolResult(
                success=False,
                exit_code=1,
                stdout="",
                stderr="Lint errors found",
                operation_id=OperationId(namespace="tools", category="quality", command="lint"),
            )
            mock_run.return_value = mock_result
            
            result = quality_tools.lint([])
            
            assert result.success is False
            assert result.exit_code == 1


class TestFormat:
    """Test format functionality."""
    
    def test_format_success(self, quality_tools: quality_module.QualityTools) -> None:
        """Test successful format execution."""
        with patch("ml_playground.tools.utils.subprocess_utils.run_uv_command") as mock_run:
            # Mock both check --fix and format commands
            check_result = ToolResult(
                success=True,
                exit_code=0,
                stdout="Fixed 5 issues",
                stderr="",
                operation_id=OperationId(namespace="tools", category="quality", command="format"),
            )
            format_result = ToolResult(
                success=True,
                exit_code=0,
                stdout="Formatted 10 files",
                stderr="",
                operation_id=OperationId(namespace="tools", category="quality", command="format"),
            )
            mock_run.side_effect = [check_result, format_result]
            
            result = quality_tools.format([])
            
            assert result.success is True
            assert result.exit_code == 0
            assert "Ruff check --fix:" in result.stdout
            assert "Ruff format:" in result.stdout
            assert "Fixed 5 issues" in result.stdout
            assert "Formatted 10 files" in result.stdout
    
    def test_format_check_failure(self, quality_tools: quality_module.QualityTools) -> None:
        """Test format when check --fix fails."""
        with patch("ml_playground.tools.utils.subprocess_utils.run_uv_command") as mock_run:
            check_result = ToolResult(
                success=False,
                exit_code=1,
                stdout="",
                stderr="Check failed",
                operation_id=OperationId(namespace="tools", category="quality", command="format"),
            )
            mock_run.return_value = check_result
            
            result = quality_tools.format([])
            
            assert result.success is False
            assert result.exit_code == 1
    
    def test_format_with_args(self, quality_tools: quality_module.QualityTools) -> None:
        """Test format with custom arguments."""
        with patch("ml_playground.tools.utils.subprocess_utils.run_uv_command") as mock_run:
            check_result = ToolResult(
                success=True,
                exit_code=0,
                stdout="",
                stderr="",
                operation_id=OperationId(namespace="tools", category="quality", command="format"),
            )
            format_result = ToolResult(
                success=True,
                exit_code=0,
                stdout="",
                stderr="",
                operation_id=OperationId(namespace="tools", category="quality", command="format"),
            )
            mock_run.side_effect = [check_result, format_result]
            
            result = quality_tools.format(["--verbose"])
            
            assert result.success is True
            
            # Check that args are passed to both commands
            assert mock_run.call_count == 2
            check_call = mock_run.call_args_list[0][0][0]
            format_call = mock_run.call_args_list[1][0][0]
            assert "--verbose" in check_call
            assert "--verbose" in format_call


class TestLintCheck:
    """Test lint-check functionality (alias)."""
    
    def test_lint_check_is_alias(self, quality_tools: quality_module.QualityTools) -> None:
        """Test that lint-check is an alias for lint."""
        with patch.object(quality_tools, "lint") as mock_lint:
            mock_result = ToolResult(
                success=True,
                exit_code=0,
                stdout="",
                stderr="",
                operation_id=OperationId(namespace="tools", category="quality", command="lint"),
            )
            mock_lint.return_value = mock_result
            
            result = quality_tools.lint_check(["--verbose"])
            
            mock_lint.assert_called_once_with(["--verbose"])
            assert result.success is True


class TestDeadcode:
    """Test deadcode functionality."""
    
    def test_deadcode_success(self, quality_tools: quality_module.QualityTools) -> None:
        """Test successful deadcode execution."""
        with patch("ml_playground.tools.utils.subprocess_utils.run_uv_command") as mock_run:
            mock_result = ToolResult(
                success=True,
                exit_code=0,
                stdout="No dead code found",
                stderr="",
                operation_id=OperationId(namespace="tools", category="quality", command="deadcode"),
            )
            mock_run.return_value = mock_result
            
            result = quality_tools.deadcode([])
            
            assert result.success is True
            assert str(result.operation_id) == "tools.quality.deadcode"
            
            # Check command construction
            call_args = mock_run.call_args[0][0]
            assert "vulture" in call_args
            assert str(quality_tools.pkg_path) in call_args
            assert "--min-confidence" in call_args
            assert "90" in call_args


class TestBasedPyright:
    """Test BasedPyright functionality."""
    
    def test_basedpyright_success(self, quality_tools: quality_module.QualityTools) -> None:
        """Test successful BasedPyright execution."""
        with patch("ml_playground.tools.utils.subprocess_utils.run_uv_command") as mock_run:
            mock_result = ToolResult(
                success=True,
                exit_code=0,
                stdout="No type errors found",
                stderr="",
                operation_id=OperationId(namespace="tools", category="quality", command="basedpyright"),
            )
            mock_run.return_value = mock_result
            
            result = quality_tools.basedpyright([])
            
            assert result.success is True
            assert str(result.operation_id) == "tools.quality.basedpyright"
            
            # Check command construction
            call_args = mock_run.call_args[0][0]
            assert "basedpyright" in call_args
            assert str(quality_tools.pkg_path) in call_args


class TestPyright:
    """Test Pyright functionality (alias)."""
    
    def test_pyright_is_alias(self, quality_tools: quality_module.QualityTools) -> None:
        """Test that pyright is an alias for basedpyright."""
        with patch.object(quality_tools, "basedpyright") as mock_basedpyright:
            mock_result = ToolResult(
                success=True,
                exit_code=0,
                stdout="",
                stderr="",
                operation_id=OperationId(namespace="tools", category="quality", command="basedpyright"),
            )
            mock_basedpyright.return_value = mock_result
            
            result = quality_tools.pyright(["--verbose"])
            
            mock_basedpyright.assert_called_once_with(["--verbose"])
            assert result.success is True


class TestMypy:
    """Test Mypy functionality."""
    
    def test_mypy_success(self, quality_tools: quality_module.QualityTools) -> None:
        """Test successful Mypy execution."""
        with patch("ml_playground.tools.utils.subprocess_utils.run_uv_command") as mock_run:
            mock_result = ToolResult(
                success=True,
                exit_code=0,
                stdout="Success: no issues found",
                stderr="",
                operation_id=OperationId(namespace="tools", category="quality", command="mypy"),
            )
            mock_run.return_value = mock_result
            
            result = quality_tools.mypy([])
            
            assert result.success is True
            assert str(result.operation_id) == "tools.quality.mypy"
            
            # Check command construction
            call_args = mock_run.call_args[0][0]
            assert "mypy" in call_args
            assert "--incremental" in call_args
            assert str(quality_tools.pkg_path) in call_args


class TestTypecheck:
    """Test typecheck functionality."""
    
    def test_typecheck_success(self, quality_tools: quality_module.QualityTools) -> None:
        """Test successful typecheck execution."""
        with patch.object(quality_tools, "basedpyright") as mock_basedpyright:
            with patch.object(quality_tools, "mypy") as mock_mypy:
                basedpyright_result = ToolResult(
                    success=True,
                    exit_code=0,
                    stdout="BasedPyright: No errors",
                    stderr="",
                    operation_id=OperationId(namespace="tools", category="quality", command="basedpyright"),
                )
                mypy_result = ToolResult(
                    success=True,
                    exit_code=0,
                    stdout="Mypy: Success",
                    stderr="",
                    operation_id=OperationId(namespace="tools", category="quality", command="mypy"),
                )
                mock_basedpyright.return_value = basedpyright_result
                mock_mypy.return_value = mypy_result
                
                result = quality_tools.typecheck([])
                
                assert result.success is True
                assert result.exit_code == 0
                assert str(result.operation_id) == "tools.quality.typecheck"
                assert "BasedPyright:" in result.stdout
                assert "Mypy:" in result.stdout
    
    def test_typecheck_basedpyright_failure(self, quality_tools: quality_module.QualityTools) -> None:
        """Test typecheck when BasedPyright fails."""
        with patch.object(quality_tools, "basedpyright") as mock_basedpyright:
            with patch.object(quality_tools, "mypy") as mock_mypy:
                basedpyright_result = ToolResult(
                    success=False,
                    exit_code=1,
                    stdout="",
                    stderr="BasedPyright errors",
                    operation_id=OperationId(namespace="tools", category="quality", command="basedpyright"),
                )
                mypy_result = ToolResult(
                    success=True,
                    exit_code=0,
                    stdout="Mypy: Success",
                    stderr="",
                    operation_id=OperationId(namespace="tools", category="quality", command="mypy"),
                )
                mock_basedpyright.return_value = basedpyright_result
                mock_mypy.return_value = mypy_result
                
                result = quality_tools.typecheck([])
                
                assert result.success is False
                assert result.exit_code == 1
    
    def test_typecheck_mypy_failure(self, quality_tools: quality_module.QualityTools) -> None:
        """Test typecheck when Mypy fails."""
        with patch.object(quality_tools, "basedpyright") as mock_basedpyright:
            with patch.object(quality_tools, "mypy") as mock_mypy:
                basedpyright_result = ToolResult(
                    success=True,
                    exit_code=0,
                    stdout="BasedPyright: No errors",
                    stderr="",
                    operation_id=OperationId(namespace="tools", category="quality", command="basedpyright"),
                )
                mypy_result = ToolResult(
                    success=False,
                    exit_code=2,
                    stdout="",
                    stderr="Mypy errors",
                    operation_id=OperationId(namespace="tools", category="quality", command="mypy"),
                )
                mock_basedpyright.return_value = basedpyright_result
                mock_mypy.return_value = mypy_result
                
                result = quality_tools.typecheck([])
                
                assert result.success is False
                assert result.exit_code == 2


class TestAllChecks:
    """Test all quality checks functionality."""
    
    def test_all_checks_success(self, quality_tools: quality_module.QualityTools) -> None:
        """Test successful execution of all quality checks."""
        with patch.object(quality_tools, "lint") as mock_lint:
            with patch.object(quality_tools, "typecheck") as mock_typecheck:
                with patch.object(quality_tools, "deadcode") as mock_deadcode:
                    lint_result = ToolResult(
                        success=True,
                        exit_code=0,
                        stdout="Lint: OK",
                        stderr="",
                        operation_id=OperationId(namespace="tools", category="quality", command="lint"),
                    )
                    typecheck_result = ToolResult(
                        success=True,
                        exit_code=0,
                        stdout="Typecheck: OK",
                        stderr="",
                        operation_id=OperationId(namespace="tools", category="quality", command="typecheck"),
                    )
                    deadcode_result = ToolResult(
                        success=True,
                        exit_code=0,
                        stdout="Deadcode: OK",
                        stderr="",
                        operation_id=OperationId(namespace="tools", category="quality", command="deadcode"),
                    )
                    mock_lint.return_value = lint_result
                    mock_typecheck.return_value = typecheck_result
                    mock_deadcode.return_value = deadcode_result
                    
                    result = quality_tools.all_checks([])
                    
                    assert result.success is True
                    assert result.exit_code == 0
                    assert str(result.operation_id) == "tools.quality.all"
                    assert "Lint:" in result.stdout
                    assert "Typecheck:" in result.stdout
                    assert "Deadcode:" in result.stdout
    
    def test_all_checks_partial_failure(self, quality_tools: quality_module.QualityTools) -> None:
        """Test all checks when some fail."""
        with patch.object(quality_tools, "lint") as mock_lint:
            with patch.object(quality_tools, "typecheck") as mock_typecheck:
                with patch.object(quality_tools, "deadcode") as mock_deadcode:
                    lint_result = ToolResult(
                        success=False,
                        exit_code=1,
                        stdout="",
                        stderr="Lint errors",
                        operation_id=OperationId(namespace="tools", category="quality", command="lint"),
                    )
                    typecheck_result = ToolResult(
                        success=True,
                        exit_code=0,
                        stdout="Typecheck: OK",
                        stderr="",
                        operation_id=OperationId(namespace="tools", category="quality", command="typecheck"),
                    )
                    deadcode_result = ToolResult(
                        success=True,
                        exit_code=0,
                        stdout="Deadcode: OK",
                        stderr="",
                        operation_id=OperationId(namespace="tools", category="quality", command="deadcode"),
                    )
                    mock_lint.return_value = lint_result
                    mock_typecheck.return_value = typecheck_result
                    mock_deadcode.return_value = deadcode_result
                    
                    result = quality_tools.all_checks([])
                    
                    assert result.success is False
                    assert result.exit_code == 1
                    assert "Lint errors:" in result.stderr