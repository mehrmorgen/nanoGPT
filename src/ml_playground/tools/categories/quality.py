"""Quality tools category implementation."""

from __future__ import annotations

from pathlib import Path
from typing import List, Optional

from ml_playground.tools.core.config import ToolsConfig
from ml_playground.tools.core.interfaces import OperationId, ToolResult
from ml_playground.tools.utils.subprocess_utils import SubprocessRunner, _default_runner


class QualityTools:
    """Quality tools implementation."""
    
    def __init__(
        self, 
        config: ToolsConfig, 
        root_path: Path, 
        subprocess_runner: Optional[SubprocessRunner] = None
    ) -> None:
        """Initialize quality tools.
        
        Args:
            config: Tool configuration
            root_path: Project root path
            subprocess_runner: Subprocess runner for dependency injection
        """
        self.config = config
        self.root_path = root_path
        self.pkg_path = root_path / "src" / "ml_playground"
        self.subprocess_runner = subprocess_runner or _default_runner
    
    @property
    def category(self) -> str:
        """Tool category identifier."""
        return "quality"
    
    def lint(self, args: List[str]) -> ToolResult:
        """Run Ruff lint checks.
        
        Args:
            args: Additional ruff arguments
            
        Returns:
            ToolResult with execution details
        """
        operation_id = OperationId(namespace="tools", category=self.category, command="lint")
        
        # Default to check mode, allow args to override
        ruff_args = ["ruff", "check", "."]
        if args:
            # Replace default args if user provides custom ones
            ruff_args = ["ruff", *args]
        
        return self.subprocess_runner.run_uv_command(
            ruff_args,
            cwd=self.root_path,
            timeout=self.config.quality.timeout,
            operation_id=operation_id,
        )
    
    def format(self, args: List[str]) -> ToolResult:
        """Format code with Ruff.
        
        Args:
            args: Additional ruff arguments
            
        Returns:
            ToolResult with execution details
        """
        operation_id = OperationId(namespace="tools", category=self.category, command="format")
        
        # Run both check --fix and format
        # First, run check with --fix
        check_result = self.subprocess_runner.run_uv_command(
            ["ruff", "check", "--fix", ".", *args],
            cwd=self.root_path,
            timeout=self.config.quality.timeout,
            operation_id=operation_id,
        )
        
        if not check_result.success:
            return check_result
        
        # Then run format
        format_result = self.subprocess_runner.run_uv_command(
            ["ruff", "format", ".", *args],
            cwd=self.root_path,
            timeout=self.config.quality.timeout,
            operation_id=operation_id,
        )
        
        # Combine outputs
        combined_stdout = ""
        if check_result.stdout:
            combined_stdout += f"Ruff check --fix:\n{check_result.stdout}\n"
        if format_result.stdout:
            combined_stdout += f"Ruff format:\n{format_result.stdout}"
        
        combined_stderr = ""
        if check_result.stderr:
            combined_stderr += f"Ruff check --fix errors:\n{check_result.stderr}\n"
        if format_result.stderr:
            combined_stderr += f"Ruff format errors:\n{format_result.stderr}"
        
        return ToolResult(
            success=format_result.success,
            exit_code=format_result.exit_code,
            stdout=combined_stdout,
            stderr=combined_stderr,
            operation_id=operation_id,
        )
    
    def lint_check(self, args: List[str]) -> ToolResult:
        """Run Ruff in check-only mode (alias for lint).
        
        Args:
            args: Additional ruff arguments
            
        Returns:
            ToolResult with execution details
        """
        # This is an alias for lint command
        return self.lint(args)
    
    def deadcode(self, args: List[str]) -> ToolResult:
        """Scan for dead code using vulture.
        
        Args:
            args: Additional vulture arguments
            
        Returns:
            ToolResult with execution details
        """
        operation_id = OperationId(namespace="tools", category=self.category, command="deadcode")
        
        vulture_args = ["vulture", str(self.pkg_path), "--min-confidence", "90"]
        if args:
            vulture_args.extend(args)
        
        return self.subprocess_runner.run_uv_command(
            vulture_args,
            cwd=self.root_path,
            timeout=self.config.quality.timeout,
            operation_id=operation_id,
        )
    
    def basedpyright(self, args: List[str]) -> ToolResult:
        """Run BasedPyright type checks.
        
        Args:
            args: Additional basedpyright arguments
            
        Returns:
            ToolResult with execution details
        """
        operation_id = OperationId(namespace="tools", category=self.category, command="basedpyright")
        
        basedpyright_args = ["basedpyright", str(self.pkg_path)]
        if args:
            basedpyright_args.extend(args)
        
        return self.subprocess_runner.run_uv_command(
            basedpyright_args,
            cwd=self.root_path,
            timeout=self.config.quality.timeout,
            operation_id=operation_id,
        )
    
    def pyright(self, args: List[str]) -> ToolResult:
        """Run BasedPyright type checks (Pyright CLI alias).
        
        Args:
            args: Additional basedpyright arguments
            
        Returns:
            ToolResult with execution details
        """
        # This is an alias for basedpyright command
        return self.basedpyright(args)
    
    def mypy(self, args: List[str]) -> ToolResult:
        """Run Mypy type checks.
        
        Args:
            args: Additional mypy arguments
            
        Returns:
            ToolResult with execution details
        """
        operation_id = OperationId(namespace="tools", category=self.category, command="mypy")
        
        mypy_args = ["mypy", "--incremental", str(self.pkg_path)]
        if args:
            mypy_args.extend(args)
        
        return self.subprocess_runner.run_uv_command(
            mypy_args,
            cwd=self.root_path,
            timeout=self.config.quality.timeout,
            operation_id=operation_id,
        )
    
    def typecheck(self, args: List[str]) -> ToolResult:
        """Run both BasedPyright and Mypy type checks.
        
        Args:
            args: Additional arguments (applied to both tools)
            
        Returns:
            ToolResult with execution details
        """
        operation_id = OperationId(namespace="tools", category=self.category, command="typecheck")
        
        # Run BasedPyright first
        basedpyright_result = self.basedpyright(args)
        
        # Run Mypy regardless of BasedPyright result
        mypy_result = self.mypy(args)
        
        # Combine results
        combined_stdout = ""
        if basedpyright_result.stdout:
            combined_stdout += f"BasedPyright:\n{basedpyright_result.stdout}\n"
        if mypy_result.stdout:
            combined_stdout += f"Mypy:\n{mypy_result.stdout}"
        
        combined_stderr = ""
        if basedpyright_result.stderr:
            combined_stderr += f"BasedPyright errors:\n{basedpyright_result.stderr}\n"
        if mypy_result.stderr:
            combined_stderr += f"Mypy errors:\n{mypy_result.stderr}"
        
        # Success only if both succeed
        success = basedpyright_result.success and mypy_result.success
        exit_code = 0 if success else (basedpyright_result.exit_code or mypy_result.exit_code)
        
        return ToolResult(
            success=success,
            exit_code=exit_code,
            stdout=combined_stdout,
            stderr=combined_stderr,
            operation_id=operation_id,
        )
    
    def all_checks(self, args: List[str]) -> ToolResult:
        """Run all quality checks (lint, typecheck, deadcode).
        
        Args:
            args: Additional arguments (applied to all tools)
            
        Returns:
            ToolResult with execution details
        """
        operation_id = OperationId(namespace="tools", category=self.category, command="all")
        
        # Run all quality checks
        lint_result = self.lint(args)
        typecheck_result = self.typecheck(args)
        deadcode_result = self.deadcode(args)
        
        # Combine results
        results = [
            ("Lint", lint_result),
            ("Typecheck", typecheck_result),
            ("Deadcode", deadcode_result),
        ]
        
        combined_stdout = ""
        combined_stderr = ""
        all_success = True
        final_exit_code = 0
        
        for name, result in results:
            if result.stdout:
                combined_stdout += f"{name}:\n{result.stdout}\n"
            if result.stderr:
                combined_stderr += f"{name} errors:\n{result.stderr}\n"
            
            if not result.success:
                all_success = False
                if final_exit_code == 0:
                    final_exit_code = result.exit_code
        
        return ToolResult(
            success=all_success,
            exit_code=final_exit_code,
            stdout=combined_stdout,
            stderr=combined_stderr,
            operation_id=operation_id,
        )