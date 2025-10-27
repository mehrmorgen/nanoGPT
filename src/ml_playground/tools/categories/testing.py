"""Testing tools category implementation."""

from __future__ import annotations

import json
import os
import shutil
from pathlib import Path
from typing import Dict, List, Optional

from ml_playground.tools.core.config import ToolsConfig
from ml_playground.tools.core.errors import ToolExecutionError
from ml_playground.tools.core.interfaces import OperationId, ToolResult
from ml_playground.tools.utils.subprocess_utils import run_pytest_command, run_uv_command


class TestingTools:
    """Testing tools implementation."""
    
    def __init__(self, config: ToolsConfig, root_path: Path) -> None:
        """Initialize testing tools.
        
        Args:
            config: Tool configuration
            root_path: Project root path
        """
        self.config = config
        self.root_path = root_path
        self.cache_dir = root_path / ".cache"
    
    @property
    def category(self) -> str:
        """Tool category identifier."""
        return "test"
    
    def _ensure_cache_dirs(self, *subdirs: str) -> None:
        """Ensure cache directories exist."""
        for subdir in subdirs:
            (self.cache_dir / subdir).mkdir(parents=True, exist_ok=True)
    
    def _coverage_file(self) -> Path:
        """Get the coverage data file path."""
        return self.cache_dir / "coverage" / "coverage.sqlite"
    
    def _coverage_env(self, coverage_file: Optional[Path] = None) -> Dict[str, str]:
        """Get environment variables for coverage execution."""
        if coverage_file is None:
            coverage_file = self._coverage_file()
        
        self._ensure_cache_dirs("coverage", "hypothesis")
        coverage_file.parent.mkdir(parents=True, exist_ok=True)
        
        return {
            "HYPOTHESIS_DATABASE_DIRECTORY": str(self.cache_dir / "hypothesis"),
            "HYPOTHESIS_STORAGE_DIRECTORY": str(self.cache_dir / "hypothesis"),
            "HYPOTHESIS_SEED": "0",
            "PYTHONHASHSEED": "0",
            "COVERAGE_FILE": str(coverage_file),
        }
    
    def unit(self, args: List[str]) -> ToolResult:
        """Run unit tests.
        
        Args:
            args: Additional pytest arguments
            
        Returns:
            ToolResult with execution details
        """
        operation_id = OperationId(namespace="tools", category=self.category, command="unit")
        
        return run_pytest_command(
            ["tests/unit", *args],
            cwd=self.root_path,
            timeout=self.config.testing.timeout,
            operation_id=operation_id,
        )
    
    def integration(self, args: List[str]) -> ToolResult:
        """Run integration tests.
        
        Args:
            args: Additional pytest arguments
            
        Returns:
            ToolResult with execution details
        """
        operation_id = OperationId(namespace="tools", category=self.category, command="integration")
        
        return run_pytest_command(
            ["-m", "integration", "--no-cov", *args],
            cwd=self.root_path,
            timeout=self.config.testing.timeout,
            operation_id=operation_id,
        )
    
    def e2e(self, args: List[str]) -> ToolResult:
        """Run end-to-end tests.
        
        Args:
            args: Additional pytest arguments
            
        Returns:
            ToolResult with execution details
        """
        operation_id = OperationId(namespace="tools", category=self.category, command="e2e")
        
        return run_pytest_command(
            ["tests/e2e", *args],
            cwd=self.root_path,
            timeout=self.config.testing.timeout,
            operation_id=operation_id,
        )
    
    def acceptance(self, args: List[str]) -> ToolResult:
        """Run acceptance tests.
        
        Args:
            args: Additional pytest arguments
            
        Returns:
            ToolResult with execution details
        """
        operation_id = OperationId(namespace="tools", category=self.category, command="acceptance")
        
        return run_pytest_command(
            ["tests/acceptance", *args],
            cwd=self.root_path,
            timeout=self.config.testing.timeout,
            operation_id=operation_id,
        )
    
    def property_tests(self, args: List[str]) -> ToolResult:
        """Run property-based tests.
        
        Args:
            args: Additional pytest arguments
            
        Returns:
            ToolResult with execution details
        """
        operation_id = OperationId(namespace="tools", category=self.category, command="property")
        
        return run_pytest_command(
            ["tests/property", *args],
            cwd=self.root_path,
            timeout=self.config.testing.timeout,
            operation_id=operation_id,
        )
    
    def all_tests(self, args: List[str]) -> ToolResult:
        """Run all tests.
        
        Args:
            args: Additional pytest arguments
            
        Returns:
            ToolResult with execution details
        """
        operation_id = OperationId(namespace="tools", category=self.category, command="all")
        
        return run_pytest_command(
            ["tests", *args],
            cwd=self.root_path,
            timeout=self.config.testing.timeout,
            operation_id=operation_id,
        )
    
    def coverage_test(self, args: List[str]) -> ToolResult:
        """Run tests with coverage collection.
        
        Args:
            args: Additional arguments (ignored for coverage test)
            
        Returns:
            ToolResult with execution details
        """
        operation_id = OperationId(namespace="tools", category=self.category, command="coverage-test")
        
        # Clean up existing coverage data
        coverage_file = self._coverage_file()
        if coverage_file.exists():
            coverage_file.unlink()
        
        # Remove any coverage fragments
        for fragment in coverage_file.parent.glob("coverage.sqlite.*"):
            if fragment.name != coverage_file.name:
                fragment.unlink()
        
        # Set up coverage environment
        env = self._coverage_env(coverage_file)
        
        # Run coverage with pytest
        return run_uv_command(
            [
                "coverage",
                "run",
                f"--data-file={coverage_file}",
                "-m",
                "pytest",
                "-n",
                "0",  # No parallel execution for coverage
                "tests/unit",
                "tests/property",
            ],
            cwd=self.root_path,
            env=env,
            timeout=self.config.testing.timeout,
            operation_id=operation_id,
        )
    
    def coverage_report(
        self, 
        args: List[str], 
        fail_under: float = 0.0, 
        verbose: bool = False
    ) -> ToolResult:
        """Generate coverage reports.
        
        Args:
            args: Additional arguments (ignored)
            fail_under: Minimum coverage threshold
            verbose: Whether to show verbose output
            
        Returns:
            ToolResult with execution details
        """
        operation_id = OperationId(namespace="tools", category=self.category, command="coverage-report")
        
        coverage_file = self._coverage_file()
        if not coverage_file.exists():
            raise ToolExecutionError(
                "Coverage data file not found",
                reason=f"Missing coverage file: {coverage_file}",
                rationale="Coverage reports require prior execution of coverage-test command"
            )
        
        # Check for empty coverage file in CI
        ci_strict = os.environ.get("CI", "").lower() == "true"
        if ci_strict and coverage_file.stat().st_size == 0:
            raise ToolExecutionError(
                "Coverage data file is empty",
                reason="Coverage file exists but contains no data",
                rationale="Empty coverage files indicate test execution problems in CI"
            )
        
        env = {"COVERAGE_FILE": str(coverage_file)}
        coverage_dir = coverage_file.parent
        
        # Generate multiple report formats
        commands = [
            (["coverage", "report", "-m", "--fail-under", f"{fail_under:.2f}"], "terminal report"),
            (["coverage", "html", "-d", str(coverage_dir / "htmlcov")], "HTML report"),
            (["coverage", "json", "-o", str(coverage_dir / "coverage.json")], "JSON report"),
            (["coverage", "xml", "-o", str(coverage_dir / "coverage.xml")], "XML report"),
        ]
        
        results = []
        for command, description in commands:
            try:
                result = run_uv_command(
                    command,
                    cwd=self.root_path,
                    env=env,
                    timeout=self.config.testing.timeout,
                    operation_id=operation_id,
                )
                if not result.success:
                    return result  # Return first failure
                results.append(f"Generated {description}")
            except Exception as exc:
                raise ToolExecutionError(
                    f"Failed to generate {description}",
                    reason=str(exc),
                    rationale="Coverage report generation must succeed for quality gates"
                ) from exc
        
        # Show artifacts if verbose
        output = "\n".join(results)
        if verbose:
            artifacts = []
            for path in sorted(coverage_dir.iterdir()):
                artifacts.append(f"  - {path.relative_to(self.root_path)}")
            if artifacts:
                output += "\n\nCoverage artifacts:\n" + "\n".join(artifacts)
        
        return ToolResult(
            success=True,
            exit_code=0,
            stdout=output,
            stderr="",
            operation_id=operation_id,
        )
    
    def coverage_threshold(
        self, 
        args: List[str], 
        line_threshold: float = 0.0, 
        branch_threshold: float = 0.0,
        verbose: bool = False
    ) -> ToolResult:
        """Check coverage thresholds.
        
        Args:
            args: Additional arguments (ignored)
            line_threshold: Minimum line coverage percentage
            branch_threshold: Minimum branch coverage percentage
            verbose: Whether to show verbose output
            
        Returns:
            ToolResult with execution details
        """
        operation_id = OperationId(namespace="tools", category=self.category, command="coverage-threshold")
        
        coverage_file = self._coverage_file()
        if not coverage_file.exists():
            raise ToolExecutionError(
                "Coverage data file not found",
                reason=f"Missing coverage file: {coverage_file}",
                rationale="Coverage threshold checks require prior execution of coverage-test command"
            )
        
        # Generate JSON report for analysis
        env = {"COVERAGE_FILE": str(coverage_file)}
        json_path = coverage_file.parent / "coverage.json"
        
        result = run_uv_command(
            ["coverage", "json", "-o", str(json_path)],
            cwd=self.root_path,
            env=env,
            timeout=self.config.testing.timeout,
            operation_id=operation_id,
        )
        
        if not result.success:
            return result
        
        # Parse coverage data
        try:
            with json_path.open(encoding="utf-8") as f:
                coverage_data = json.load(f)
            totals = coverage_data["totals"]
        except (json.JSONDecodeError, KeyError) as exc:
            raise ToolExecutionError(
                "Failed to parse coverage data",
                reason=f"Invalid coverage JSON: {exc}",
                rationale="Coverage data must be parseable for threshold validation"
            ) from exc
        
        # Extract metrics
        num_branches = totals.get("num_branches", 0)
        covered_branches = totals.get("covered_branches", 0)
        covered_lines = totals.get("covered_lines", 0)
        num_statements = totals.get("num_statements", 0)
        
        # Calculate percentages
        line_pct = (covered_lines / num_statements) * 100 if num_statements else 0.0
        branch_pct = (covered_branches / num_branches) * 100 if num_branches else 0.0
        
        # Check thresholds
        messages = []
        
        if line_threshold > 0:
            if num_statements == 0:
                messages.append("Line coverage totals missing from coverage data")
            elif line_pct < line_threshold:
                messages.append(
                    f"Line coverage {line_pct:.2f}% < {line_threshold:.2f}%. "
                    "Run 'uv run tools test coverage-test' to collect coverage data."
                )
        
        if branch_threshold > 0:
            if num_branches == 0:
                messages.append("Branch coverage data missing from coverage data")
            elif branch_pct < branch_threshold:
                messages.append(
                    f"Branch coverage {branch_pct:.2f}% < {branch_threshold:.2f}%."
                )
        
        # Prepare output
        output = ""
        if verbose:
            output = f"Coverage totals: lines={line_pct:.2f}% branches={branch_pct:.2f}%"
        
        if messages:
            error_output = "\n".join(f"[coverage] {msg}" for msg in messages)
            return ToolResult(
                success=False,
                exit_code=1,
                stdout=output,
                stderr=error_output,
                operation_id=operation_id,
            )
        
        return ToolResult(
            success=True,
            exit_code=0,
            stdout=output,
            stderr="",
            operation_id=operation_id,
        )
    
    def clean(self, args: List[str]) -> ToolResult:
        """Clean test artifacts and caches.
        
        Args:
            args: Additional arguments (ignored)
            
        Returns:
            ToolResult with execution details
        """
        operation_id = OperationId(namespace="tools", category=self.category, command="clean")
        
        # Paths to clean
        paths_to_clean = [
            self.root_path / ".pytest_cache",
            self.root_path / "htmlcov",
            self.cache_dir / "coverage",
            self.cache_dir / "hypothesis",
        ]
        
        cleaned = []
        for path in paths_to_clean:
            if path.exists():
                if path.is_dir():
                    shutil.rmtree(path, ignore_errors=True)
                else:
                    path.unlink(missing_ok=True)
                cleaned.append(str(path.relative_to(self.root_path)))
        
        output = f"Cleaned {len(cleaned)} paths" if cleaned else "No artifacts to clean"
        if cleaned:
            output += ":\n" + "\n".join(f"  - {path}" for path in cleaned)
        
        return ToolResult(
            success=True,
            exit_code=0,
            stdout=output,
            stderr="",
            operation_id=operation_id,
        )