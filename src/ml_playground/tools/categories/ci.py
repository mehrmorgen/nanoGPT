"""CI tools category implementation."""

from __future__ import annotations

import json
import os
import subprocess
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from ml_playground.tools.core.config import ToolsConfig
from ml_playground.tools.core.errors import ToolExecutionError
from ml_playground.tools.core.interfaces import OperationId, ToolResult
from ml_playground.tools.utils.subprocess_utils import run_uv_command


class CITools:
    """CI/CD tools implementation."""
    
    def __init__(self, config: ToolsConfig, root_path: Path) -> None:
        """Initialize CI tools.
        
        Args:
            config: Tool configuration
            root_path: Project root path
        """
        self.config = config
        self.root_path = root_path
        self.cache_dir = root_path / ".cache"
        self.pre_commit_config = root_path / ".githooks" / ".pre-commit-config.yaml"
    
    @property
    def category(self) -> str:
        """Tool category identifier."""
        return "ci"
    
    def _coverage_file(self) -> Path:
        """Get the coverage data file path."""
        return self.cache_dir / "coverage" / "coverage.sqlite"
    
    def _cosmic_ray_session_file(self) -> Path:
        """Get the Cosmic Ray session file path."""
        return self.cache_dir / "cosmic_ray" / "session.sqlite"
    
    def _ensure_cache_dirs(self, *subdirs: str) -> None:
        """Ensure cache directories exist."""
        for subdir in subdirs:
            (self.cache_dir / subdir).mkdir(parents=True, exist_ok=True)
    
    def quality_gate(self, args: List[str]) -> ToolResult:
        """Run the full pre-commit quality gate.
        
        Args:
            args: Additional pre-commit arguments
            
        Returns:
            ToolResult with execution details
        """
        operation_id = OperationId(namespace="tools", category=self.category, command="quality-gate")
        
        # Run pre-commit on all files
        precommit_result = run_uv_command(
            [
                "pre-commit",
                "run",
                "--config", str(self.pre_commit_config),
                "--all-files",
                *args
            ],
            cwd=self.root_path,
            timeout=self.config.ci.timeout,
            operation_id=operation_id,
        )
        
        if not precommit_result.success:
            return precommit_result
        
        # Run integration tests
        integration_result = run_uv_command(
            ["python", "-m", "pytest", "-m", "integration", "--no-cov"],
            cwd=self.root_path,
            timeout=self.config.ci.timeout,
            operation_id=operation_id,
        )
        
        if not integration_result.success:
            return integration_result
        
        # Run acceptance tests
        acceptance_result = run_uv_command(
            ["python", "-m", "pytest", "tests/acceptance"],
            cwd=self.root_path,
            timeout=self.config.ci.timeout,
            operation_id=operation_id,
        )
        
        if not acceptance_result.success:
            return acceptance_result
        
        # Run e2e tests
        e2e_result = run_uv_command(
            ["python", "-m", "pytest", "tests/e2e"],
            cwd=self.root_path,
            timeout=self.config.ci.timeout,
            operation_id=operation_id,
        )
        
        # Combine outputs
        combined_stdout = ""
        if precommit_result.stdout:
            combined_stdout += f"Pre-commit:\n{precommit_result.stdout}\n"
        if integration_result.stdout:
            combined_stdout += f"Integration tests:\n{integration_result.stdout}\n"
        if acceptance_result.stdout:
            combined_stdout += f"Acceptance tests:\n{acceptance_result.stdout}\n"
        if e2e_result.stdout:
            combined_stdout += f"E2E tests:\n{e2e_result.stdout}"
        
        combined_stderr = ""
        if precommit_result.stderr:
            combined_stderr += f"Pre-commit errors:\n{precommit_result.stderr}\n"
        if integration_result.stderr:
            combined_stderr += f"Integration test errors:\n{integration_result.stderr}\n"
        if acceptance_result.stderr:
            combined_stderr += f"Acceptance test errors:\n{acceptance_result.stderr}\n"
        if e2e_result.stderr:
            combined_stderr += f"E2E test errors:\n{e2e_result.stderr}"
        
        return ToolResult(
            success=e2e_result.success,
            exit_code=e2e_result.exit_code,
            stdout=combined_stdout,
            stderr=combined_stderr,
            operation_id=operation_id,
        )
    
    def quality_fast(self, args: List[str]) -> ToolResult:
        """Run lint/format focused pre-commit hooks.
        
        Args:
            args: Additional pre-commit arguments
            
        Returns:
            ToolResult with execution details
        """
        operation_id = OperationId(namespace="tools", category=self.category, command="quality-fast")
        
        # Run specific pre-commit hooks for fast feedback
        hooks = ["ruff", "ruff-format", "mdformat"]
        results = []
        
        for hook in hooks:
            result = run_uv_command(
                [
                    "pre-commit",
                    "run",
                    "--config", str(self.pre_commit_config),
                    "--all-files",
                    hook,
                    *args
                ],
                cwd=self.root_path,
                timeout=self.config.ci.timeout,
                operation_id=operation_id,
            )
            
            if not result.success:
                return result  # Return first failure
            
            results.append((hook, result))
        
        # Combine successful results
        combined_stdout = ""
        combined_stderr = ""
        
        for hook, result in results:
            if result.stdout:
                combined_stdout += f"{hook}:\n{result.stdout}\n"
            if result.stderr:
                combined_stderr += f"{hook} warnings:\n{result.stderr}\n"
        
        return ToolResult(
            success=True,
            exit_code=0,
            stdout=combined_stdout,
            stderr=combined_stderr,
            operation_id=operation_id,
        )
    
    def quality_ext(self, args: List[str]) -> ToolResult:
        """Run quality gates followed by mutation testing.
        
        Args:
            args: Additional arguments (ignored)
            
        Returns:
            ToolResult with execution details
        """
        operation_id = OperationId(namespace="tools", category=self.category, command="quality-ext")
        
        # Run full quality gate first
        quality_result = self.quality_gate([])
        if not quality_result.success:
            return quality_result
        
        # Run mutation testing
        mutation_result = self.mutation_run([])
        
        # Combine outputs
        combined_stdout = ""
        if quality_result.stdout:
            combined_stdout += f"Quality gate:\n{quality_result.stdout}\n"
        if mutation_result.stdout:
            combined_stdout += f"Mutation testing:\n{mutation_result.stdout}"
        
        combined_stderr = ""
        if quality_result.stderr:
            combined_stderr += f"Quality gate warnings:\n{quality_result.stderr}\n"
        if mutation_result.stderr:
            combined_stderr += f"Mutation testing errors:\n{mutation_result.stderr}"
        
        return ToolResult(
            success=mutation_result.success,
            exit_code=mutation_result.exit_code,
            stdout=combined_stdout,
            stderr=combined_stderr,
            operation_id=operation_id,
        )
    
    def quality_ci_local(self, args: List[str], bind_caches: bool = True) -> ToolResult:
        """Run the GitHub quality workflow locally using act.
        
        Args:
            args: Additional act arguments
            bind_caches: Whether to bind local caches into the container
            
        Returns:
            ToolResult with execution details
        """
        operation_id = OperationId(namespace="tools", category=self.category, command="quality-ci-local")
        
        # Ensure cache directories exist
        self._ensure_cache_dirs("uv", "pre-commit", "ruff")
        (self.root_path / ".venv").mkdir(parents=True, exist_ok=True)
        
        # Build act command
        command = [
            "act",
            "--container-architecture", "linux/amd64",
            "-P", "ubuntu-latest=catthehacker/ubuntu:act-latest",
            "-W", ".github/workflows/quality.yml",
            "--job", "quality",
        ]
        
        # Add cache binds if requested
        if bind_caches:
            binds: List[Tuple[Path, str]] = [
                (self.cache_dir / "uv", "/root/.cache/uv"),
                (self.cache_dir / "pre-commit", "/root/.cache/pre-commit"),
                (self.cache_dir / "ruff", "/root/.cache/ruff"),
                (self.root_path / ".venv", "/root/project/.venv"),
            ]
            
            for host_path, container_path in binds:
                host_path.mkdir(parents=True, exist_ok=True)
                command.extend(["--bind", f"{host_path}:{container_path}"])
        
        # Add additional arguments
        command.extend(args)
        
        # Run act directly with subprocess since it's not a uv command
        try:
            result = subprocess.run(
                command,
                cwd=self.root_path,
                capture_output=True,
                text=True,
                timeout=self.config.ci.timeout,
            )
            
            return ToolResult(
                success=result.returncode == 0,
                exit_code=result.returncode,
                stdout=result.stdout,
                stderr=result.stderr,
                operation_id=operation_id,
            )
        except subprocess.TimeoutExpired as exc:
            raise ToolExecutionError(
                f"Act command timed out after {self.config.ci.timeout} seconds",
                reason="Command execution exceeded configured timeout",
                rationale="CI operations must complete within reasonable time bounds"
            ) from exc
        except Exception as exc:
            raise ToolExecutionError(
                f"Failed to execute act command: {exc}",
                reason="Subprocess execution failed",
                rationale="Act must be available and executable for local CI testing"
            ) from exc
    
    def coverage_badge(self, args: List[str]) -> ToolResult:
        """Regenerate the SVG coverage badges.
        
        Args:
            args: Additional arguments (ignored)
            
        Returns:
            ToolResult with execution details
        """
        operation_id = OperationId(namespace="tools", category=self.category, command="coverage-badge")
        
        # Ensure coverage JSON exists
        json_path = self.cache_dir / "coverage" / "coverage.json"
        if not json_path.exists():
            # Try to generate coverage report first
            coverage_result = run_uv_command(
                ["coverage", "json", "-o", str(json_path)],
                cwd=self.root_path,
                env={"COVERAGE_FILE": str(self._coverage_file())},
                timeout=self.config.ci.timeout,
                operation_id=operation_id,
            )
            
            if not coverage_result.success:
                raise ToolExecutionError(
                    "Failed to generate coverage JSON for badge creation",
                    reason="Coverage report generation failed",
                    rationale="Badge generation requires valid coverage data"
                )
        
        # Generate badges
        return run_uv_command(
            ["python", "tools/coverage_badges.py", str(json_path), "docs/assets"],
            cwd=self.root_path,
            timeout=self.config.ci.timeout,
            operation_id=operation_id,
        )
    
    def mutation_reset(self, args: List[str]) -> ToolResult:
        """Remove the cached Cosmic Ray session.
        
        Args:
            args: Additional arguments (ignored)
            
        Returns:
            ToolResult with execution details
        """
        operation_id = OperationId(namespace="tools", category=self.category, command="mutation-reset")
        
        session_file = self._cosmic_ray_session_file()
        if session_file.exists():
            try:
                session_file.unlink()
                output = f"Removed Cosmic Ray session: {session_file}"
            except Exception as exc:
                raise ToolExecutionError(
                    f"Failed to remove Cosmic Ray session file: {session_file}",
                    reason=f"File deletion failed: {exc}",
                    rationale="Session file must be removable for clean mutation testing"
                ) from exc
        else:
            output = f"Cosmic Ray session file does not exist: {session_file}"
        
        return ToolResult(
            success=True,
            exit_code=0,
            stdout=output,
            stderr="",
            operation_id=operation_id,
        )
    
    def mutation_summary(self, args: List[str]) -> ToolResult:
        """Show a summary of the previous Cosmic Ray run.
        
        Args:
            args: Additional arguments (ignored)
            
        Returns:
            ToolResult with execution details
        """
        operation_id = OperationId(namespace="tools", category=self.category, command="mutation-summary")
        
        return run_uv_command(
            ["python", "tools/mutation_summary.py", "--config", "pyproject.toml"],
            cwd=self.root_path,
            timeout=self.config.ci.timeout,
            operation_id=operation_id,
        )
    
    def mutation_init(self, args: List[str]) -> ToolResult:
        """Initialize the Cosmic Ray session database if needed.
        
        Args:
            args: Additional arguments (ignored)
            
        Returns:
            ToolResult with execution details
        """
        operation_id = OperationId(namespace="tools", category=self.category, command="mutation-init")
        
        session_file = self._cosmic_ray_session_file()
        session_file.parent.mkdir(parents=True, exist_ok=True)
        
        # Run cosmic-ray init, allowing non-zero exit (reusing existing session)
        result = run_uv_command(
            ["cosmic-ray", "init", "pyproject.toml", str(session_file)],
            cwd=self.root_path,
            timeout=self.config.ci.timeout,
            operation_id=operation_id,
        )
        
        # Adjust output based on result
        if result.success:
            output = "Cosmic Ray session initialized"
        else:
            output = "Cosmic Ray init skipped (reusing existing session)"
            # Convert to success since this is expected behavior
            result = ToolResult(
                success=True,
                exit_code=0,
                stdout=output,
                stderr=result.stderr,
                operation_id=operation_id,
            )
        
        return result
    
    def mutation_exec(self, args: List[str]) -> ToolResult:
        """Execute mutation tests with Cosmic Ray.
        
        Args:
            args: Additional arguments (ignored)
            
        Returns:
            ToolResult with execution details
        """
        operation_id = OperationId(namespace="tools", category=self.category, command="mutation-exec")
        
        session_file = self._cosmic_ray_session_file()
        if not session_file.exists():
            raise ToolExecutionError(
                "Cosmic Ray session file not found",
                reason=f"Missing session file: {session_file}",
                rationale="Mutation execution requires initialized session database"
            )
        
        return run_uv_command(
            ["cosmic-ray", "exec", "pyproject.toml", str(session_file)],
            cwd=self.root_path,
            timeout=self.config.ci.timeout,
            operation_id=operation_id,
        )
    
    def mutation_report(self, args: List[str]) -> ToolResult:
        """Render a mutation testing report.
        
        Args:
            args: Additional arguments (ignored)
            
        Returns:
            ToolResult with execution details
        """
        operation_id = OperationId(namespace="tools", category=self.category, command="mutation-report")
        
        return run_uv_command(
            ["python", "tools/mutation_report.py", "--config", "pyproject.toml"],
            cwd=self.root_path,
            timeout=self.config.ci.timeout,
            operation_id=operation_id,
        )
    
    def mutation_run(self, args: List[str]) -> ToolResult:
        """Run the full mutation testing pipeline.
        
        Args:
            args: Additional arguments (ignored)
            
        Returns:
            ToolResult with execution details
        """
        operation_id = OperationId(namespace="tools", category=self.category, command="mutation-run")
        
        # Run mutation testing pipeline
        steps = [
            ("reset", self.mutation_reset),
            ("summary", self.mutation_summary),
            ("init", self.mutation_init),
            ("exec", self.mutation_exec),
            ("report", self.mutation_report),
        ]
        
        results = []
        combined_stdout = ""
        combined_stderr = ""
        
        for step_name, step_func in steps:
            try:
                result = step_func([])
                results.append((step_name, result))
                
                if result.stdout:
                    combined_stdout += f"Mutation {step_name}:\n{result.stdout}\n"
                if result.stderr:
                    combined_stderr += f"Mutation {step_name} warnings:\n{result.stderr}\n"
                
                if not result.success:
                    # Return failure at first failed step
                    return ToolResult(
                        success=False,
                        exit_code=result.exit_code,
                        stdout=combined_stdout,
                        stderr=combined_stderr,
                        operation_id=operation_id,
                    )
            except Exception as exc:
                raise ToolExecutionError(
                    f"Mutation testing step '{step_name}' failed",
                    reason=str(exc),
                    rationale="All mutation testing steps must complete successfully"
                ) from exc
        
        return ToolResult(
            success=True,
            exit_code=0,
            stdout=combined_stdout,
            stderr=combined_stderr,
            operation_id=operation_id,
        )