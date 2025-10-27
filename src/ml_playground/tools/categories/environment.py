"""Environment tools category implementation."""

from __future__ import annotations

import shutil
from pathlib import Path
from typing import Dict, List, Optional

from ml_playground.tools.core.config import ToolsConfig
from ml_playground.tools.core.errors import EnvironmentSetupError, ToolExecutionError
from ml_playground.tools.core.interfaces import OperationId, ToolResult
from ml_playground.tools.utils.subprocess_utils import SubprocessRunner, _default_runner


class EnvironmentTools:
    """Environment management tools implementation."""
    
    def __init__(
        self, 
        config: ToolsConfig, 
        root_path: Path, 
        subprocess_runner: Optional[SubprocessRunner] = None
    ) -> None:
        """Initialize environment tools.
        
        Args:
            config: Tool configuration
            root_path: Project root path
            subprocess_runner: Subprocess runner for dependency injection
        """
        self.config = config
        self.root_path = root_path
        self.cache_dir = root_path / ".cache"
        self.venv_path = root_path / ".venv"
        self.pkg_name = "ml_playground"
        self.subprocess_runner = subprocess_runner or _default_runner
    
    @property
    def category(self) -> str:
        """Tool category identifier."""
        return "env"
    
    def setup(self, args: List[str], clear: bool = False) -> ToolResult:
        """Create a fresh uv-managed virtual environment and install all dependencies.
        
        Args:
            args: Additional arguments (ignored)
            clear: Whether to remove existing virtual environment first
            
        Returns:
            ToolResult with execution details
        """
        operation_id = OperationId(namespace="tools", category=self.category, command="setup")
        
        results = []
        
        # Clear existing venv if requested
        if clear and self.venv_path.exists():
            try:
                shutil.rmtree(self.venv_path, ignore_errors=True)
                results.append("Removed existing virtual environment")
            except Exception as exc:
                raise EnvironmentSetupError(
                    "Failed to remove existing virtual environment",
                    reason=f"Could not delete {self.venv_path}: {exc}",
                    rationale="Virtual environment must be cleanly removed before setup"
                ) from exc
        
        # Create virtual environment
        venv_result = self.subprocess_runner.run_uv_command(
            ["venv"] + (["--clear"] if clear else []),
            cwd=self.root_path,
            timeout=self.config.environment.timeout,
            operation_id=operation_id,
        )
        
        if not venv_result.success:
            return venv_result
        
        results.append("Created virtual environment")
        
        # Sync all dependencies
        sync_result = self.subprocess_runner.run_uv_command(
            ["sync", "--all-groups"],
            cwd=self.root_path,
            timeout=self.config.environment.timeout,
            operation_id=operation_id,
        )
        
        if not sync_result.success:
            return sync_result
        
        results.append("Synchronized all dependency groups")
        
        # Combine outputs
        combined_stdout = venv_result.stdout
        if sync_result.stdout:
            combined_stdout += f"\n{sync_result.stdout}"
        
        combined_stderr = venv_result.stderr
        if sync_result.stderr:
            combined_stderr += f"\n{sync_result.stderr}"
        
        output = "\n".join(results)
        if combined_stdout:
            output += f"\n\nCommand output:\n{combined_stdout}"
        
        return ToolResult(
            success=True,
            exit_code=0,
            stdout=output,
            stderr=combined_stderr,
            operation_id=operation_id,
        )
    
    def sync(
        self, 
        args: List[str], 
        groups: Optional[List[str]] = None,
        all_groups: bool = False,
        frozen: bool = False
    ) -> ToolResult:
        """Sync project dependencies using uv.
        
        Args:
            args: Additional uv sync arguments
            groups: Specific dependency groups to sync
            all_groups: Whether to install all optional dependency groups
            frozen: Use existing lockfile without resolving new versions
            
        Returns:
            ToolResult with execution details
        """
        operation_id = OperationId(namespace="tools", category=self.category, command="sync")
        
        sync_args = ["sync"]
        
        if frozen:
            sync_args.append("--frozen")
        
        if all_groups:
            sync_args.append("--all-groups")
        elif groups:
            for group in groups:
                sync_args.extend(["--group", group])
        
        # Add any additional arguments
        sync_args.extend(args)
        
        return self.subprocess_runner.run_uv_command(
            sync_args,
            cwd=self.root_path,
            timeout=self.config.environment.timeout,
            operation_id=operation_id,
        )
    
    def verify(self, args: List[str]) -> ToolResult:
        """Ensure the project package imports correctly.
        
        Args:
            args: Additional arguments (ignored)
            
        Returns:
            ToolResult with execution details
        """
        operation_id = OperationId(namespace="tools", category=self.category, command="verify")
        
        # Test package import
        import_cmd = [
            "python", 
            "-c", 
            f"import {self.pkg_name}; print('✓ {self.pkg_name} import OK')"
        ]
        
        return self.subprocess_runner.run_uv_command(
            import_cmd,
            cwd=self.root_path,
            timeout=self.config.environment.timeout,
            operation_id=operation_id,
        )
    
    def clean(self, args: List[str]) -> ToolResult:
        """Remove caches and temporary build artifacts.
        
        Args:
            args: Additional arguments (ignored)
            
        Returns:
            ToolResult with execution details
        """
        operation_id = OperationId(namespace="tools", category=self.category, command="clean")
        
        # Check cache directory before cleanup
        before_entries = []
        if self.cache_dir.exists():
            before_entries = sorted(self.cache_dir.iterdir())
        
        # Define cleanup targets
        cache_targets = [
            self.cache_dir / "pytest",
            self.cache_dir / "coverage", 
            self.cache_dir / "hypothesis",
            self.cache_dir / "pre-commit",
            self.cache_dir / "ruff",
            self.cache_dir / "uv",
            self.cache_dir / "mypy",
        ]
        
        build_targets = [
            self.root_path / "htmlcov",
            self.root_path / "build",
            self.root_path / "dist",
            self.root_path / "*.egg-info",
        ]
        
        # Clean cache directories
        cleaned_paths = []
        for target in cache_targets + build_targets:
            if target.name.endswith("*.egg-info"):
                # Handle glob pattern for egg-info directories
                for egg_info in self.root_path.glob("*.egg-info"):
                    if egg_info.exists():
                        shutil.rmtree(egg_info, ignore_errors=True)
                        cleaned_paths.append(str(egg_info.relative_to(self.root_path)))
            elif target.exists():
                if target.is_dir():
                    shutil.rmtree(target, ignore_errors=True)
                else:
                    target.unlink(missing_ok=True)
                cleaned_paths.append(str(target.relative_to(self.root_path)))
        
        # Clean __pycache__ directories
        pycache_count = 0
        for pycache in self.root_path.rglob("__pycache__"):
            if pycache.exists():
                shutil.rmtree(pycache, ignore_errors=True)
                pycache_count += 1
        
        # Prepare output
        output_lines = []
        
        if before_entries:
            output_lines.append("Cache contents before cleanup:")
            for entry in before_entries:
                output_lines.append(f"  - {entry.relative_to(self.root_path)}")
        else:
            output_lines.append("Cache directory was empty or missing")
        
        if cleaned_paths:
            output_lines.append(f"\nCleaned {len(cleaned_paths)} cache/build paths:")
            for path in cleaned_paths:
                output_lines.append(f"  - {path}")
        
        if pycache_count > 0:
            output_lines.append(f"\nRemoved {pycache_count} __pycache__ directories")
        
        # Check cache directory after cleanup
        after_entries = []
        if self.cache_dir.exists():
            after_entries = sorted(self.cache_dir.iterdir())
        
        if after_entries:
            output_lines.append("\nCache contents after cleanup:")
            for entry in after_entries:
                output_lines.append(f"  - {entry.relative_to(self.root_path)}")
        else:
            output_lines.append("\nCache directory is now empty or removed")
        
        return ToolResult(
            success=True,
            exit_code=0,
            stdout="\n".join(output_lines),
            stderr="",
            operation_id=operation_id,
        )
    
    def info(self, args: List[str]) -> ToolResult:
        """Show environment information.
        
        Args:
            args: Additional arguments (ignored)
            
        Returns:
            ToolResult with execution details
        """
        operation_id = OperationId(namespace="tools", category=self.category, command="info")
        
        info_lines = []
        
        # Project information
        info_lines.append(f"Project root: {self.root_path}")
        info_lines.append(f"Package name: {self.pkg_name}")
        
        # Virtual environment
        if self.venv_path.exists():
            info_lines.append(f"Virtual environment: {self.venv_path} (exists)")
        else:
            info_lines.append(f"Virtual environment: {self.venv_path} (missing)")
        
        # Cache directory
        if self.cache_dir.exists():
            cache_size = sum(
                f.stat().st_size 
                for f in self.cache_dir.rglob("*") 
                if f.is_file()
            )
            cache_size_mb = cache_size / (1024 * 1024)
            info_lines.append(f"Cache directory: {self.cache_dir} ({cache_size_mb:.1f} MB)")
        else:
            info_lines.append(f"Cache directory: {self.cache_dir} (missing)")
        
        # Check if package is importable
        try:
            import_result = self.subprocess_runner.run_uv_command(
                ["python", "-c", f"import {self.pkg_name}; print('OK')"],
                cwd=self.root_path,
                timeout=30,  # Short timeout for info check
                operation_id=operation_id,
            )
            if import_result.success:
                info_lines.append(f"Package import: ✓ {self.pkg_name} imports successfully")
            else:
                info_lines.append(f"Package import: ✗ {self.pkg_name} import failed")
        except Exception:
            info_lines.append(f"Package import: ✗ Could not test {self.pkg_name} import")
        
        return ToolResult(
            success=True,
            exit_code=0,
            stdout="\n".join(info_lines),
            stderr="",
            operation_id=operation_id,
        )
    
    def ai_guidelines(self, args: List[str], tool: str, dry_run: bool = False) -> ToolResult:
        """Set up AI guideline symlinks for the requested tool.
        
        Args:
            args: Additional arguments (ignored)
            tool: Target tool name for AI guidelines
            dry_run: Whether to preview actions without executing
            
        Returns:
            ToolResult with execution details
        """
        operation_id = OperationId(namespace="tools", category=self.category, command="ai-guidelines")
        
        if not tool.strip():
            raise ToolExecutionError(
                "Missing tool name for AI guidelines setup",
                reason="Tool name argument is required but was empty",
                rationale="AI guidelines setup requires a specific tool name to configure"
            )
        
        # Build command
        cmd = ["python", "tools/setup_ai_guidelines.py", tool]
        if dry_run:
            cmd.append("--dry-run")
        
        return self.subprocess_runner.run_uv_command(
            cmd,
            cwd=self.root_path,
            timeout=self.config.environment.timeout,
            operation_id=operation_id,
        )
    
    def tensorboard(
        self, 
        args: List[str], 
        logdir: Path, 
        port: int = 6006, 
        host: str = "127.0.0.1"
    ) -> ToolResult:
        """Launch TensorBoard for the given log directory.
        
        Args:
            args: Additional tensorboard arguments
            logdir: TensorBoard log directory
            port: Port to bind TensorBoard to
            host: Host interface to bind to
            
        Returns:
            ToolResult with execution details
        """
        operation_id = OperationId(namespace="tools", category=self.category, command="tensorboard")
        
        if not logdir.exists():
            raise ToolExecutionError(
                f"TensorBoard log directory does not exist: {logdir}",
                reason="Specified logdir path is not accessible",
                rationale="TensorBoard requires an existing directory with log files"
            )
        
        if not logdir.is_dir():
            raise ToolExecutionError(
                f"TensorBoard logdir is not a directory: {logdir}",
                reason="Logdir path points to a file, not a directory",
                rationale="TensorBoard requires a directory containing log files"
            )
        
        # Build tensorboard command
        cmd = [
            "tensorboard",
            "--logdir", str(logdir),
            "--port", str(port),
            "--host", host,
        ]
        cmd.extend(args)
        
        return self.subprocess_runner.run_uv_command(
            cmd,
            cwd=self.root_path,
            timeout=self.config.environment.timeout,
            operation_id=operation_id,
        )
    
    def gguf_help(self, args: List[str]) -> ToolResult:
        """Show llama.cpp GGUF conversion help.
        
        Args:
            args: Additional arguments (ignored)
            
        Returns:
            ToolResult with execution details
        """
        operation_id = OperationId(namespace="tools", category=self.category, command="gguf-help")
        
        # Run the GGUF converter help
        result = self.subprocess_runner.run_uv_command(
            ["python", "tools/llama_cpp/convert-hf-to-gguf.py", "--help"],
            cwd=self.root_path,
            timeout=self.config.environment.timeout,
            operation_id=operation_id,
        )
        
        # GGUF converter may exit with non-zero status even for help
        # This is expected behavior, so we adjust the result
        if not result.success and "usage:" in result.stdout.lower():
            # Help was displayed successfully despite non-zero exit
            return ToolResult(
                success=True,
                exit_code=0,
                stdout=result.stdout,
                stderr="GGUF converter help displayed (exit code adjusted)",
                operation_id=operation_id,
            )
        
        return result