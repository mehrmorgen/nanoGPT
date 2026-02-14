"""Environment tools category implementation."""

from __future__ import annotations

import os
import shutil
from pathlib import Path
from typing import List, Optional

from ml_playground.tools.core.config import ToolsConfig
from ml_playground.tools.core.errors import EnvironmentSetupError, ToolExecutionError
from ml_playground.framework.core.project_config import get_default_host
from ml_playground.tools.core.interfaces import OperationId, ToolResult
from ml_playground.tools.utils.subprocess_utils import SubprocessRunner, DEFAULT_RUNNER
from ml_playground.tools.dev.dev import DevTools
from .constants import (
    DEFAULT_SYNC_GROUP,
    PRE_COMMIT_CONFIG_PATH,
    REQUIRED_ENV_TOOLS,
)


class EnvironmentTools:
    """Environment management tools implementation."""

    def __init__(
        self,
        config: ToolsConfig,
        root_path: Path,
        subprocess_runner: Optional[SubprocessRunner] = None,
    ) -> None:
        """Initialize environment tools.

        Args:
            config: Tool configuration
            root_path: Project root path
            subprocess_runner: Subprocess runner for dependency injection
        """
        self._config = config
        self._root_path = root_path
        self._cache_dir = root_path / ".cache"
        self._venv_path = root_path / ".venv"
        self._pkg_name = "ml_playground"
        self._subprocess_runner = subprocess_runner or DEFAULT_RUNNER

    @property
    def config(self) -> ToolsConfig:
        return self._config

    @property
    def root_path(self) -> Path:
        return self._root_path

    @property
    def cache_dir(self) -> Path:
        return self._cache_dir

    @property
    def venv_path(self) -> Path:
        return self._venv_path

    @property
    def pkg_name(self) -> str:
        return self._pkg_name

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
        operation_id = OperationId(namespace="tools", category="env", command="setup")

        results: list[str] = []
        backup_path: Path | None = None

        # Clear existing venv if requested
        if clear and self._venv_path.exists():
            try:
                backup_path = self._venv_backup_path()
                if backup_path.exists():
                    shutil.rmtree(backup_path, ignore_errors=True)
                self._venv_path.rename(backup_path)
                results.append(
                    f"Backed up existing virtual environment to {backup_path}"
                )
            except Exception as exc:
                raise EnvironmentSetupError(
                    "Failed to remove existing virtual environment",
                    reason=f"Could not prepare backup for {self._venv_path}: {exc}",
                    rationale="Virtual environment backup must succeed before clear setup",
                ) from exc

        # Create virtual environment
        venv_command = ["uv", "venv"] + (["--clear"] if clear else [])
        venv_result = self._subprocess_runner.run_subprocess(
            venv_command,
            cwd=self._root_path,
            timeout=self._config.environment.timeout,
            operation_id=operation_id,
        )

        if not venv_result.success:
            self._restore_venv_backup(backup_path, results)
            return venv_result

        results.append("Created virtual environment")

        # Sync all dependencies
        sync_command = ["uv", "sync", "--group", DEFAULT_SYNC_GROUP]
        sync_command.extend(args)
        sync_result = self._subprocess_runner.run_subprocess(
            sync_command,
            cwd=self._root_path,
            timeout=self._config.environment.timeout,
            operation_id=operation_id,
        )

        if not sync_result.success:
            self._restore_venv_backup(backup_path, results)
            return sync_result

        results.append("Synchronized all dependency groups")

        # Setup git hooks
        hooks_result = self._setup_git_hooks(operation_id)
        if hooks_result.success:
            results.append("Configured git hooks")
        else:
            # Don't fail setup if hooks fail, just warn
            results.append(f"Warning: Git hooks setup failed: {hooks_result.stderr}")

        if backup_path is not None and backup_path.exists():
            shutil.rmtree(backup_path, ignore_errors=True)
            results.append("Removed virtual environment backup")

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

    def _venv_backup_path(self) -> Path:
        return self._venv_path.parent / f"{self._venv_path.name}.backup"

    def _restore_venv_backup(
        self, backup_path: Path | None, results: list[str]
    ) -> None:
        if backup_path is None or not backup_path.exists():
            return
        if self._venv_path.exists():
            shutil.rmtree(self._venv_path, ignore_errors=True)
        backup_path.rename(self._venv_path)
        results.append("Restored previous virtual environment from backup")

    def sync(
        self,
        args: List[str],
        groups: Optional[List[str]] = None,
        all_groups: bool = False,
        frozen: bool = False,
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
        operation_id = OperationId(namespace="tools", category="env", command="sync")

        sync_args = ["sync"]

        if frozen:
            sync_args.append("--frozen")

        if all_groups:
            sync_args.extend(["--group", DEFAULT_SYNC_GROUP])
        elif groups:
            for group in groups:
                sync_args.extend(["--group", group])

        # Add any additional arguments
        sync_args.extend(args)

        command = ["uv", "sync"]
        command.extend(sync_args[1:])  # skip the initial "sync"
        return self._subprocess_runner.run_subprocess(
            command,
            cwd=self._root_path,
            timeout=self._config.environment.timeout,
            operation_id=operation_id,
        )

    def verify(self, args: List[str]) -> ToolResult:
        """Ensure the project package imports correctly.

        Args:
            args: Additional arguments (ignored)

        Returns:
            ToolResult with execution details
        """
        operation_id = OperationId(namespace="tools", category="env", command="verify")

        verify_cmd = [
            "python",
            "-c",
            (
                "import shutil, sys; "
                f"import {self._pkg_name}; "
                f"required = {list(REQUIRED_ENV_TOOLS)!r}; "
                "missing = [name for name in required if shutil.which(name) is None]; "
                f"print('✓ {self._pkg_name} import OK'); "
                "print('✓ env toolchain OK' if not missing else 'missing: ' + ', '.join(missing)); "
                "raise SystemExit(0 if not missing else 1)"
            ),
        ]
        verify_result = self._subprocess_runner.run_uv_command(
            verify_cmd,
            cwd=self._root_path,
            timeout=self._config.environment.timeout,
            operation_id=operation_id,
        )
        if verify_result.success:
            return verify_result

        remediation = (
            f"Missing required tooling for quality gates ({', '.join(REQUIRED_ENV_TOOLS)}). "
            f"Run `uv sync --group {DEFAULT_SYNC_GROUP}` or `uv run tools env setup --clear`."
        )
        stderr = (
            f"{verify_result.stderr}\n{remediation}"
            if verify_result.stderr
            else remediation
        )
        return ToolResult(
            success=False,
            exit_code=verify_result.exit_code,
            stdout=verify_result.stdout,
            stderr=stderr,
            operation_id=operation_id,
        )

    def clean(self, args: List[str]) -> ToolResult:
        """Remove caches and temporary build artifacts.

        Args:
            args: Additional arguments (ignored)

        Returns:
            ToolResult with execution details
        """
        operation_id = OperationId(namespace="tools", category="env", command="clean")

        # Check cache directory before cleanup
        before_entries: list[Path] = []
        if self._cache_dir.exists():
            before_entries = sorted(self._cache_dir.iterdir())

        # Define cleanup targets
        cache_targets: list[Path] = [
            self._cache_dir / "pytest",
            self._cache_dir / "coverage",
            self._cache_dir / "hypothesis",
            self._cache_dir / "pre-commit",
            self._cache_dir / "ruff",
            self._cache_dir / "uv",
            self._cache_dir / "mypy",
        ]

        build_targets: list[Path] = [
            self._root_path / "htmlcov",
            self._root_path / "build",
            self._root_path / "dist",
        ]

        # Clean cache directories
        cleaned_paths: list[str] = []
        for target in cache_targets + build_targets:
            if target.exists():
                if target.is_dir():
                    shutil.rmtree(target, ignore_errors=True)
                else:
                    target.unlink(missing_ok=True)
                cleaned_paths.append(str(target.relative_to(self._root_path)))

        # Handle glob pattern for egg-info directories
        for egg_info in self._root_path.glob("*.egg-info"):
            if egg_info.exists():
                shutil.rmtree(egg_info, ignore_errors=True)
                cleaned_paths.append(str(egg_info.relative_to(self._root_path)))

        # Clean __pycache__ directories while skipping heavyweight/external trees.
        # This keeps `tools env clean` fast and deterministic in large repos.
        pycache_count = 0
        skip_dirs = {".venv", ".git", ".direnv", ".nix", "node_modules"}
        scan_roots = [
            self._root_path / "src",
            self._root_path / "tests",
            self._root_path / "tools",
            self._root_path / "scripts",
        ]
        for base in scan_roots:
            if not base.exists():
                continue
            for root, dirs, _files in os.walk(base, topdown=True):
                dirs[:] = [
                    directory for directory in dirs if directory not in skip_dirs
                ]
                if "__pycache__" not in dirs:
                    continue

                pycache = Path(root) / "__pycache__"
                if pycache.exists():
                    shutil.rmtree(pycache, ignore_errors=True)
                    pycache_count += 1
                dirs.remove("__pycache__")

        # Prepare output
        output_lines: list[str] = []

        if before_entries:
            output_lines.append("Cache contents before cleanup:")
            for entry in before_entries:
                output_lines.append(f"  - {entry.relative_to(self._root_path)}")
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
        if self._cache_dir.exists():
            after_entries = sorted(self._cache_dir.iterdir())

        if after_entries:
            output_lines.append("\nCache contents after cleanup:")
            for entry in after_entries:
                output_lines.append(f"  - {entry.relative_to(self._root_path)}")
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
        operation_id = OperationId(namespace="tools", category="env", command="info")

        info_lines: list[str] = []

        # Project information
        info_lines.append(f"Project root: {self._root_path}")
        info_lines.append(f"Package name: {self._pkg_name}")

        # Virtual environment
        if self._venv_path.exists():
            info_lines.append(f"Virtual environment: {self._venv_path} (exists)")
        else:
            info_lines.append(f"Virtual environment: {self._venv_path} (missing)")

        # Cache directory
        if self._cache_dir.exists():
            cache_size = sum(
                f.stat().st_size for f in self._cache_dir.rglob("*") if f.is_file()
            )
            cache_size_mb = cache_size / (1024 * 1024)
            info_lines.append(
                f"Cache directory: {self._cache_dir} ({cache_size_mb:.1f} MB)"
            )
        else:
            info_lines.append(f"Cache directory: {self._cache_dir} (missing)")

        # Check if package is importable
        try:
            import_result = self._subprocess_runner.run_uv_command(
                ["python", "-c", f"import {self._pkg_name}; print('OK')"],
                cwd=self._root_path,
                timeout=30,  # Short timeout for info check
                operation_id=operation_id,
            )
            if import_result.success:
                info_lines.append(
                    f"Package import: ✓ {self._pkg_name} imports successfully"
                )
            else:
                info_lines.append(f"Package import: ✗ {self._pkg_name} import failed")
        except Exception:
            info_lines.append(
                f"Package import: ✗ Could not test {self._pkg_name} import"
            )

        return ToolResult(
            success=True,
            exit_code=0,
            stdout="\n".join(info_lines),
            stderr="",
            operation_id=operation_id,
        )

    def _setup_git_hooks(self, operation_id: OperationId) -> ToolResult:
        """Setup git hooks by creating pre-commit hook in .git/hooks.

        Args:
            operation_id: Operation identifier for tracking

        Returns:
            ToolResult with setup status
        """

        git_hooks_dir = self._root_path / ".git" / "hooks"

        # Handle git worktrees
        git_file = self._root_path / ".git"
        if git_file.is_file():
            # This is a worktree, read the gitdir
            try:
                gitdir_content = git_file.read_text().strip()
                if gitdir_content.startswith("gitdir: "):
                    gitdir_path = gitdir_content[8:]  # Remove "gitdir: " prefix
                    git_hooks_dir = Path(gitdir_path) / "hooks"
            except Exception:
                pass  # Fall back to default behavior

        try:
            # Create hooks directory if it doesn't exist
            git_hooks_dir.mkdir(parents=True, exist_ok=True)

            # Create pre-commit hook
            pre_commit_hook = git_hooks_dir / "pre-commit"
            hook_content = """#!/usr/bin/env bash
# Pre-commit hook using pre-commit framework with uv

set -euo pipefail

# Hard enforcement BEFORE pre-commit's isolated staging: scan working tree tests/ for mocks
if files=$(find tests -type f -name '*.py' 2>/dev/null) && [ -n "$files" ]; then
  if [ -n "$files" ]; then
    found=0
    tokens=(
      'monkeypatch'
      'pytest.MonkeyPatch'
      'unittest.mock'
      'from unittest import mock'
      'pytest_mock'
      'MagicMock'
      'patch('
    )
    while IFS= read -r f; do
      for t in "${tokens[@]}"; do
        lines=$(grep -n "$t" "$f" || true)
        # Drop matches where the actual file content (after the colon) is a comment
        # or where the token only appears inside string literals (e.g., "patch(")
        lines=$(echo "$lines" | awk -F: -v tok="$t" '
          function has_token_outside(line, tok,    i, c, in_single, in_double, tok_len) {
            in_single = 0; in_double = 0; tok_len = length(tok);
            for (i = 1; i <= length(line) - tok_len + 1; i++) {
              c = substr(line, i, 1);
              if (c == "\"" && substr(line, i - 1, 1) != "\\\\") {
                in_double = !in_double;
              } else if (c == "'"'"'" && substr(line, i - 1, 1) != "\\\\") {
                in_single = !in_single;
              }
              if (!in_single && !in_double && substr(line, i, tok_len) == tok) {
                return 1;
              }
            }
            return 0;
          }
          {
            line = $0; sub(/^[^:]*:/, "", line);
            if (line ~ /^[[:space:]]*#/) next;
            if (has_token_outside(line, tok)) print $0;
          }
        ' || true)
        if [ -n "$lines" ]; then
          echo "$lines"
          found=1
        fi
      done
    done <<EOF
$(echo "$files")
EOF
    if [ "$found" -eq 1 ]; then
      echo 'Error: found disallowed mocking APIs in tests. Use fixtures/DI per .dev-guidelines/TESTING.md.' >&2
      exit 1
    fi
  fi
fi

echo "[pre-commit] Core quality gates (ruff, format, pyright, mypy, coverage via uv)"
uv run pre-commit run --config .githooks/.pre-commit-config.yaml

# Note: Mutation testing is excluded from pre-commit. Run manually via `make quality-ext` when needed.
"""

            pre_commit_hook.write_text(hook_content)
            pre_commit_hook.chmod(0o755)

            return ToolResult(
                success=True,
                exit_code=0,
                stdout=(
                    f"Created pre-commit hook at {pre_commit_hook}. "
                    f"Pre-commit config is in {PRE_COMMIT_CONFIG_PATH}."
                ),
                stderr="",
                operation_id=operation_id,
            )

        except Exception as exc:
            return ToolResult(
                success=False,
                exit_code=1,
                stdout="",
                stderr=f"Failed to setup git hooks: {exc}",
                operation_id=operation_id,
            )

    def ai_guidelines(
        self, args: List[str], tool: str, dry_run: bool = False
    ) -> ToolResult:
        """Set up AI guideline symlinks for the requested tool.

        Args:
            args: Additional arguments (ignored)
            tool: Target tool name for AI guidelines
            dry_run: Whether to preview actions without executing

        Returns:
            ToolResult with execution details
        """
        operation_id = OperationId(
            namespace="tools", category="env", command="ai-guidelines"
        )

        if not tool.strip():
            raise ToolExecutionError(
                "Missing tool name for AI guidelines setup",
                reason="Tool name argument is required but was empty",
                rationale="AI guidelines setup requires a specific tool name to configure",
            )

        # Delegate to integrated implementation in DevTools
        try:
            dev = DevTools(
                config=self._config,
                root_path=self._root_path,
                subprocess_runner=self._subprocess_runner,
            )
            result = dev.setup_ai_guidelines(tool=tool, dry_run=dry_run)
            # Attach our operation_id for consistent CLI reporting
            result.operation_id = operation_id
            return result
        except Exception as exc:
            return ToolResult(
                success=False,
                exit_code=1,
                stdout="",
                stderr=f"Failed to setup AI guidelines: {exc}",
                operation_id=operation_id,
            )

    def tensorboard(
        self,
        args: List[str],
        logdir: Path,
        port: int = 6006,
        host: Optional[str] = None,
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
        operation_id = OperationId(
            namespace="tools", category="env", command="tensorboard"
        )

        if host is None:
            try:
                host = get_default_host()
            except (ValueError, TypeError):
                host = "127.0.0.1"

        if not logdir.exists():
            raise ToolExecutionError(
                f"TensorBoard log directory does not exist: {logdir}",
                reason="Specified logdir path is not accessible",
                rationale="TensorBoard requires an existing directory with log files",
            )

        if not logdir.is_dir():
            raise ToolExecutionError(
                f"TensorBoard logdir is not a directory: {logdir}",
                reason="Logdir path points to a file, not a directory",
                rationale="TensorBoard requires a directory containing log files",
            )

        # Build tensorboard command
        cmd = [
            "tensorboard",
            "--logdir",
            str(logdir),
            "--port",
            str(port),
            "--host",
            host,
        ]
        cmd.extend(args)

        return self._subprocess_runner.run_uv_command(
            cmd,
            cwd=self._root_path,
            timeout=self._config.environment.timeout,
            operation_id=operation_id,
        )

    def gguf_help(self, args: List[str]) -> ToolResult:
        """Show llama.cpp GGUF conversion help.

        Args:
            args: Additional arguments (ignored)

        Returns:
            ToolResult with execution details
        """
        operation_id = OperationId(
            namespace="tools", category="env", command="gguf-help"
        )

        # Run the GGUF converter help
        result = self._subprocess_runner.run_uv_command(
            ["python", "tools/llama_cpp/convert-hf-to-gguf.py", "--help"],
            cwd=self._root_path,
            timeout=self._config.environment.timeout,
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
