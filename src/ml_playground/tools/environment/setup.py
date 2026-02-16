"""Environment setup functionality for environment tools."""

from __future__ import annotations

import shutil
from pathlib import Path
from typing import List, Optional

from ..core.config import ToolsConfig
from ..core.errors import EnvironmentSetupError
from ..core.interfaces import OperationId, ToolResult
from ..utils.subprocess_utils import SubprocessRunner
from .constants import (
    DEFAULT_SYNC_GROUP,
    PRE_COMMIT_CONFIG_PATH,
    resolve_pre_commit_hook_template,
)


def run_setup(
    config: ToolsConfig,
    root_path: Path,
    venv_path: Path,
    pkg_name: str,
    args: List[str],
    clear: bool,
    subprocess_runner: SubprocessRunner,
) -> ToolResult:
    """Create a fresh uv-managed virtual environment and install all dependencies.

    Args:
        config: Tool configuration
        root_path: Project root path
        venv_path: Virtual environment path
        pkg_name: Package name for hooks setup
        args: Additional arguments (ignored)
        clear: Whether to remove existing virtual environment first
        subprocess_runner: Subprocess runner

    Returns:
        ToolResult with execution details
    """
    operation_id = OperationId(namespace="tools", category="env", command="setup")

    results: list[str] = []

    backup_path: Path | None = None

    # Clear existing venv if requested
    if clear and venv_path.exists():
        try:
            backup_path = _venv_backup_path(venv_path)
            if backup_path.exists():
                shutil.rmtree(backup_path, ignore_errors=True)
            venv_path.rename(backup_path)
            results.append(f"Backed up existing virtual environment to {backup_path}")
        except OSError as exc:
            raise EnvironmentSetupError(
                "Failed to remove existing virtual environment",
                reason=f"Could not prepare backup for {venv_path}: {exc}",
                rationale="Virtual environment backup must succeed before clear setup",
            ) from exc

    # Create virtual environment
    venv_result = subprocess_runner.run_subprocess(
        ["uv", "venv"] + (["--clear"] if clear else []),
        cwd=root_path,
        timeout=config.environment.timeout,
        operation_id=operation_id,
    )

    if not venv_result.success:
        _restore_venv_backup(venv_path, backup_path, results)
        return venv_result

    results.append("Created virtual environment")

    # Sync all dependencies
    sync_command = ["uv", "sync", "--group", DEFAULT_SYNC_GROUP]
    sync_command.extend(args)
    sync_result = subprocess_runner.run_subprocess(
        sync_command,
        cwd=root_path,
        timeout=config.environment.timeout,
        operation_id=operation_id,
    )

    if not sync_result.success:
        _restore_venv_backup(venv_path, backup_path, results)
        return sync_result

    results.append("Synchronized all dependency groups")

    # Setup git hooks
    hooks_result = _setup_git_hooks(
        config, root_path, pkg_name, operation_id, subprocess_runner
    )
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


def _venv_backup_path(venv_path: Path) -> Path:
    return venv_path.parent / f"{venv_path.name}.backup"


def _restore_venv_backup(
    venv_path: Path, backup_path: Path | None, results: list[str]
) -> None:
    if backup_path is None or not backup_path.exists():
        return
    if venv_path.exists():
        shutil.rmtree(venv_path, ignore_errors=True)
    backup_path.rename(venv_path)
    results.append("Restored previous virtual environment from backup")


def run_sync(
    config: ToolsConfig,
    root_path: Path,
    args: List[str],
    groups: Optional[List[str]],
    all_groups: bool,
    frozen: bool,
    subprocess_runner: SubprocessRunner,
) -> ToolResult:
    """Sync project dependencies using uv.

    Args:
        config: Tool configuration
        root_path: Project root path
        args: Additional uv sync arguments
        groups: Specific dependency groups to sync
        all_groups: Whether to install all optional dependency groups
        frozen: Use existing lockfile without resolving new versions
        subprocess_runner: Subprocess runner

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
    command.extend(sync_args[1:])
    return subprocess_runner.run_subprocess(
        command,
        cwd=root_path,
        timeout=config.environment.timeout,
        operation_id=operation_id,
    )


def _setup_git_hooks(
    config: ToolsConfig,
    root_path: Path,
    pkg_name: str,
    operation_id: OperationId,
    subprocess_runner: SubprocessRunner,
) -> ToolResult:
    """Setup git hooks by creating pre-commit hook in .git/hooks.

    Args:
        config: Tool configuration
        root_path: Project root path
        pkg_name: Package name for mocking checks
        operation_id: Operation identifier for tracking
        subprocess_runner: Subprocess runner

    Returns:
        ToolResult with setup status
    """
    git_hooks_dir = root_path / ".git" / "hooks"

    # Handle git worktrees
    git_file = root_path / ".git"
    if git_file.is_file():
        # This is a worktree, read the gitdir
        try:
            gitdir_content = git_file.read_text().strip()
            if gitdir_content.startswith("gitdir: "):
                gitdir_path = gitdir_content[8:]  # Remove "gitdir: " prefix
                git_hooks_dir = Path(gitdir_path) / "hooks"
        except OSError:
            pass  # Fall back to default behavior

    try:
        # Create hooks directory if it doesn't exist
        git_hooks_dir.mkdir(parents=True, exist_ok=True)

        # Create pre-commit hook from the repository source-of-truth template.
        pre_commit_hook = git_hooks_dir / "pre-commit"
        hook_content = resolve_pre_commit_hook_template(root_path)

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

    except OSError as exc:
        return ToolResult(
            success=False,
            exit_code=1,
            stdout="",
            stderr=f"Failed to setup git hooks: {exc}",
            operation_id=operation_id,
        )
