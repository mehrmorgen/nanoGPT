"""Environment setup functionality for environment tools."""

from __future__ import annotations

import shutil
from pathlib import Path
from typing import List, Optional

from ..core.config import ToolsConfig
from ..core.errors import EnvironmentSetupError
from ..core.interfaces import OperationId, ToolResult
from ..utils.subprocess_utils import SubprocessRunner


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

    # Clear existing venv if requested
    if clear and venv_path.exists():
        try:
            shutil.rmtree(venv_path, ignore_errors=True)
            results.append("Removed existing virtual environment")
        except Exception as exc:
            raise EnvironmentSetupError(
                "Failed to remove existing virtual environment",
                reason=f"Could not delete {venv_path}: {exc}",
                rationale="Virtual environment must be cleanly removed before setup",
            ) from exc

    # Create virtual environment
    venv_result = subprocess_runner.run_uv_command(
        ["venv"] + (["--clear"] if clear else []),
        cwd=root_path,
        timeout=config.environment.timeout,
        operation_id=operation_id,
    )

    if not venv_result.success:
        return venv_result

    results.append("Created virtual environment")

    # Sync all dependencies
    sync_result = subprocess_runner.run_uv_command(
        ["sync", "--all-groups"],
        cwd=root_path,
        timeout=config.environment.timeout,
        operation_id=operation_id,
    )

    if not sync_result.success:
        return sync_result

    results.append("Synchronized all dependency groups")

    # Setup git hooks
    hooks_result = _setup_git_hooks(config, root_path, pkg_name, operation_id, subprocess_runner)
    if hooks_result.success:
        results.append("Configured git hooks")
    else:
        # Don't fail setup if hooks fail, just warn
        results.append(f"Warning: Git hooks setup failed: {hooks_result.stderr}")

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
        sync_args.append("--all-groups")
    elif groups:
        for group in groups:
            sync_args.extend(["--group", group])

    # Add any additional arguments
    sync_args.extend(args)

    return subprocess_runner.run_uv_command(
        sync_args,
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
        except Exception:
            pass  # Fall back to default behavior

    try:
        # Create hooks directory if it doesn't exist
        git_hooks_dir.mkdir(parents=True, exist_ok=True)

        # Create pre-commit hook
        pre_commit_hook = git_hooks_dir / "pre-commit"
        hook_content = f'''#!/usr/bin/env bash
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
      for t in "${{tokens[@]}}"; do
        lines=$(grep -n "$t" "$f" || true)
        # Drop matches where the actual file content (after the colon) is a comment
        # or where the token only appears inside string literals (e.g., "patch(")
        lines=$(echo "$lines" | awk -F: -v tok="$t" '
          function has_token_outside(line, tok,    i, c, in_single, in_double, tok_len) {{
            in_single = 0; in_double = 0; tok_len = length(tok);
            for (i = 1; i <= length(line) - tok_len + 1; i++) {{
              c = substr(line, i, 1);
              if (c == "\"" && substr(line, i - 1, 1) != "\\\\") {{
                in_double = !in_double;
              }} else if (c == "'"'"'" && substr(line, i - 1, 1) != "\\\\") {{
                in_single = !in_single;
              }}
              if (!in_single && !in_double && substr(line, i, tok_len) == tok) {{
                return 1;
              }}
            }}
            return 0;
          }}
          {{
            line = $0; sub(/^[^:]*:/, "", line);
            if (line ~ /^[[:space:]]*#/) next;
            if (has_token_outside(line, tok)) print $0;
          }}
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
uv run pre-commit run

# Note: Mutation testing is excluded from pre-commit. Run manually via `make quality-ext` when needed.
'''

        pre_commit_hook.write_text(hook_content)
        pre_commit_hook.chmod(0o755)

        return ToolResult(
            success=True,
            exit_code=0,
            stdout=f"Created pre-commit hook at {pre_commit_hook}. Pre-commit config is in .pre-commit-config.yaml.",
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
