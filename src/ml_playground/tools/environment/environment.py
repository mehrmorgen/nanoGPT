"""Environment tools category implementation."""

from __future__ import annotations

from pathlib import Path
from typing import List, Optional

from ml_playground.tools.core.config import ToolsConfig
from ml_playground.tools.core.interfaces import OperationId, ToolResult
from ml_playground.tools.utils.subprocess_utils import (
    SubprocessRunner,
    RealSubprocessRunner,
)

from .setup import run_setup, run_sync
from .verify import run_verify, run_info
from .clean import run_clean
from .services import run_ai_guidelines, run_tensorboard, run_gguf_help


# Module-level default runner for tests to patch if needed
_default_runner: SubprocessRunner | None = None


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
        self.config = config
        self.root_path = root_path
        self.cache_dir = root_path / ".cache"
        self.venv_path = root_path / ".venv"
        self.pkg_name = "ml_playground"
        # Module-level patch point for tests
        global _default_runner  # noqa: PLW0603 - providing a test patch point
        if _default_runner is None:
            _default_runner = RealSubprocessRunner()
        self.subprocess_runner = subprocess_runner or _default_runner

    @property
    def category(self) -> str:
        """Tool category identifier."""
        return "env"

    def setup(self, args: List[str], clear: bool = False) -> ToolResult:
        """Create a fresh uv-managed virtual environment and install all dependencies."""
        return run_setup(
            config=self.config,
            root_path=self.root_path,
            venv_path=self.venv_path,
            pkg_name=self.pkg_name,
            args=args,
            clear=clear,
            subprocess_runner=self.subprocess_runner,
        )

    def sync(
        self,
        args: List[str],
        groups: Optional[List[str]] = None,
        all_groups: bool = False,
        frozen: bool = False,
    ) -> ToolResult:
        """Sync project dependencies using uv."""
        return run_sync(
            config=self.config,
            root_path=self.root_path,
            args=args,
            groups=groups,
            all_groups=all_groups,
            frozen=frozen,
            subprocess_runner=self.subprocess_runner,
        )

    def verify(self, args: List[str]) -> ToolResult:
        """Ensure the project package imports correctly."""
        return run_verify(
            config=self.config,
            root_path=self.root_path,
            pkg_name=self.pkg_name,
            args=args,
            subprocess_runner=self.subprocess_runner,
        )

    def clean(self, args: List[str]) -> ToolResult:
        """Remove caches and temporary build artifacts."""
        return run_clean(
            config=self.config,
            root_path=self.root_path,
            cache_dir=self.cache_dir,
            args=args,
            subprocess_runner=self.subprocess_runner,
        )

    def info(self, args: List[str]) -> ToolResult:
        """Show environment information."""
        return run_info(
            config=self.config,
            root_path=self.root_path,
            pkg_name=self.pkg_name,
            venv_path=self.venv_path,
            cache_dir=self.cache_dir,
            args=args,
            subprocess_runner=self.subprocess_runner,
        )

    def _setup_git_hooks(self, operation_id: OperationId) -> ToolResult:
        """Setup git hooks by creating pre-commit hook in .git/hooks.

        Args:
            operation_id: Operation identifier for tracking

        Returns:
            ToolResult with setup status
        """

        git_hooks_dir = self.root_path / ".git" / "hooks"

        # Handle git worktrees
        git_file = self.root_path / ".git"
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
uv run pre-commit run

# Note: Mutation testing is excluded from pre-commit. Run manually via `make quality-ext` when needed.
"""

            pre_commit_hook.write_text(hook_content)
            pre_commit_hook.chmod(0o755)

            return ToolResult(
                success=True,
                exit_code=0,
                stdout=f"Created pre-commit hook at {pre_commit_hook}. Pre-commit config is in .pre-commit-config.yaml.",
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

    def ai_guidelines(
        self, args: List[str], tool: str, dry_run: bool = False
    ) -> ToolResult:
        """Set up AI guideline symlinks for the requested tool."""
        return run_ai_guidelines(
            config=self.config,
            root_path=self.root_path,
            args=args,
            tool=tool,
            dry_run=dry_run,
            subprocess_runner=self.subprocess_runner,
        )

    def tensorboard(
        self, args: List[str], logdir: Path, port: int = 6006, host: str = "127.0.0.1"
    ) -> ToolResult:
        """Launch TensorBoard for the given log directory."""
        return run_tensorboard(
            config=self.config,
            root_path=self.root_path,
            args=args,
            logdir=logdir,
            port=port,
            host=host,
            subprocess_runner=self.subprocess_runner,
        )

    def gguf_help(self, args: List[str]) -> ToolResult:
        """Show llama.cpp GGUF conversion help."""
        return run_gguf_help(
            config=self.config,
            root_path=self.root_path,
            args=args,
            subprocess_runner=self.subprocess_runner,
        )
