from __future__ import annotations

from pathlib import Path

from ml_playground.tools.core.config import ToolsConfig
from ml_playground.tools.core.interfaces import OperationId
from ml_playground.tools.environment.environment import EnvironmentTools
from tests.unit.tools.fakes import FakeSubprocessRunner


def test_setup_git_hooks_creates_hook_in_standard_repo(tmp_path: Path) -> None:
    cfg = ToolsConfig()
    tools = EnvironmentTools(cfg, tmp_path, subprocess_runner=FakeSubprocessRunner())
    op_id = OperationId(namespace="tools", category="env", command="setup-git-hooks")
    (tmp_path / ".githooks").mkdir(parents=True, exist_ok=True)
    (tmp_path / ".githooks" / "pre-commit").write_text(
        "#!/usr/bin/env bash\nuv run pre-commit run -v --config .githooks/.pre-commit-config.yaml\n",
        encoding="utf-8",
    )

    result = tools._setup_git_hooks(op_id)  # type: ignore[attr-defined]

    hook = tmp_path / ".git" / "hooks" / "pre-commit"
    assert result.success is True
    assert hook.exists()
    content = hook.read_text(encoding="utf-8")
    assert "pre-commit" in content
    assert "--config .githooks/.pre-commit-config.yaml" in content
    assert "pre-commit run -v --config .githooks/.pre-commit-config.yaml" in content


def test_setup_git_hooks_uses_worktree_gitdir(tmp_path: Path) -> None:
    cfg = ToolsConfig()
    tools = EnvironmentTools(cfg, tmp_path, subprocess_runner=FakeSubprocessRunner())
    op_id = OperationId(namespace="tools", category="env", command="setup-git-hooks")
    (tmp_path / ".githooks").mkdir(parents=True, exist_ok=True)
    (tmp_path / ".githooks" / "pre-commit").write_text(
        "#!/usr/bin/env bash\nuv run pre-commit run -v --config .githooks/.pre-commit-config.yaml\n",
        encoding="utf-8",
    )

    # Simulate a worktree .git file pointing at alternate gitdir
    worktree_gitdir = tmp_path / "worktrees" / "main.git"
    hooks_dir = worktree_gitdir / "hooks"
    worktree_gitdir.parent.mkdir(parents=True, exist_ok=True)
    (tmp_path / ".git").write_text(f"gitdir: {worktree_gitdir}", encoding="utf-8")

    result = tools._setup_git_hooks(op_id)  # type: ignore[attr-defined]

    hook = hooks_dir / "pre-commit"
    assert result.success is True
    assert hook.exists()
    content = hook.read_text(encoding="utf-8")
    assert "--config .githooks/.pre-commit-config.yaml" in content


def test_setup_git_hooks_handles_io_error(tmp_path: Path) -> None:
    cfg = ToolsConfig()
    tools = EnvironmentTools(cfg, tmp_path, subprocess_runner=FakeSubprocessRunner())
    op_id = OperationId(namespace="tools", category="env", command="setup-git-hooks")

    # Point .git/hooks at a location we cannot create (simulate by making .git a file
    # and writing invalid gitdir content that will cause an exception when used).
    git_file = tmp_path / ".git"
    git_file.write_text("not-a-gitdir", encoding="utf-8")

    result = tools._setup_git_hooks(op_id)  # type: ignore[attr-defined]

    assert result.success is False
    assert "Failed to setup git hooks" in result.stderr


def test_setup_git_hooks_uses_fallback_when_template_missing(tmp_path: Path) -> None:
    cfg = ToolsConfig()
    tools = EnvironmentTools(cfg, tmp_path, subprocess_runner=FakeSubprocessRunner())
    op_id = OperationId(namespace="tools", category="env", command="setup-git-hooks")

    result = tools._setup_git_hooks(op_id)  # type: ignore[attr-defined]

    hook = tmp_path / ".git" / "hooks" / "pre-commit"
    assert result.success is True
    assert hook.exists()
    content = hook.read_text(encoding="utf-8")
    assert "missing or unreadable" in content


def test_ai_guidelines_tensorboard_and_gguf_help_delegate(tmp_path: Path) -> None:
    cfg = ToolsConfig()
    runner = FakeSubprocessRunner()
    tools = EnvironmentTools(cfg, tmp_path, subprocess_runner=runner)

    logdir = tmp_path / "logs"
    logdir.mkdir()

    tools.ai_guidelines([], tool="pre-commit")
    tools.tensorboard([], logdir=logdir)
    tools.gguf_help([])

    # All three methods should send commands through the subprocess runner
    assert runner.calls, "Expected subprocess calls for environment helpers"
