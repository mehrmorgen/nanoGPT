from __future__ import annotations

import os
from pathlib import Path
from types import SimpleNamespace

from ml_playground.tools.dev import ai_guidelines
from ml_playground.tools.dev.ai_guidelines import (
    TOOL_MAP,
    SetupResult,
    ToolSpec,
    run_setup_ai_guidelines,
)


def test_run_setup_ai_guidelines_wraps_unexpected_exceptions(tmp_path: Path) -> None:
    """Unexpected errors inside the setup workflow should be wrapped.

    This exercises the top-level defensive ``except Exception`` branch by
    registering a ToolSpec whose root path is invalid (uses ".."), which
    causes _project_path to raise ValueError under the main try block.
    """

    # Snapshot and extend TOOL_MAP with a deliberately invalid entry.
    original_spec = TOOL_MAP.get("broken")
    TOOL_MAP["broken"] = ToolSpec(primary_link="AGENTS.md", root="../invalid-root")
    try:
        result = run_setup_ai_guidelines("broken", project_dir=tmp_path, dry_run=False)
    finally:
        # Restore previous value to avoid leaking state across tests.
        if original_spec is None:
            TOOL_MAP.pop("broken", None)
        else:
            TOOL_MAP["broken"] = original_spec

    assert isinstance(result, SetupResult)
    assert result.success is False
    assert result.error is not None
    assert "Failed to setup AI guidelines" in result.error
    assert any("Failed to setup AI guidelines" in line for line in result.logs)


def test_run_setup_ai_guidelines_rejects_absolute_paths(tmp_path: Path) -> None:
    """Absolute ToolSpec roots should be rejected with a wrapped error."""

    original_spec = TOOL_MAP.get("absolute")
    TOOL_MAP["absolute"] = ToolSpec(
        primary_link="AGENTS.md",
        root=str((tmp_path / "abs-root").resolve()),
    )
    try:
        result = run_setup_ai_guidelines(
            "absolute", project_dir=tmp_path, dry_run=False
        )
    finally:
        if original_spec is None:
            TOOL_MAP.pop("absolute", None)
        else:
            TOOL_MAP["absolute"] = original_spec

    assert isinstance(result, SetupResult)
    assert result.success is False
    assert result.error is not None
    assert "ToolSpec paths must be project-relative" in result.error


def test_run_setup_ai_guidelines_unknown_tool_errors(tmp_path: Path) -> None:
    """Unknown tool keys should return a structured error."""

    result = run_setup_ai_guidelines(
        "unknown-tool", project_dir=tmp_path, dry_run=False
    )

    assert isinstance(result, SetupResult)
    assert result.success is False
    assert result.error is not None
    assert "Unknown tool" in result.error
    assert any("ERROR" in line for line in result.logs)


def test_run_setup_ai_guidelines_single_file_root_symlink(tmp_path: Path) -> None:
    """Single-file tooling should create a symlink that points at README."""

    result = run_setup_ai_guidelines("codex", project_dir=tmp_path, dry_run=False)

    assert result.success is True

    readme = tmp_path / ".dev-guidelines" / "README.md"
    primary_path = tmp_path / "AGENTS.md"
    assert primary_path.is_symlink()
    assert primary_path.resolve() == readme.resolve()
    assert any("link   " in line for line in result.logs)


def test_run_setup_ai_guidelines_is_idempotent_for_matching_link(
    tmp_path: Path,
) -> None:
    """Running the setup twice should detect existing matching symlinks."""

    first = run_setup_ai_guidelines("codex", project_dir=tmp_path, dry_run=False)
    assert first.success is True

    second = run_setup_ai_guidelines("codex", project_dir=tmp_path, dry_run=False)
    assert second.success is True
    assert any("ok     " in line for line in second.logs)


def test_run_setup_ai_guidelines_replaces_existing_file(tmp_path: Path) -> None:
    """Existing regular files at the primary path should be replaced."""

    primary_path = tmp_path / "AGENTS.md"
    primary_path.write_text("stale")

    result = run_setup_ai_guidelines("codex", project_dir=tmp_path, dry_run=False)

    assert result.success is True
    assert primary_path.is_symlink()


def test_run_setup_ai_guidelines_replaces_empty_directory(tmp_path: Path) -> None:
    """Empty directories should be removed before linking."""

    primary_path = tmp_path / "AGENTS.md"
    primary_path.mkdir()

    result = run_setup_ai_guidelines("codex", project_dir=tmp_path, dry_run=False)

    assert result.success is True
    assert primary_path.is_symlink()


def test_run_setup_ai_guidelines_dry_run_reports_git_and_ai_ignore(
    tmp_path: Path,
) -> None:
    """Dry-run logging should surface gitignore and aiignore states."""

    project_dir = tmp_path
    (project_dir / ".gitignore").write_text(".github/\n")
    (project_dir / ".aiignore").write_text(".github/\n")

    base_dir = project_dir / ".dev-guidelines"
    base_dir.mkdir()
    (base_dir / "README.md").write_text("notes")
    (base_dir / "policies.md").write_text("placeholder")

    result = run_setup_ai_guidelines("copilot", project_dir=project_dir, dry_run=True)

    assert result.success is True
    assert any("[dry-run]" in line for line in result.logs)
    assert any("git    '.github/' ignored" in line for line in result.logs)
    assert any("WARNING: ai     '.github/' is excluded" in line for line in result.logs)


def test_run_setup_ai_guidelines_handles_exists_oserror(tmp_path: Path) -> None:
    """Path.exists raising OSError should be tolerated during setup."""

    base_dir = tmp_path / ".dev-guidelines"
    readme = base_dir / "README.md"
    original_exists = ai_guidelines.Path.exists

    def flaky_exists(self: Path, *, follow_symlinks: bool = True) -> bool:
        if self in {base_dir, readme}:
            raise OSError("fs failure")
        return original_exists(self, follow_symlinks=follow_symlinks)

    ai_guidelines.Path.exists = flaky_exists  # type: ignore[assignment]
    try:
        result = run_setup_ai_guidelines("codex", project_dir=tmp_path, dry_run=True)
    finally:
        ai_guidelines.Path.exists = original_exists  # type: ignore[assignment]

    assert result.success is True
    assert any("[dry-run] mkdir -p" in line for line in result.logs)
    assert any("[dry-run] touch" in line for line in result.logs)


def test_run_setup_ai_guidelines_windows_junction_failure(tmp_path: Path) -> None:
    """Windows junction creation failures should be surfaced as errors."""

    base_dir = tmp_path / ".dev-guidelines"
    readme_dir = base_dir / "README.md"
    readme_dir.mkdir(parents=True)
    github_dir = tmp_path / ".github"
    github_dir.mkdir()

    original_os = ai_guidelines.os
    fake_os = SimpleNamespace(
        name="nt",
        path=os.path,
        sep=os.sep,
        link=os.link,
    )

    original_subprocess_run = ai_guidelines.subprocess.run

    def fake_run(cmd: list[str], *, capture_output: bool, text: bool):  # type: ignore[override]
        return SimpleNamespace(returncode=1, stderr="junction failed")

    ai_guidelines.os = fake_os  # type: ignore[assignment]
    ai_guidelines.subprocess.run = fake_run  # type: ignore[assignment]
    try:
        result = run_setup_ai_guidelines("copilot", project_dir=tmp_path, dry_run=False)
    finally:
        ai_guidelines.os = original_os  # type: ignore[assignment]
        ai_guidelines.subprocess.run = original_subprocess_run  # type: ignore[assignment]

    assert result.success is False
    assert result.error is not None
    assert "failed to create junction" in result.error


def test_run_setup_ai_guidelines_non_empty_directory_rejected(tmp_path: Path) -> None:
    """Non-empty directories at primary path should raise a structured error."""

    base_dir = tmp_path / ".dev-guidelines"
    readme = base_dir / "README.md"
    base_dir.mkdir()
    readme.touch()

    primary_path = tmp_path / "AGENTS.md"
    primary_path.mkdir()
    (primary_path / "stale.txt").write_text("data", encoding="utf-8")

    result = run_setup_ai_guidelines("codex", project_dir=tmp_path, dry_run=False)

    assert result.success is False
    assert result.error is not None
    assert "Cannot replace non-empty directory" in result.error


def test_run_setup_ai_guidelines_symlink_noop(tmp_path: Path) -> None:
    """Existing symlink to same target should be recreated if representation differs."""

    project_dir = tmp_path
    base_dir = project_dir / ".dev-guidelines"
    readme = base_dir / "README.md"
    base_dir.mkdir()
    readme.write_text("notes", encoding="utf-8")

    tool_dir = project_dir / ".github"
    tool_dir.mkdir()
    primary_path = project_dir / ".github" / "copilot-instructions.md"
    primary_path.symlink_to(readme)

    result = run_setup_ai_guidelines("copilot", project_dir=project_dir, dry_run=False)

    assert result.success is True
    assert primary_path.is_symlink()
    assert any("link   " in line for line in result.logs)


def test_run_setup_ai_guidelines_removes_broken_symlink(tmp_path: Path) -> None:
    """Broken symlinks under tool_dir pointing into base should be cleaned."""

    project_dir = tmp_path
    base_dir = project_dir / ".dev-guidelines"
    readme = base_dir / "README.md"
    base_dir.mkdir()
    readme.write_text("notes", encoding="utf-8")

    tool_dir = project_dir / ".github"
    tool_dir.mkdir()
    broken = tool_dir / "old.md"
    broken.symlink_to(base_dir / "missing.md")

    result = run_setup_ai_guidelines("copilot", project_dir=project_dir, dry_run=False)

    assert result.success is True
    assert not broken.exists()
    assert any("clean  removed broken symlink" in line for line in result.logs)


def test_run_setup_ai_guidelines_windows_hardlink_success(tmp_path: Path) -> None:
    """Windows hardlink creation path should be exercised when target is file."""

    project_dir = tmp_path
    base_dir = project_dir / ".dev-guidelines"
    base_dir.mkdir()
    readme = base_dir / "README.md"
    readme.write_text("notes", encoding="utf-8")
    subdir = base_dir / "policies"
    subdir.mkdir()
    (subdir / "guide.md").write_text("text", encoding="utf-8")

    links: list[tuple[Path, Path]] = []

    def fake_link(src: Path, dst: Path) -> None:
        links.append((src, dst))

    original_os = ai_guidelines.os
    original_run = ai_guidelines.subprocess.run
    ai_guidelines.subprocess.run = lambda *args, **kwargs: SimpleNamespace(
        returncode=0, stderr=""
    )  # type: ignore[assignment]
    ai_guidelines.os = SimpleNamespace(
        name="nt", path=os.path, link=fake_link, sep=os.sep
    )  # type: ignore[assignment]
    try:
        result = run_setup_ai_guidelines(
            "codex", project_dir=project_dir, dry_run=False
        )
    finally:
        ai_guidelines.os = original_os  # type: ignore[assignment]
        ai_guidelines.subprocess.run = original_run  # type: ignore[assignment]

    primary_path = project_dir / "AGENTS.md"
    assert result.success is True
    assert links and links[0][0] == readme and links[0][1] == primary_path


def test_run_setup_ai_guidelines_gitignore_warns_when_not_ignored(
    tmp_path: Path,
) -> None:
    """log_gitignore_status should warn when paths are not ignored."""

    project_dir = tmp_path
    base_dir = project_dir / ".dev-guidelines"
    base_dir.mkdir()
    (base_dir / "README.md").write_text("notes", encoding="utf-8")
    (base_dir / "policies.md").write_text("placeholder", encoding="utf-8")

    (project_dir / ".gitignore").write_text("node_modules/\n", encoding="utf-8")
    (project_dir / ".aiignore").write_text("", encoding="utf-8")

    result = run_setup_ai_guidelines("copilot", project_dir=project_dir, dry_run=True)

    assert result.success is True
    assert any("not ignored by .gitignore" in line for line in result.logs)
    assert any("accessible to AI tools" in line for line in result.logs)


def test_run_setup_ai_guidelines_gitignore_negated_pattern(tmp_path: Path) -> None:
    """Negated gitignore patterns should be reported as kept."""

    project_dir = tmp_path
    base_dir = project_dir / ".dev-guidelines"
    base_dir.mkdir()
    (base_dir / "README.md").write_text("notes", encoding="utf-8")
    (base_dir / "policies.md").write_text("placeholder", encoding="utf-8")

    gitignore_content = ".github/\n!.github/\n"
    (project_dir / ".gitignore").write_text(gitignore_content, encoding="utf-8")
    (project_dir / ".aiignore").write_text("", encoding="utf-8")

    result = run_setup_ai_guidelines("copilot", project_dir=project_dir, dry_run=True)

    assert result.success is True
    assert any("kept by negated pattern" in line for line in result.logs)


def test_run_setup_ai_guidelines_aiignore_warns(tmp_path: Path) -> None:
    """Entries in .aiignore should warn as inaccessible."""

    project_dir = tmp_path
    base_dir = project_dir / ".dev-guidelines"
    base_dir.mkdir()
    (base_dir / "README.md").write_text("notes", encoding="utf-8")
    (base_dir / "policies.md").write_text("placeholder", encoding="utf-8")

    (project_dir / ".gitignore").write_text("", encoding="utf-8")
    (project_dir / ".aiignore").write_text(".github/\n", encoding="utf-8")

    result = run_setup_ai_guidelines("copilot", project_dir=project_dir, dry_run=True)

    assert result.success is True
    assert any("excluded by .aiignore" in line for line in result.logs)


def test_run_setup_ai_guidelines_gitignore_invalid_pattern_skips(
    tmp_path: Path,
) -> None:
    """Invalid gitignore patterns should be ignored gracefully."""

    project_dir = tmp_path
    base_dir = project_dir / ".dev-guidelines"
    base_dir.mkdir()
    (base_dir / "README.md").write_text("notes", encoding="utf-8")

    (project_dir / ".gitignore").write_text("[[\n", encoding="utf-8")
    (project_dir / ".aiignore").write_text("", encoding="utf-8")

    result = run_setup_ai_guidelines("copilot", project_dir=project_dir, dry_run=True)

    assert result.success is True
    assert any("not ignored by .gitignore" in line for line in result.logs)


def test_create_or_update_link_relpath_value_error_fallback(tmp_path: Path) -> None:
    """Non-relative paths should fall back to absolute target representation."""

    project_dir = tmp_path
    base_dir = project_dir / ".dev-guidelines"
    base_dir.mkdir()
    readme = base_dir / "README.md"
    readme.write_text("notes", encoding="utf-8")

    original_relpath = ai_guidelines.os.path.relpath

    def selective_relpath(
        path: str | os.PathLike[str], start: str | os.PathLike[str]
    ) -> str:
        if Path(path).name == "README.md":
            raise ValueError("no relpath")
        return original_relpath(path, start)  # type: ignore[arg-type]

    ai_guidelines.os.path.relpath = selective_relpath  # type: ignore[assignment]
    try:
        result = run_setup_ai_guidelines(
            "copilot", project_dir=project_dir, dry_run=True
        )
    finally:
        ai_guidelines.os.path.relpath = original_relpath  # type: ignore[assignment]

    assert result.success is True


def test_create_or_update_link_readlink_samefile_fallback(tmp_path: Path) -> None:
    """samefile OSError should fall back to resolve comparison even when readlink fails."""

    project_dir = tmp_path
    base_dir = project_dir / ".dev-guidelines"
    base_dir.mkdir()
    readme = base_dir / "README.md"
    readme.write_text("notes", encoding="utf-8")

    tool_dir = project_dir / ".github"
    tool_dir.mkdir()
    primary_path = tool_dir / "copilot-instructions.md"
    primary_path.symlink_to(readme)

    original_samefile = ai_guidelines.os.path.samefile
    original_readlink = ai_guidelines.Path.readlink

    def failing_samefile(_a: Path, _b: Path) -> bool:
        raise OSError("boom")

    def failing_readlink(self: Path) -> Path:
        raise OSError("nope")

    ai_guidelines.os.path.samefile = failing_samefile  # type: ignore[assignment]
    ai_guidelines.Path.readlink = failing_readlink  # type: ignore[assignment]
    try:
        result = run_setup_ai_guidelines(
            "copilot", project_dir=project_dir, dry_run=False
        )
    finally:
        ai_guidelines.os.path.samefile = original_samefile  # type: ignore[assignment]
        ai_guidelines.Path.readlink = original_readlink  # type: ignore[assignment]

    assert result.success is True


def test_run_setup_ai_guidelines_windows_hardlink_error(tmp_path: Path) -> None:
    """Hardlink failures on Windows should surface a runtime error."""

    project_dir = tmp_path
    base_dir = project_dir / ".dev-guidelines"
    base_dir.mkdir()
    readme = base_dir / "README.md"
    readme.write_text("notes", encoding="utf-8")

    def failing_link(_src: Path, _dst: Path) -> None:
        raise OSError("disk mismatch")

    original_os = ai_guidelines.os
    ai_guidelines.os = SimpleNamespace(
        name="nt", path=os.path, link=failing_link, sep=os.sep
    )  # type: ignore[assignment]
    try:
        result = run_setup_ai_guidelines(
            "codex", project_dir=project_dir, dry_run=False
        )
    finally:
        ai_guidelines.os = original_os  # type: ignore[assignment]

    assert result.success is False
    assert "failed to create hardlink" in (result.error or "")


def test_ensure_dir_noop_when_exists(tmp_path: Path) -> None:
    """ensure_dir should short-circuit when path exists and not log."""

    project_dir = tmp_path
    base_dir = project_dir / ".dev-guidelines"
    base_dir.mkdir()
    (base_dir / "README.md").write_text("notes", encoding="utf-8")

    existing = base_dir / "README.md"
    result = run_setup_ai_guidelines("codex", project_dir=project_dir, dry_run=False)

    assert result.success is True
    # ensure_dir should not log touch when file already exists
    assert all("[dry-run] touch" not in line for line in result.logs)
    assert existing.exists()


def test_ensure_dir_handles_os_error(tmp_path: Path) -> None:
    """ensure_dir should ignore exists() OSError and proceed to create."""

    project_dir = tmp_path
    base_dir = project_dir / ".dev-guidelines"
    target = base_dir / "README.md"

    original_exists = ai_guidelines.Path.exists
    seen: dict[str, bool] = {"raised": False}

    def exploding_exists(self: Path) -> bool:
        if self == target and not seen["raised"]:
            seen["raised"] = True
            raise OSError("blocked")
        return original_exists(self)

    ai_guidelines.Path.exists = exploding_exists  # type: ignore[assignment]
    try:
        result = run_setup_ai_guidelines(
            "codex", project_dir=project_dir, dry_run=False
        )
    finally:
        ai_guidelines.Path.exists = original_exists  # type: ignore[assignment]

    assert result.success is True
    assert target.exists()


def test_create_or_update_link_symlink_failure_logs_error(tmp_path: Path) -> None:
    """Symlink creation failure should emit error log."""

    project_dir = tmp_path
    base_dir = project_dir / ".dev-guidelines"
    base_dir.mkdir()
    readme = base_dir / "README.md"
    readme.write_text("notes", encoding="utf-8")

    tool_dir = project_dir / ".github"
    tool_dir.mkdir()

    original_symlink = ai_guidelines.Path.symlink_to

    def failing_symlink(
        self: Path, target: Path, target_is_directory: bool = False
    ) -> None:  # type: ignore[override]
        raise OSError("denied")

    ai_guidelines.Path.symlink_to = failing_symlink  # type: ignore[assignment]
    try:
        result = run_setup_ai_guidelines(
            "copilot", project_dir=project_dir, dry_run=False
        )
    finally:
        ai_guidelines.Path.symlink_to = original_symlink  # type: ignore[assignment]

    assert result.success is True
    assert any("failed to create symlink" in line for line in result.logs)


def test_windows_junction_success_and_failure(tmp_path: Path) -> None:
    """Exercise Windows junction success and failure paths."""

    project_dir = tmp_path
    base_dir = project_dir / ".dev-guidelines"
    base_dir.mkdir()
    readme = base_dir / "README.md"
    readme.mkdir()
    (readme / "index.md").write_text("notes", encoding="utf-8")

    junction_calls: list[tuple[Path, Path]] = []

    def fake_run(cmd: list[str], capture_output: bool, text: bool) -> SimpleNamespace:
        del capture_output, text
        junction_calls.append((Path(cmd[-2]), Path(cmd[-1])))
        return SimpleNamespace(returncode=0, stderr="")

    original_os = ai_guidelines.os
    original_run = ai_guidelines.subprocess.run
    ai_guidelines.os = SimpleNamespace(
        name="nt", path=os.path, link=os.link, sep=os.sep
    )  # type: ignore[assignment]
    ai_guidelines.subprocess.run = fake_run  # type: ignore[assignment]
    try:
        result = run_setup_ai_guidelines(
            "aiassistant", project_dir=project_dir, dry_run=False
        )
    finally:
        ai_guidelines.os = original_os  # type: ignore[assignment]
        ai_guidelines.subprocess.run = original_run  # type: ignore[assignment]

    assert result.success is True
    assert junction_calls

    def failing_run(
        cmd: list[str], capture_output: bool, text: bool
    ) -> SimpleNamespace:
        del capture_output, text
        return SimpleNamespace(returncode=1, stderr="oops")

    ai_guidelines.os = SimpleNamespace(
        name="nt", path=os.path, link=os.link, sep=os.sep
    )  # type: ignore[assignment]
    ai_guidelines.subprocess.run = failing_run  # type: ignore[assignment]
    try:
        result = run_setup_ai_guidelines(
            "aiassistant", project_dir=project_dir, dry_run=False
        )
    finally:
        ai_guidelines.os = original_os  # type: ignore[assignment]
        ai_guidelines.subprocess.run = original_run  # type: ignore[assignment]

    assert result.success is False
    assert "failed to create junction" in (result.error or "")


def test_create_or_update_link_handles_exists_os_error(tmp_path: Path) -> None:
    project_dir = tmp_path
    base_dir = project_dir / ".dev-guidelines"
    base_dir.mkdir()
    readme = base_dir / "README.md"
    readme.write_text("notes", encoding="utf-8")

    tool_dir = project_dir / ".github"
    tool_dir.mkdir()

    primary_path = tool_dir / "copilot-instructions.md"
    primary_path.write_text("old", encoding="utf-8")

    original_exists = ai_guidelines.Path.exists

    def failing_exists(self: Path) -> bool:
        if self == primary_path:
            raise OSError("boom")
        return original_exists(self)

    ai_guidelines.Path.exists = failing_exists  # type: ignore[assignment]
    try:
        result = run_setup_ai_guidelines(
            "copilot", project_dir=project_dir, dry_run=False
        )
    finally:
        ai_guidelines.Path.exists = original_exists  # type: ignore[assignment]

    assert result.success is True


def test_create_or_update_link_handles_target_is_dir_os_error(tmp_path: Path) -> None:
    project_dir = tmp_path
    base_dir = project_dir / ".dev-guidelines"
    base_dir.mkdir()
    readme = base_dir / "README.md"
    readme.write_text("notes", encoding="utf-8")

    tool_dir = project_dir / ".github"
    tool_dir.mkdir()

    original_is_dir = ai_guidelines.Path.is_dir

    def failing_is_dir(self: Path) -> bool:
        if self == readme:
            raise OSError("boom")
        return original_is_dir(self)

    ai_guidelines.Path.is_dir = failing_is_dir  # type: ignore[assignment]
    try:
        result = run_setup_ai_guidelines(
            "copilot", project_dir=project_dir, dry_run=True
        )
    finally:
        ai_guidelines.Path.is_dir = original_is_dir  # type: ignore[assignment]

    assert result.success is True


def test_create_or_update_link_resolve_os_error_fallback(tmp_path: Path) -> None:
    project_dir = tmp_path
    base_dir = project_dir / ".dev-guidelines"
    base_dir.mkdir()
    readme = base_dir / "README.md"
    readme.write_text("notes", encoding="utf-8")

    tool_dir = project_dir / ".github"
    tool_dir.mkdir()
    primary_path = tool_dir / "copilot-instructions.md"
    primary_path.symlink_to(readme)

    original_samefile = ai_guidelines.os.path.samefile
    original_resolve = ai_guidelines.Path.resolve

    def failing_samefile(_a: Path, _b: Path) -> bool:
        raise OSError("boom")

    seen: dict[str, bool] = {"raised": False}

    def failing_resolve(self: Path, strict: bool = False) -> Path:
        del strict
        if self == primary_path and not seen["raised"]:
            seen["raised"] = True
            raise OSError("boom")
        return original_resolve(self)

    ai_guidelines.os.path.samefile = failing_samefile  # type: ignore[assignment]
    ai_guidelines.Path.resolve = failing_resolve  # type: ignore[assignment]
    try:
        result = run_setup_ai_guidelines(
            "copilot", project_dir=project_dir, dry_run=False
        )
    finally:
        ai_guidelines.os.path.samefile = original_samefile  # type: ignore[assignment]
        ai_guidelines.Path.resolve = original_resolve  # type: ignore[assignment]

    assert result.success is True


def test_create_or_update_link_dry_run_removes_existing(tmp_path: Path) -> None:
    project_dir = tmp_path
    base_dir = project_dir / ".dev-guidelines"
    base_dir.mkdir()
    readme = base_dir / "README.md"
    readme.write_text("notes", encoding="utf-8")

    tool_dir = project_dir / ".github"
    tool_dir.mkdir()
    primary_path = tool_dir / "copilot-instructions.md"
    primary_path.write_text("old", encoding="utf-8")

    result = run_setup_ai_guidelines("copilot", project_dir=project_dir, dry_run=True)

    assert result.success is True
    assert any("[dry-run] rm" in line for line in result.logs)


def test_create_or_update_link_unlink_missing_ok_branch(tmp_path: Path) -> None:
    project_dir = tmp_path
    base_dir = project_dir / ".dev-guidelines"
    base_dir.mkdir()
    readme = base_dir / "README.md"
    readme.write_text("notes", encoding="utf-8")

    tool_dir = project_dir / ".github"
    tool_dir.mkdir()
    primary_path = tool_dir / "copilot-instructions.md"
    primary_path.write_text("old", encoding="utf-8")

    original_is_symlink = ai_guidelines.Path.is_symlink
    original_is_file = ai_guidelines.Path.is_file
    original_is_dir = ai_guidelines.Path.is_dir
    original_unlink = ai_guidelines.Path.unlink

    unlink_calls: list[bool] = []

    def always_false(self: Path) -> bool:
        return False

    def recording_unlink(self: Path, missing_ok: bool = False) -> None:
        if self == primary_path:
            unlink_calls.append(missing_ok)
        return original_unlink(self)

    ai_guidelines.Path.is_symlink = always_false  # type: ignore[assignment]
    ai_guidelines.Path.is_file = always_false  # type: ignore[assignment]
    ai_guidelines.Path.is_dir = always_false  # type: ignore[assignment]
    ai_guidelines.Path.unlink = recording_unlink  # type: ignore[assignment]
    try:
        result = run_setup_ai_guidelines(
            "copilot", project_dir=project_dir, dry_run=False
        )
    finally:
        ai_guidelines.Path.is_symlink = original_is_symlink  # type: ignore[assignment]
        ai_guidelines.Path.is_file = original_is_file  # type: ignore[assignment]
        ai_guidelines.Path.is_dir = original_is_dir  # type: ignore[assignment]
        ai_guidelines.Path.unlink = original_unlink  # type: ignore[assignment]

    assert result.success is True
    assert unlink_calls == [True]


def test_create_or_update_link_remove_existing_raises_runtime_error(
    tmp_path: Path,
) -> None:
    project_dir = tmp_path
    base_dir = project_dir / ".dev-guidelines"
    base_dir.mkdir()
    readme = base_dir / "README.md"
    readme.write_text("notes", encoding="utf-8")

    tool_dir = project_dir / ".github"
    tool_dir.mkdir()
    primary_path = tool_dir / "copilot-instructions.md"
    primary_path.write_text("old", encoding="utf-8")

    original_unlink = ai_guidelines.Path.unlink

    def failing_unlink(self: Path, missing_ok: bool = False) -> None:
        del missing_ok
        if self == primary_path:
            raise OSError("boom")
        return original_unlink(self)

    ai_guidelines.Path.unlink = failing_unlink  # type: ignore[assignment]
    try:
        result = run_setup_ai_guidelines(
            "copilot", project_dir=project_dir, dry_run=False
        )
    finally:
        ai_guidelines.Path.unlink = original_unlink  # type: ignore[assignment]

    assert result.success is False
    assert "failed to remove existing path" in (result.error or "")


def test_create_or_update_link_dry_run_windows_logs(tmp_path: Path) -> None:
    project_dir = tmp_path
    base_dir = project_dir / ".dev-guidelines"
    base_dir.mkdir()
    readme = base_dir / "README.md"
    readme.write_text("notes", encoding="utf-8")

    original_os = ai_guidelines.os
    ai_guidelines.os = SimpleNamespace(
        name="nt", path=os.path, link=os.link, sep=os.sep
    )  # type: ignore[assignment]
    try:
        result = run_setup_ai_guidelines("codex", project_dir=project_dir, dry_run=True)
    finally:
        ai_guidelines.os = original_os  # type: ignore[assignment]

    assert result.success is True
    assert any("[dry-run] hardlink" in line for line in result.logs)


def test_create_or_update_link_dry_run_windows_junction_logs(tmp_path: Path) -> None:
    project_dir = tmp_path
    base_dir = project_dir / ".dev-guidelines"
    base_dir.mkdir()
    (base_dir / "README.md").write_text("notes", encoding="utf-8")
    subdir = base_dir / "policies"
    subdir.mkdir()
    (subdir / "index.md").write_text("notes", encoding="utf-8")

    original_os = ai_guidelines.os
    ai_guidelines.os = SimpleNamespace(
        name="nt", path=os.path, link=os.link, sep=os.sep
    )  # type: ignore[assignment]
    try:
        result = run_setup_ai_guidelines(
            "aiassistant", project_dir=project_dir, dry_run=True
        )
    finally:
        ai_guidelines.os = original_os  # type: ignore[assignment]

    assert result.success is True
    assert any("[dry-run] junction" in line for line in result.logs)


def test_project_path_dot_gitignore_candidates_root(tmp_path: Path) -> None:
    project_dir = tmp_path
    base_dir = project_dir / ".dev-guidelines"
    base_dir.mkdir()
    (base_dir / "README.md").write_text("notes", encoding="utf-8")

    original_spec = TOOL_MAP.get("dot-root")
    TOOL_MAP["dot-root"] = ToolSpec(
        primary_link=".github/copilot-instructions.md", root="."
    )
    (project_dir / ".gitignore").write_text("/\n", encoding="utf-8")
    (project_dir / ".aiignore").write_text("", encoding="utf-8")
    try:
        result = run_setup_ai_guidelines(
            "dot-root", project_dir=project_dir, dry_run=True
        )
    finally:
        if original_spec is None:
            TOOL_MAP.pop("dot-root", None)
        else:
            TOOL_MAP["dot-root"] = original_spec

    assert result.success is True


def test_broken_symlink_dry_run_log_and_non_symlink_ignored(tmp_path: Path) -> None:
    project_dir = tmp_path
    base_dir = project_dir / ".dev-guidelines"
    base_dir.mkdir()
    (base_dir / "README.md").write_text("notes", encoding="utf-8")
    (base_dir / "policies.md").write_text("placeholder", encoding="utf-8")

    tool_dir = project_dir / ".github"
    tool_dir.mkdir()

    broken = tool_dir / "old.md"
    broken.symlink_to(base_dir / "missing.md")
    (tool_dir / "real.txt").write_text("x", encoding="utf-8")

    result = run_setup_ai_guidelines("copilot", project_dir=project_dir, dry_run=True)

    assert result.success is True
    assert any("[dry-run] rm broken symlink" in line for line in result.logs)


def test_ensure_dir_creates_missing_readme_file_and_logs(tmp_path: Path) -> None:
    project_dir = tmp_path
    base_dir = project_dir / ".dev-guidelines"
    base_dir.mkdir()

    result = run_setup_ai_guidelines("codex", project_dir=project_dir, dry_run=False)

    readme = base_dir / "README.md"
    assert result.success is True
    assert readme.exists()
    assert any("create" in line and "README.md" in line for line in result.logs)


def test_symlink_to_directory_branch(tmp_path: Path) -> None:
    project_dir = tmp_path
    base_dir = project_dir / ".dev-guidelines"
    base_dir.mkdir()
    (base_dir / "README.md").write_text("notes", encoding="utf-8")
    subdir = base_dir / "policies"
    subdir.mkdir()
    (subdir / "index.md").write_text("notes", encoding="utf-8")

    # create a non-single-file tool root so mirror_tree runs
    tool_dir = project_dir / ".github"
    tool_dir.mkdir()

    result = run_setup_ai_guidelines("copilot", project_dir=project_dir, dry_run=False)

    linked = tool_dir / "policies"
    assert result.success is True
    assert linked.is_symlink()


def test_gitignore_directory_candidates_root_path(tmp_path: Path) -> None:
    project_dir = tmp_path
    base_dir = project_dir / ".dev-guidelines"
    base_dir.mkdir()
    (base_dir / "README.md").write_text("notes", encoding="utf-8")
    (base_dir / "policies.md").write_text("x", encoding="utf-8")

    # ensure we hit the directory candidate root '/' branch; behavior is warn/keep
    (project_dir / ".gitignore").write_text("!.github/\n", encoding="utf-8")
    (project_dir / ".aiignore").write_text("", encoding="utf-8")

    original_spec = TOOL_MAP.get("dot-root")
    TOOL_MAP["dot-root"] = ToolSpec(
        primary_link=".github/copilot-instructions.md", root="."
    )
    try:
        result = run_setup_ai_guidelines(
            "dot-root", project_dir=project_dir, dry_run=True
        )
    finally:
        if original_spec is None:
            TOOL_MAP.pop("dot-root", None)
        else:
            TOOL_MAP["dot-root"] = original_spec

    assert result.success is True
    assert any(
        ("kept by negated pattern" in line or "not ignored by .gitignore" in line)
        for line in result.logs
    )


def test_gitignore_value_error_pattern_is_skipped(tmp_path: Path) -> None:
    project_dir = tmp_path
    base_dir = project_dir / ".dev-guidelines"
    base_dir.mkdir()
    (base_dir / "README.md").write_text("notes", encoding="utf-8")
    (base_dir / "policies.md").write_text("x", encoding="utf-8")

    # NUL byte should make GitWildMatchPattern raise ValueError and be skipped
    (project_dir / ".gitignore").write_bytes(b"\x00\n")
    (project_dir / ".aiignore").write_text("", encoding="utf-8")

    result = run_setup_ai_guidelines("copilot", project_dir=project_dir, dry_run=True)

    assert result.success is True
