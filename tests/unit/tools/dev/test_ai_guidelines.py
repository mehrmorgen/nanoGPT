from __future__ import annotations

from pathlib import Path

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
