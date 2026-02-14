"""Unit tests for file_state.py using only public APIs."""

from __future__ import annotations

from pathlib import Path

from ml_playground.framework.core.file_state import (
    FileState,
    snapshot_file_states,
    diff_file_states,
)


def test_snapshot_file_states_basic(tmp_path: Path) -> None:
    """Test basic snapshot_file_states functionality."""
    # Create test files
    file1 = tmp_path / "file1.txt"
    file2 = tmp_path / "file2.txt"
    file1.write_text("content1")
    file2.write_text("content2")

    # Take snapshot
    snapshot = snapshot_file_states([file1, file2])

    # Check results
    assert len(snapshot) == 2
    assert str(file1) in snapshot
    assert str(file2) in snapshot

    # Both files should exist with non-zero size and mtime
    exists1, mtime1, size1 = snapshot[str(file1)]
    assert exists1 is True
    assert mtime1 > 0
    assert size1 > 0

    exists2, mtime2, size2 = snapshot[str(file2)]
    assert exists2 is True
    assert mtime2 > 0
    assert size2 > 0


def test_snapshot_file_states_nonexistent(tmp_path: Path) -> None:
    """Test snapshot_file_states with non-existent files."""
    file1 = tmp_path / "nonexistent1.txt"
    file2 = tmp_path / "nonexistent2.txt"

    # Take snapshot of non-existent files
    snapshot = snapshot_file_states([file1, file2])

    # Check results
    assert len(snapshot) == 2
    assert str(file1) in snapshot
    assert str(file2) in snapshot

    # Both files should not exist
    exists1, mtime1, size1 = snapshot[str(file1)]
    assert exists1 is False
    assert mtime1 == 0.0
    assert size1 == 0

    exists2, mtime2, size2 = snapshot[str(file2)]
    assert exists2 is False
    assert mtime2 == 0.0
    assert size2 == 0


def test_snapshot_file_states_mixed(tmp_path: Path) -> None:
    """Test snapshot_file_states with mix of existing and non-existent files."""
    file1 = tmp_path / "exists.txt"
    file2 = tmp_path / "nonexistent.txt"
    file1.write_text("content")

    # Take snapshot
    snapshot = snapshot_file_states([file1, file2])

    # Check results
    assert len(snapshot) == 2

    # Existing file
    exists1, mtime1, size1 = snapshot[str(file1)]
    assert exists1 is True
    assert mtime1 > 0
    assert size1 > 0

    # Non-existent file
    exists2, mtime2, size2 = snapshot[str(file2)]
    assert exists2 is False
    assert mtime2 == 0.0
    assert size2 == 0


def test_diff_file_states_created(tmp_path: Path) -> None:
    """Test diff_file_states with created files."""
    file1 = tmp_path / "file1.txt"
    file2 = tmp_path / "file2.txt"

    # Initial snapshot (empty)
    before: dict[str, FileState] = {}

    # Create files
    file1.write_text("content1")
    file2.write_text("content2")

    # Take after snapshot
    snapshot_file_states([file1, file2])

    # Diff
    created, updated, skipped = diff_file_states([file1, file2], before)

    assert str(file1) in created
    assert str(file2) in created
    assert len(updated) == 0
    assert len(skipped) == 0


def test_diff_file_states_updated(tmp_path: Path) -> None:
    """Test diff_file_states with updated files."""
    file1 = tmp_path / "file1.txt"
    file1.write_text("original")

    # Initial snapshot
    before = snapshot_file_states([file1])

    # Wait a bit to ensure different timestamp
    import time

    time.sleep(0.01)

    # Update file
    file1.write_text("updated")

    # Take after snapshot
    snapshot_file_states([file1])

    # Diff
    created, updated, skipped = diff_file_states([file1], before)

    assert len(created) == 0
    assert str(file1) in updated
    assert len(skipped) == 0


def test_diff_file_states_deleted(tmp_path: Path) -> None:
    """Test diff_file_states with deleted files."""
    file1 = tmp_path / "file1.txt"
    file1.write_text("content")

    # Initial snapshot
    before = snapshot_file_states([file1])

    # Delete file
    file1.unlink()

    # Diff
    created, updated, skipped = diff_file_states([file1], before)

    assert len(created) == 0
    assert len(updated) == 0
    assert len(skipped) == 0
    # Deleted files don't appear in any category


def test_diff_file_states_unchanged(tmp_path: Path) -> None:
    """Test diff_file_states with unchanged files."""
    file1 = tmp_path / "file1.txt"
    file1.write_text("content")

    # Initial snapshot
    before = snapshot_file_states([file1])

    # Take after snapshot without changes
    snapshot_file_states([file1])

    # Diff
    created, updated, skipped = diff_file_states([file1], before)

    assert len(created) == 0
    assert len(updated) == 0
    assert str(file1) in skipped


def test_file_state_type() -> None:
    """Test FileState type alias."""
    # FileState should be a tuple of (bool, float, int)
    state: FileState = (True, 123.456, 789)
    assert state[0] is True
    assert state[1] == 123.456
    assert state[2] == 789
