from __future__ import annotations

from pathlib import Path
import os
import time
from typing import cast

from ml_playground.framework.core.file_state import (
    diff_file_states,
    snapshot_file_states,
)


class _RaisingPath(Path):
    _inner: Path

    def __new__(cls, inner: Path) -> "_RaisingPath":  # type: ignore[override]
        return Path.__new__(cls, str(inner))

    def __init__(self, inner: Path) -> None:
        object.__setattr__(self, "_inner", inner)

    def exists(
        self, *, follow_symlinks: bool = True
    ) -> bool:  # pragma: no cover - exercised through snapshot
        raise OSError("boom")

    def stat(
        self, *, follow_symlinks: bool = True
    ):  # pragma: no cover - exercised through snapshot
        _ = follow_symlinks
        raise AssertionError("stat should not be called after exists fails")

    def __repr__(self) -> str:
        return f"_RaisingPath({self._inner!s})"


def test_snapshot_file_states_handles_oserror(tmp_path: Path) -> None:
    """Test snapshot file states handles oserror."""
    underlying = tmp_path / "sentinel"
    sentinel = _RaisingPath(underlying)
    snapshot = snapshot_file_states([cast(Path, sentinel)])
    assert snapshot[str(underlying)] == (False, 0.0, 0)


def test_diff_file_states_tracks_created_updated_and_skipped(tmp_path: Path) -> None:
    """Test diff file states tracks created updated and skipped."""
    created_path = tmp_path / "created.bin"
    updated_path = tmp_path / "updated.bin"
    removed_path = tmp_path / "removed.bin"
    unchanged_path = tmp_path / "unchanged.bin"

    updated_path.write_bytes(b"old")
    removed_path.write_bytes(b"gone")
    unchanged_path.write_bytes(b"still-here")

    before = snapshot_file_states(
        [created_path, updated_path, removed_path, unchanged_path]
    )

    time.sleep(0.01)
    updated_path.write_bytes(b"new")
    removed_path.unlink()
    created_path.write_bytes(b"created")

    created, updated, skipped = diff_file_states(
        [created_path, updated_path, removed_path, unchanged_path], before
    )

    assert created == {str(created_path)}
    assert updated == {str(updated_path)}
    assert skipped == {str(unchanged_path)}
    assert str(removed_path) not in created | updated | skipped


def test_snapshot_file_states_handles_disappearing_path(tmp_path: Path) -> None:
    """Test snapshot file states handles disappearing path."""
    base = tmp_path / "vanish"

    class VanishingPath(type(base)):  # type: ignore[misc]
        _inner: Path
        _calls: int

        def __new__(cls, inner: Path) -> "VanishingPath":  # type: ignore[override]
            self = Path.__new__(cls, str(inner))
            object.__setattr__(self, "_inner", inner)
            object.__setattr__(self, "_calls", 0)
            # Initialize a dummy stat result for the new stat method
            object.__setattr__(
                self, "_stat_result", os.stat_result((0, 0, 0, 0, 0, 0, 0, 0, 0, 0))
            )
            return self

        def exists(self, *, follow_symlinks: bool = True) -> bool:  # type: ignore[override]
            object.__setattr__(self, "_calls", self._calls + 1)
            return self._calls == 1

        def stat(self, *, follow_symlinks: bool = True) -> os.stat_result:
            _ = follow_symlinks
            raise OSError("stat failed")

    ghost = VanishingPath(base)
    states = snapshot_file_states([ghost])
    assert states[str(ghost)] == (False, 0.0, 0)


def test_diff_file_states_detects_updated_mtime(tmp_path: Path) -> None:
    """diff_file_states should detect files with changed mtime."""
    import time

    path = tmp_path / "file.txt"
    path.write_text("content")

    before = snapshot_file_states([path])

    # Wait and modify file to change mtime
    time.sleep(0.01)
    path.write_text("new content")

    created, updated, skipped = diff_file_states([path], before)

    assert str(path) in updated
    assert str(path) not in created
    assert str(path) not in skipped


def test_diff_file_states_detects_updated_size(tmp_path: Path) -> None:
    """diff_file_states should detect files with changed size."""
    import time

    path = tmp_path / "file.txt"
    path.write_text("x")

    before = snapshot_file_states([path])

    # Wait and modify file to change size
    time.sleep(0.01)
    path.write_text("much longer content")

    created, updated, skipped = diff_file_states([path], before)

    assert str(path) in updated
    assert str(path) not in created
    assert str(path) not in skipped


def test_diff_file_states_detects_deleted_file(tmp_path: Path) -> None:
    """diff_file_states should detect deleted files."""
    path = tmp_path / "file.txt"
    path.write_text("content")

    before = snapshot_file_states([path])

    path.unlink()

    created, updated, skipped = diff_file_states([path], before)

    assert str(path) not in created
    assert str(path) not in updated
    assert str(path) not in skipped


def test_diff_file_states_unchanged_file(tmp_path: Path) -> None:
    """diff_file_states should detect unchanged files."""
    path = tmp_path / "file.txt"
    path.write_text("content")

    before = snapshot_file_states([path])

    # Don't modify, just check again
    created, updated, skipped = diff_file_states([path], before)

    assert str(path) not in created
    assert str(path) not in updated
    assert str(path) in skipped
