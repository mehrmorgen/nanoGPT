from __future__ import annotations

from pathlib import Path
import time

from ml_playground.core.file_state import diff_file_states, snapshot_file_states


class _RaisingPath:
    def __init__(self, inner: Path) -> None:
        self._inner = inner

    def exists(self) -> bool:  # pragma: no cover - exercised through snapshot
        raise OSError("boom")

    def stat(self):  # pragma: no cover - exercised through snapshot
        raise AssertionError("stat should not be called after exists fails")

    def __hash__(self) -> int:
        return hash(self._inner)

    def __repr__(self) -> str:
        return f"_RaisingPath({self._inner!s})"


def test_snapshot_file_states_handles_oserror(tmp_path: Path) -> None:
    sentinel = _RaisingPath(tmp_path / "sentinel")
    snapshot = snapshot_file_states([sentinel])
    assert snapshot[sentinel] == (False, 0.0, 0)


def test_diff_file_states_tracks_created_updated_and_skipped(tmp_path: Path) -> None:
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

    assert created == {created_path}
    assert updated == {updated_path}
    assert skipped == {unchanged_path}
    assert removed_path not in created | updated | skipped
