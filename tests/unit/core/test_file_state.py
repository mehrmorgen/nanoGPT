from __future__ import annotations

from pathlib import Path
import time
from typing import cast

from ml_playground.core.file_state import diff_file_states, snapshot_file_states


class _RaisingPath(Path):
    def __new__(cls, inner: Path) -> "_RaisingPath":  # type: ignore[override]
        return Path.__new__(cls, str(inner))

    def __init__(self, inner: Path) -> None:
        self._inner = inner

    def exists(
        self, *, follow_symlinks: bool = True
    ) -> bool:  # pragma: no cover - exercised through snapshot
        raise OSError("boom")

    def stat(
        self, *, follow_symlinks: bool = True
    ):  # pragma: no cover - exercised through snapshot
        raise AssertionError("stat should not be called after exists fails")

    def __repr__(self) -> str:
        return f"_RaisingPath({self._inner!s})"


def test_snapshot_file_states_handles_oserror(tmp_path: Path) -> None:
    underlying = tmp_path / "sentinel"
    sentinel = _RaisingPath(underlying)
    snapshot = snapshot_file_states([cast(Path, sentinel)])
    assert snapshot[cast(Path, sentinel)] == (False, 0.0, 0)


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


def test_snapshot_file_states_handles_disappearing_path(tmp_path: Path) -> None:
    base = tmp_path / "vanish"

    class VanishingPath(type(base)):  # type: ignore[misc]
        def __new__(cls, inner: Path) -> "VanishingPath":  # type: ignore[override]
            self = Path.__new__(cls, str(inner))
            self._inner = inner
            self._calls = 0
            return self

        def exists(self, *, follow_symlinks: bool = True) -> bool:  # type: ignore[override]
            self._calls += 1
            return self._calls == 1

        def stat(self, *, follow_symlinks: bool = True):  # type: ignore[override]
            raise OSError("stat failed")

    ghost = VanishingPath(base)
    states = snapshot_file_states([ghost])
    assert states[ghost] == (False, 0.0, 0)
