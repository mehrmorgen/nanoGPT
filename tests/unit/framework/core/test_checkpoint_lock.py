from __future__ import annotations

import json
from pathlib import Path

import pytest

from ml_playground.framework.core.checkpoint_lock import (
    CheckpointLockError,
    checkpoint_lock,
    checkpoint_lock_path,
)


def test_checkpoint_lock_creates_and_cleans_lock(tmp_path: Path) -> None:
    """Lock file is created while held and cleaned afterward."""
    lock_path = checkpoint_lock_path(tmp_path, "ckpt_last.pt")
    with checkpoint_lock(lock_path, owner="train:test"):
        assert lock_path.exists()
    assert not lock_path.exists()


@pytest.mark.parametrize(  # type: ignore[reportAny]
    "candidate", ["..", ".", "/", "//"]
)
def test_checkpoint_lock_path_rejects_parent_escape(
    tmp_path: Path, candidate: str
) -> None:
    with pytest.raises(ValueError, match=r"must not (be|resolve)"):
        checkpoint_lock_path(tmp_path, candidate)


def test_checkpoint_lock_reports_existing_owner(tmp_path: Path) -> None:
    """Second acquisition surfaces owner of the existing lock."""
    lock_path = checkpoint_lock_path(tmp_path, "ckpt_last.pt")
    with checkpoint_lock(lock_path, owner="train:primary"):
        with pytest.raises(CheckpointLockError) as exc:
            with checkpoint_lock(lock_path, owner="sample:secondary"):
                pass
        message = str(exc.value)
        assert "train:primary" in message
        assert str(lock_path) in message


def test_checkpoint_lock_handles_bad_timestamp(tmp_path: Path) -> None:
    """Timestamp conversion should fall back to 'unknown' on overflow."""
    lock_path = checkpoint_lock_path(tmp_path, "ckpt_last.pt")

    # Create lock with invalid timestamp
    lock_path.write_text(
        json.dumps({"owner": "test", "pid": 123, "timestamp": float("inf")})
    )

    # Should handle gracefully and report 'unknown' timestamp
    with pytest.raises(CheckpointLockError, match="since unknown"):
        with checkpoint_lock(lock_path, owner="test2"):
            pass


def test_checkpoint_lock_metadata_missing(tmp_path: Path) -> None:
    """Handles case where lock file exists but has no metadata."""
    lock_path = checkpoint_lock_path(tmp_path, "ckpt_last.pt")

    # Create an empty lock file
    lock_path.write_text("")

    def _return_none(path: Path) -> dict[str, object] | None:
        return None

    with pytest.raises(CheckpointLockError, match="metadata unavailable"):
        with checkpoint_lock(lock_path, owner="test", metadata_reader=_return_none):
            pass


def test_read_lock_metadata_returns_none_for_non_mapping_json(
    tmp_path: Path,
) -> None:
    """read_lock_metadata returns None when the lock file contains non-Mapping JSON."""
    from ml_playground.framework.core.checkpoint_lock import read_lock_metadata

    lock_path = tmp_path / "ckpt_last.pt.lock"
    lock_path.write_text("[1, 2, 3]", encoding="utf-8")

    result = read_lock_metadata(lock_path)
    assert result is None
