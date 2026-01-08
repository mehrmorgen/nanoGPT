from __future__ import annotations

import json
from pathlib import Path

import pytest

from ml_playground.core.checkpoint_lock import (
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


@pytest.mark.parametrize("candidate", ["..", ".", "/", "//"])
def test_checkpoint_lock_path_rejects_parent_escape(tmp_path: Path, candidate: str) -> None:
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
    lock_path.write_text("{}")

    def _metadata_reader(path: Path) -> dict[str, object]:
        return {"owner": "train:primary", "pid": 1234, "timestamp": 10**18}

    with pytest.raises(CheckpointLockError) as exc:
        with checkpoint_lock(
            lock_path,
            owner="sample:secondary",
            metadata_reader=_metadata_reader,
        ):
            pass
    assert "unknown" in str(exc.value)


def test_checkpoint_lock_cleans_stale_metadata_lock(tmp_path: Path) -> None:
    """Stale locks based on metadata timestamps should be removed."""
    lock_path = checkpoint_lock_path(tmp_path, "ckpt_last.pt")
    lock_path.write_text(json.dumps({"owner": "train:primary", "timestamp": 0}))

    def _metadata_reader(path: Path) -> dict[str, object]:
        return json.loads(path.read_text())

    with checkpoint_lock(
        lock_path,
        owner="train:recovery",
        metadata_reader=_metadata_reader,
        stale_lock_timeout=0.0,
    ):
        assert lock_path.exists()

    assert not lock_path.exists()


def test_checkpoint_lock_reports_unknown_pid(tmp_path: Path) -> None:
    """Error message should show pid=unknown when metadata omits pid."""
    lock_path = checkpoint_lock_path(tmp_path, "ckpt_last.pt")

    def _metadata_reader(path: Path) -> dict[str, object]:
        data = json.loads(path.read_text())
        data.pop("pid", None)
        return data

    with checkpoint_lock(lock_path, owner="train:primary"):
        with pytest.raises(CheckpointLockError) as exc:
            with checkpoint_lock(
                lock_path,
                owner="sample:secondary",
                metadata_reader=_metadata_reader,
            ):
                pass
    message = str(exc.value)
    assert "pid=unknown" in message


def test_checkpoint_lock_retries_when_lock_removed(
    tmp_path: Path,
) -> None:
    """Lock acquisition retries when metadata read finds a stale file."""
    lock_path = checkpoint_lock_path(tmp_path, "ckpt_last.pt")
    lock_path.write_text("{}")

    def _remove_and_return_none(path: Path) -> dict[str, object] | None:
        path.unlink(missing_ok=True)
        return None

    with checkpoint_lock(
        lock_path, owner="train:test", metadata_reader=_remove_and_return_none
    ):
        assert lock_path.exists()

    assert not lock_path.exists()
