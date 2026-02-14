from __future__ import annotations

import json
import logging
import os
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Callable, Iterator, Mapping, cast

__all__ = [
    "CheckpointLockError",
    "checkpoint_lock",
    "checkpoint_lock_path",
    "read_lock_metadata",
]


class CheckpointLockError(RuntimeError):
    """Raised when a checkpoint directory is already locked by another command."""


def checkpoint_lock_path(out_dir: Path, checkpoint_filename: str) -> Path:
    """Return the canonical lock path derived from ``checkpoint_filename``."""
    candidate = checkpoint_filename.strip()
    if candidate in {".", ".."}:
        raise ValueError("checkpoint_filename must not be '.' or '..'")

    candidate_path = Path(candidate)
    if candidate_path.is_absolute():
        raise ValueError("checkpoint_filename must not be absolute")

    filename = candidate_path.name or "checkpoint.pt"
    if filename in {".", ".."} or filename.startswith(os.sep):
        raise ValueError(
            "checkpoint_filename must not resolve to a root or parent path"
        )
    return out_dir / f"{filename}.lock"


def read_lock_metadata(lock_path: Path) -> dict[str, object] | None:
    """Best-effort read of lock metadata for diagnostics."""
    try:
        raw = lock_path.read_text(encoding="utf-8")
        data = cast(object, json.loads(raw))
        if isinstance(data, Mapping):
            # materialize to plain dict[str, object]
            typed_data = cast(dict[str, object], data)
            return {str(k): v for k, v in typed_data.items()}
    except (OSError, json.JSONDecodeError, UnicodeDecodeError):
        pass
    return None


@contextmanager
def checkpoint_lock(
    lock_path: Path,
    *,
    owner: str,
    metadata_reader: Callable[[Path], dict[str, object] | None] | None = None,
    max_retries: int = 3,
    stale_lock_timeout: float | None = 600.0,
) -> Iterator[None]:
    """Acquire an exclusive lock for the checkpoint directory."""
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    lock_created_ts = time.time()
    info: dict[str, float | int | str] = {
        "owner": owner,
        "pid": os.getpid(),
        "timestamp": lock_created_ts,
    }
    if max_retries < 0:
        raise ValueError("max_retries must be non-negative")
    if stale_lock_timeout is not None and stale_lock_timeout < 0:
        raise ValueError("stale_lock_timeout must be non-negative or None")

    flags = os.O_CREAT | os.O_EXCL | os.O_WRONLY
    attempts = 0
    fd: int = -1

    def _handle_existing_lock(
        metadata: dict[str, object] | None, current_attempts: int
    ) -> tuple[bool, int]:
        """Return (should_retry, updated_attempts) for an existing lock."""
        stale_lock = metadata is None and not lock_path.exists()
        if stale_lock and current_attempts < max_retries:
            return True, current_attempts + 1
        if metadata:
            owner_str = str(metadata.get("owner", "unknown"))
            pid = metadata.get("pid")
            pid_str = str(pid) if pid is not None else "unknown"
            ts = metadata.get("timestamp")
            timestamp = "unknown"
            stale_by_metadata = False
            if isinstance(ts, (int, float)):
                try:
                    ts_float = float(ts)
                    timestamp = time.strftime(
                        "%Y-%m-%d %H:%M:%S UTC", time.gmtime(ts_float)
                    )
                except (ValueError, OSError, OverflowError, TypeError):
                    timestamp = "unknown"
                else:
                    should_consider_stale = ts_float <= lock_created_ts
                    if should_consider_stale and (
                        stale_lock_timeout is not None
                        and time.time() - ts_float > stale_lock_timeout
                    ):
                        stale_by_metadata = True
            if stale_by_metadata and current_attempts < max_retries:
                try:
                    lock_path.unlink(missing_ok=True)
                except OSError as unlink_exc:
                    logging.getLogger(__name__).warning(
                        "Failed to remove stale checkpoint lock at %s owned by %s",
                        lock_path,
                        owner_str,
                        exc_info=unlink_exc,
                    )
                return True, current_attempts + 1
            raise CheckpointLockError(
                f"Checkpoint lock at {lock_path} is already held by {owner_str} (pid={pid_str}) since {timestamp}."
            )
        raise CheckpointLockError(
            f"Checkpoint lock at {lock_path} is already held; metadata unavailable."
        )

    while True:
        try:
            fd = os.open(lock_path, flags, mode=0o644)
            break
        except FileExistsError as exc:
            reader = metadata_reader or read_lock_metadata
            metadata = reader(lock_path)
            should_retry, attempts = _handle_existing_lock(metadata, attempts)
            if should_retry:
                continue
            raise exc

    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            try:
                json.dump(info, handle)
                handle.flush()
            except Exception:
                lock_path.unlink(missing_ok=True)
                raise
            yield
    finally:
        try:
            lock_path.unlink(missing_ok=True)
        except OSError as exc:
            logging.getLogger(__name__).warning(
                "Failed to remove checkpoint lock at %s owned by %s",
                lock_path,
                owner,
                exc_info=exc,
            )
