from __future__ import annotations

from pathlib import Path
from typing import Iterable


FileState = tuple[bool, float, int]


def _path_key(path: Path) -> str:
    try:
        return str(path)
    except (AttributeError, TypeError, ValueError):
        inner = getattr(path, "_inner", None)
        if isinstance(inner, Path):
            return str(inner)
        return repr(path)


def snapshot_file_states(paths: Iterable[Path]) -> dict[str, FileState]:
    """Capture existence, modification time, and size for each path."""

    snapshot: dict[str, FileState] = {}
    for path in paths:
        path_key = _path_key(path)
        try:
            exists = path.exists()
            if exists:
                try:
                    stat = path.stat()
                except OSError:
                    # Path may have disappeared between exists() and stat()
                    if not path.exists():
                        snapshot[path_key] = (False, 0.0, 0)
                    else:
                        snapshot[path_key] = (True, 0.0, 0)
                else:
                    snapshot[path_key] = (True, stat.st_mtime, stat.st_size)
            else:
                snapshot[path_key] = (False, 0.0, 0)
        except OSError:
            snapshot[path_key] = (False, 0.0, 0)
    return snapshot


def diff_file_states(
    paths: Iterable[Path], before: dict[str, FileState]
) -> tuple[set[str], set[str], set[str]]:
    """Compare file states and determine created, updated, and skipped paths."""

    after = snapshot_file_states(paths)
    created: set[str] = set()
    updated: set[str] = set()
    skipped: set[str] = set()

    for path_key in set(before.keys()) | set(after.keys()):
        b_exists, b_mtime, b_size = before.get(path_key, (False, 0.0, 0))
        a_exists, a_mtime, a_size = after.get(path_key, (False, 0.0, 0))

        if not b_exists and a_exists:
            created.add(path_key)
        elif b_exists and not a_exists:
            continue
        elif b_exists and a_exists:
            if b_mtime != a_mtime or b_size != a_size:
                updated.add(path_key)
            else:
                skipped.add(path_key)

    return created, updated, skipped


__all__ = [
    "FileState",
    "snapshot_file_states",
    "diff_file_states",
]
