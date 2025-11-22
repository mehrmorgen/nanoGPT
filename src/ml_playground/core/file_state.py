from __future__ import annotations

from pathlib import Path
from typing import Dict, Iterable, Set, Tuple


def _ensure_hashable_path(path: Path) -> None:
    """Populate internal Path attributes for subclasses lacking them."""

    if hasattr(path, "_raw_paths") and getattr(path, "_raw_paths") is not None:  # type: ignore[attr-defined]  # TODO(path-hack): Accessing internal Path attributes for hashability
        return

    inner = getattr(path, "_inner", None)
    source = inner if isinstance(inner, Path) else Path(str(path))

    raw_paths = getattr(source, "_raw_paths", None)  # type: ignore[attr-defined]  # TODO(path-hack): Accessing internal Path attributes for hashability
    if raw_paths is None:
        raw_paths = [str(source)]

    setattr(path, "_raw_paths", raw_paths)
    setattr(path, "_drv", getattr(source, "_drv", source.drive))
    setattr(path, "_root", getattr(source, "_root", source.root))
    setattr(path, "_parts", getattr(source, "_parts", source.parts))
    flavour = getattr(source, "_flavour", getattr(type(source), "_flavour", None))
    if flavour is not None:
        setattr(path, "_flavour", flavour)
    setattr(path, "_hash", hash(tuple(raw_paths)))


FileState = Tuple[bool, float, int]


def snapshot_file_states(paths: Iterable[Path]) -> Dict[Path, FileState]:
    """Capture existence, modification time, and size for each path."""

    snapshot: Dict[Path, FileState] = {}
    for path in paths:
        try:
            _ensure_hashable_path(path)
            exists = path.exists()
            if exists:
                try:
                    stat = path.stat()
                except OSError:
                    # Path may have disappeared between exists() and stat()
                    if not path.exists():
                        snapshot[path] = (False, 0.0, 0)
                    else:
                        snapshot[path] = (True, 0.0, 0)
                else:
                    snapshot[path] = (True, stat.st_mtime, stat.st_size)
            else:
                snapshot[path] = (False, 0.0, 0)
        except OSError:
            snapshot[path] = (False, 0.0, 0)
    return snapshot


def diff_file_states(
    paths: Iterable[Path], before: Dict[Path, FileState]
) -> Tuple[Set[Path], Set[Path], Set[Path]]:
    """Compare file states and determine created, updated, and skipped paths."""

    after = snapshot_file_states(paths)
    created: Set[Path] = set()
    updated: Set[Path] = set()
    skipped: Set[Path] = set()

    for path in set(before.keys()) | set(after.keys()):
        b_exists, b_mtime, b_size = before.get(path, (False, 0.0, 0))
        a_exists, a_mtime, a_size = after.get(path, (False, 0.0, 0))

        if not b_exists and a_exists:
            created.add(path)
        elif b_exists and not a_exists:
            continue
        elif b_exists and a_exists:
            if b_mtime != a_mtime or b_size != a_size:
                updated.add(path)
            else:
                skipped.add(path)

    return created, updated, skipped


__all__ = [
    "FileState",
    "snapshot_file_states",
    "diff_file_states",
]
