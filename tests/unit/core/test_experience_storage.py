from __future__ import annotations

from pathlib import Path

import pytest

from ml_playground.core.error_handling import DataError
from ml_playground.core.experience_storage import (
    ExperienceEntry,
    JSONFilePersistenceStrategy,
)


def test_json_persistence_load_returns_empty_when_file_missing(tmp_path: Path) -> None:
    missing = tmp_path / "missing.json"
    strategy = JSONFilePersistenceStrategy(missing)
    assert strategy.load() == {}


def test_json_persistence_load_raises_on_invalid_json(tmp_path: Path) -> None:
    bad = tmp_path / "bad.json"
    bad.write_text("not json")
    strategy = JSONFilePersistenceStrategy(bad)
    with pytest.raises(DataError, match="Failed to read experience storage"):
        strategy.load()


def test_json_persistence_load_raises_on_non_dict_payload(tmp_path: Path) -> None:
    bad = tmp_path / "bad.json"
    bad.write_text('["not", "a", "dict"]')
    strategy = JSONFilePersistenceStrategy(bad)
    with pytest.raises(DataError, match="must be a mapping"):
        strategy.load()


def test_json_persistence_save_raises_on_non_string_key(tmp_path: Path) -> None:
    path = tmp_path / "store.json"
    strategy = JSONFilePersistenceStrategy(path)
    with pytest.raises(DataError, match="Invalid entry key"):
        strategy.save({123: ExperienceEntry(moves=(1,), winner=1, start_player=0)})


# Additional branches merged from former branches file


def test_json_persistence_load_skips_invalid_indices_in_itos(tmp_path: Path) -> None:
    """Load raises DataError for invalid entry hash."""
    path = tmp_path / "store.json"
    path.write_text('{"key1": {"moves": [1], "winner": 1, "start_player": 0}}')
    strategy = JSONFilePersistenceStrategy(path)
    with pytest.raises(DataError, match="failed hash verification"):
        strategy.load()


def test_json_persistence_save_handles_os_error(tmp_path: Path) -> None:
    """Save handles OSError gracefully when path is not writable."""
    from ml_playground.core.experience_storage import ExperienceEntry

    # Create a path whose parent is a file, not a directory
    parent_as_file = tmp_path / "not_a_dir"
    parent_as_file.write_text("file")
    path = parent_as_file / "store.json"

    strategy = JSONFilePersistenceStrategy(path)
    with pytest.raises((DataError, OSError)):
        strategy.save({"key": ExperienceEntry(moves=(1,), winner=1, start_player=0)})


def test_json_persistence_load_handles_empty_file(tmp_path: Path) -> None:
    """Load raises DataError on empty file."""
    path = tmp_path / "store.json"
    path.write_text("")
    strategy = JSONFilePersistenceStrategy(path)
    with pytest.raises(DataError, match="Failed to read experience storage"):
        strategy.load()
