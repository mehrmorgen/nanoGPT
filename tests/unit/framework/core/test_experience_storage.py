from __future__ import annotations

from pathlib import Path
import tempfile

import pytest
from typing import Any, cast

from ml_playground.framework.configuration.models import ExperienceStorageConfig
from ml_playground.framework.core.error_handling import DataError
from ml_playground.framework.core.experience_storage import (
    ExperienceEntry,
    InMemoryExperienceStorage,
    JSONFilePersistenceStrategy,
    build_experience_storage,
)


def _entry(
    moves: tuple[int, ...] = (1, 2),
    *,
    winner: int = 1,
    start_player: int = 0,
    priority_score: float = 0.0,
) -> ExperienceEntry:
    return ExperienceEntry(
        moves=moves,
        winner=winner,
        start_player=start_player,
        priority_score=priority_score,
    )


def test_json_persistence_load_returns_empty_when_missing(tmp_path: Path) -> None:
    """JSON persistence returns empty mapping when file is missing."""
    persistence = JSONFilePersistenceStrategy(tmp_path / "missing.json")
    assert persistence.load() == {}


def test_json_persistence_load_rejects_invalid_json(tmp_path: Path) -> None:
    """JSON persistence raises DataError on invalid JSON."""
    path = tmp_path / "bad.json"
    path.write_text("{not json}", encoding="utf-8")
    persistence = JSONFilePersistenceStrategy(path)

    with pytest.raises(DataError):
        persistence.load()


def test_json_persistence_load_rejects_non_mapping(tmp_path: Path) -> None:
    """JSON persistence raises DataError on non-mapping payloads."""
    path = tmp_path / "list.json"
    path.write_text("[1, 2]", encoding="utf-8")
    persistence = JSONFilePersistenceStrategy(path)

    with pytest.raises(DataError):
        persistence.load()


def test_json_persistence_load_rejects_non_string_key(tmp_path: Path) -> None:
    """JSON persistence raises DataError on non-string keys."""
    path = tmp_path / "payload.json"
    path.write_text("{}", encoding="utf-8")

    def _loader(_text: str) -> dict[str, object]:
        return cast(
            dict[str, object], {1: {"moves": [1], "winner": 1, "start_player": 0}}
        )

    persistence = JSONFilePersistenceStrategy(path, loader=_loader)

    with pytest.raises(DataError):
        persistence.load()


def test_json_persistence_decode_entry_rejects_non_mapping_payload() -> None:
    """JSON persistence rejects non-mapping entry payloads."""
    with pytest.raises(DataError):
        JSONFilePersistenceStrategy._decode_entry("key", [1, 2])


def test_json_persistence_decode_entry_rejects_missing_fields() -> None:
    """JSON persistence rejects missing core fields."""
    with pytest.raises(DataError):
        JSONFilePersistenceStrategy._decode_entry("key", {"moves": [1]})


def test_json_persistence_decode_entry_rejects_invalid_moves() -> None:
    """JSON persistence rejects invalid move sequences."""
    payload = {"moves": "nope", "winner": 1, "start_player": 0}
    with pytest.raises(DataError):
        JSONFilePersistenceStrategy._decode_entry("key", payload)


def test_json_persistence_decode_entry_rejects_invalid_winner() -> None:
    """JSON persistence rejects non-integer winner fields."""
    payload = {"moves": [1], "winner": "x", "start_player": 0}
    with pytest.raises(DataError):
        JSONFilePersistenceStrategy._decode_entry("key", payload)


def test_json_persistence_validate_targets_rejects_invalid_policy() -> None:
    """JSON persistence rejects invalid policy target payloads."""
    payload = {
        "moves": [1],
        "winner": 1,
        "start_player": 0,
        "policy_targets": ["bad"],
    }
    with pytest.raises(DataError):
        JSONFilePersistenceStrategy._decode_entry("key", payload)


def test_json_persistence_validate_targets_rejects_invalid_value() -> None:
    """JSON persistence rejects invalid value target payloads."""
    payload = {
        "moves": [1],
        "winner": 1,
        "start_player": 0,
        "value_targets": {"d": "nope"},
    }
    with pytest.raises(DataError):
        JSONFilePersistenceStrategy._decode_entry("key", payload)


def test_json_persistence_get_int_rejects_non_integer() -> None:
    """JSON persistence rejects non-integer metadata values."""
    payload = {
        "moves": [1],
        "winner": 1,
        "start_player": 0,
        "visit_count": "nope",
    }
    with pytest.raises(DataError):
        JSONFilePersistenceStrategy._decode_entry("key", payload)


def test_json_persistence_get_float_rejects_non_numeric() -> None:
    """JSON persistence rejects non-numeric metadata values."""
    payload = {
        "moves": [1],
        "winner": 1,
        "start_player": 0,
        "priority_score": "nope",
    }
    with pytest.raises(DataError):
        JSONFilePersistenceStrategy._decode_entry("key", payload)


def test_json_persistence_rejects_hash_mismatch() -> None:
    """JSON persistence rejects entries whose hash does not match the key."""
    entry = _entry(moves=(1, 2), winner=1, start_player=0)
    payload = JSONFilePersistenceStrategy._encode_entry(entry)

    with pytest.raises(DataError):
        JSONFilePersistenceStrategy._decode_entry("wrong-key", payload)


def test_json_persistence_save_rejects_non_string_key(tmp_path: Path) -> None:
    """JSON persistence rejects non-string keys on save."""
    persistence = JSONFilePersistenceStrategy(tmp_path / "store.json")

    with pytest.raises(DataError):
        persistence.save(cast(Any, {1: _entry()}))


def test_json_persistence_save_rejects_write_error(tmp_path: Path) -> None:
    """JSON persistence raises DataError when writes fail."""
    path = tmp_path / "dir"
    path.mkdir()
    persistence = JSONFilePersistenceStrategy(path)

    with pytest.raises(DataError):
        persistence.save({"k": _entry()})


def test_inmemory_storage_store_update_and_clear() -> None:
    """In-memory storage updates entries and handles clears."""
    storage = InMemoryExperienceStorage()
    first = _entry(priority_score=1.0)
    second = _entry(moves=(2, 3), priority_score=2.0)

    key1 = storage.store(first)
    key2 = storage.store(second)
    assert storage.has(key1)
    assert storage.get(key2) == second

    storage.update(key1, priority_score=3.0)
    assert storage.get_by_priority(1)[0].priority_score == 3.0

    storage.clear()
    assert list(storage.entries()) == []


def test_inmemory_storage_update_missing_key_raises() -> None:
    """In-memory storage raises on updates for unknown keys."""
    storage = InMemoryExperienceStorage()

    with pytest.raises(KeyError):
        storage.update("missing", priority_score=1.0)


def test_inmemory_storage_store_existing_entry_skips_rebuild() -> None:
    """Storing an existing entry avoids index rebuilds."""
    storage = InMemoryExperienceStorage()
    entry = _entry()
    key = storage.store(entry)
    index_before = list(storage._priority_index)

    storage.store(entry)
    assert storage._priority_index == index_before
    assert storage.has(key)


def test_inmemory_storage_update_keeps_priority_when_unchanged() -> None:
    """Updating without priority changes preserves ordering."""
    storage = InMemoryExperienceStorage()
    entry = _entry(priority_score=1.0)
    key = storage.store(entry)

    storage.update(key, visit_count=2)
    assert storage.get_by_priority(1)[0].priority_score == 1.0


def test_inmemory_storage_flush_persists_when_enabled(tmp_path: Path) -> None:
    """Flush-on-store persistence writes entries to disk."""
    path = tmp_path / "store.json"
    persistence = JSONFilePersistenceStrategy(path)
    storage = InMemoryExperienceStorage(persistence=persistence, flush_on_store=True)

    storage.store(_entry())
    assert path.exists()

    reloaded = InMemoryExperienceStorage(persistence=persistence)
    assert len(list(reloaded.entries())) == 1


def test_inmemory_storage_update_flushes_when_enabled(tmp_path: Path) -> None:
    """Flush-on-store persistence writes updates to disk."""
    path = tmp_path / "store.json"
    persistence = JSONFilePersistenceStrategy(path)
    storage = InMemoryExperienceStorage(persistence=persistence, flush_on_store=True)
    key = storage.store(_entry(priority_score=1.0))

    storage.update(key, priority_score=2.0)
    assert path.exists()


def test_build_experience_storage_constructs_json_backend(tmp_path: Path) -> None:
    """Factory builds JSON-backed storage when configured."""
    config = ExperienceStorageConfig(strategy="json_file", path=tmp_path / "store.json")
    storage = build_experience_storage(config)

    key = storage.store(_entry())
    assert storage.has(key)


def test_build_experience_storage_rejects_missing_path() -> None:
    """Factory rejects json_file configs without a path."""
    config = ExperienceStorageConfig.model_construct(
        strategy="json_file",
        path=None,
        flush_on_store=False,
        extras={},
        logger=object(),
    )

    with pytest.raises(ValueError):
        build_experience_storage(config)


def test_build_experience_storage_constructs_memory_backend() -> None:
    """Factory builds an in-memory storage backend by default."""
    config = ExperienceStorageConfig(strategy="memory")
    storage = build_experience_storage(config)
    assert isinstance(storage, InMemoryExperienceStorage)


def test_inmemory_storage_bool_reflects_content() -> None:
    """In-memory storage truthiness reflects whether entries exist."""
    storage = InMemoryExperienceStorage()
    assert not storage
    storage.store(_entry())
    assert storage


def test_inmemory_storage_entries_iterable() -> None:
    """In-memory storage provides iterable entries."""
    storage = InMemoryExperienceStorage()
    entry = _entry()
    storage.store(entry)

    assert list(storage) == [entry]


def test_inmemory_storage_flush_no_persistence_is_noop() -> None:
    """Flush is a no-op when no persistence is configured."""
    storage = InMemoryExperienceStorage()
    storage.flush()


def test_inmemory_storage_load_rebuilds_indices(tmp_path: Path) -> None:
    """Loading from persistence rebuilds priority indices."""
    path = tmp_path / "store.json"
    persistence = JSONFilePersistenceStrategy(path)
    storage = InMemoryExperienceStorage(persistence=persistence)
    storage.store(_entry(priority_score=1.0))
    storage.flush()

    reloaded = InMemoryExperienceStorage(persistence=persistence)
    assert reloaded.get_by_priority(1)[0].priority_score == 1.0


def test_inmemory_storage_load_refreshes_entries(tmp_path: Path) -> None:
    """Explicit load refreshes entries from persistence."""
    path = tmp_path / "store.json"
    persistence = JSONFilePersistenceStrategy(path)
    persistence.save({_entry().get_hash(): _entry()})

    storage = InMemoryExperienceStorage(persistence=persistence)
    storage.clear()
    storage.load()
    assert len(list(storage.entries())) == 1


def test_inmemory_storage_load_no_persistence_is_noop() -> None:
    """Load is a no-op when no persistence is configured."""
    storage = InMemoryExperienceStorage()
    storage.load()


def test_json_persistence_round_trip(tmp_path: Path) -> None:
    """JSON persistence round-trips entries through disk."""
    path = tmp_path / "store.json"
    persistence = JSONFilePersistenceStrategy(path)
    entry = _entry(priority_score=2.5)
    persistence.save({entry.get_hash(): entry})

    loaded = persistence.load()
    assert list(loaded.values()) == [entry]


def test_json_persistence_decode_entry_rejects_invalid_policy_value_types() -> None:
    """JSON persistence rejects invalid policy/value target types."""
    payload = {
        "moves": [1],
        "winner": 1,
        "start_player": 0,
        "policy_targets": {"a": "nope"},
        "value_targets": {"b": 1.0},
    }
    with pytest.raises(DataError):
        JSONFilePersistenceStrategy._decode_entry("key", payload)


def test_json_persistence_decode_entry_accepts_defaults() -> None:
    """JSON persistence fills missing optional fields with defaults."""
    entry = _entry(moves=(3,), winner=0, start_player=1)
    payload = {
        "moves": [3],
        "winner": 0,
        "start_player": 1,
    }
    decoded = JSONFilePersistenceStrategy._decode_entry(entry.get_hash(), payload)
    assert decoded.first_seen_step == 0
    assert decoded.priority_score == 0.0


def test_json_persistence_loader_invalid_entry_types(tmp_path: Path) -> None:
    """JSON persistence rejects entries with invalid types from loader."""
    path = tmp_path / "payload.json"
    path.write_text("{}", encoding="utf-8")

    def _loader(_text: str) -> dict[str, object]:
        return {"key": {"moves": [1], "winner": 1, "start_player": "nope"}}

    persistence = JSONFilePersistenceStrategy(path, loader=_loader)

    with pytest.raises(DataError):
        persistence.load()


def test_json_persistence_encode_decode_policy_value_targets_roundtrip() -> None:
    """JSON persistence encodes and decodes target maps."""
    entry = ExperienceEntry(
        moves=(1, 2),
        winner=1,
        start_player=0,
        policy_targets={"a": [1, 2]},
        value_targets={"b": 1.5},
        visit_count=3,
    )
    payload = JSONFilePersistenceStrategy._encode_entry(entry)
    decoded = JSONFilePersistenceStrategy._decode_entry(entry.get_hash(), payload)
    assert decoded.policy_targets == {"a": [1, 2]}
    assert decoded.value_targets == {"b": 1.5}


def test_json_persistence_save_rejects_non_string_key_in_entries(
    tmp_path: Path,
) -> None:
    """JSON persistence rejects non-string keys in entries mapping."""
    persistence = JSONFilePersistenceStrategy(tmp_path / "store.json")
    entries = {"ok": _entry(), 2: _entry(moves=(3,))}

    with pytest.raises(DataError):
        persistence.save(cast(Any, entries))


def test_json_persistence_round_trip_with_tempdir() -> None:
    """JSON persistence saves and loads from temp directory paths."""
    with tempfile.TemporaryDirectory() as tmp_dir:
        path = Path(tmp_dir) / "store.json"
        persistence = JSONFilePersistenceStrategy(path)
        entry = _entry(moves=(9,), priority_score=1.0)
        persistence.save({entry.get_hash(): entry})
        loaded = persistence.load()
        assert loaded[entry.get_hash()] == entry
