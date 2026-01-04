from __future__ import annotations

import json
import tempfile
from pathlib import Path

import pytest
from hypothesis import given, strategies as st
from pydantic import ValidationError

from ml_playground.configuration.models import ExperienceStorageConfig
from ml_playground.core.error_handling import DataError
from ml_playground.core.experience_storage import (
    ExperienceEntry,
    InMemoryExperienceStorage,
    JSONFilePersistenceStrategy,
    build_experience_storage,
)


def _entry_strategy() -> st.SearchStrategy[ExperienceEntry]:
    moves = st.lists(st.integers(min_value=0, max_value=6), min_size=1, max_size=8).map(
        tuple
    )
    winner = st.integers(min_value=0, max_value=2)
    start_player = st.integers(min_value=0, max_value=1)
    policy_key = st.text(min_size=1, max_size=5)
    value_key = st.text(min_size=1, max_size=5)
    policy_targets = st.dictionaries(
        keys=policy_key,
        values=st.lists(st.integers(min_value=0, max_value=42), max_size=4),
    )
    value_targets = st.dictionaries(
        keys=value_key, values=st.floats(allow_nan=False, allow_infinity=False)
    )
    meta_int = st.integers(min_value=0, max_value=10_000)
    meta_float = st.floats(min_value=0.0, max_value=10_000.0, allow_infinity=False)
    return st.builds(
        ExperienceEntry,
        moves=moves,
        winner=winner,
        start_player=start_player,
        policy_targets=policy_targets,
        value_targets=value_targets,
        first_seen_step=meta_int,
        last_seen_step=meta_int,
        visit_count=meta_int,
        priority_score=meta_float,
    )


@given(_entry_strategy())
def test_hash_deterministic(entry: ExperienceEntry) -> None:
    """Entry hash is stable for identical gameplay attributes."""
    first = entry.get_hash()
    second = ExperienceEntry(
        moves=entry.moves,
        winner=entry.winner,
        start_player=entry.start_player,
        policy_targets=entry.policy_targets,
        value_targets=entry.value_targets,
        first_seen_step=entry.first_seen_step,
        last_seen_step=entry.last_seen_step,
        visit_count=entry.visit_count,
        priority_score=entry.priority_score,
    ).get_hash()
    assert first == second
    assert len(first) == 64


@given(entry_list=st.lists(_entry_strategy(), min_size=1, max_size=6))
def test_json_persistence_roundtrip(entry_list: list[ExperienceEntry]) -> None:
    """JSON persistence strategy preserves entries exactly."""
    with tempfile.TemporaryDirectory() as td:
        path = Path(td) / "store.json"
        persistence = JSONFilePersistenceStrategy(path)
        storage = InMemoryExperienceStorage(
            persistence=persistence, flush_on_store=True
        )

        expected: dict[str, ExperienceEntry] = {}
        for entry in entry_list:
            storage.store(entry)
            expected[entry.get_hash()] = entry

        reloaded = JSONFilePersistenceStrategy(path).load()
        assert reloaded == expected


def test_json_persistence_rejects_invalid_payload(tmp_path: Path) -> None:
    """Invalid JSON payloads raise DataError."""
    path = tmp_path / "store.json"

    # 1. Not a mapping
    path.write_text('["not", "a", "mapping"]', encoding="utf-8")
    persistence = JSONFilePersistenceStrategy(path)
    with pytest.raises(DataError, match="must be a mapping"):
        persistence.load()

    # 2. Corrupt JSON
    path.write_text("{corrupt", encoding="utf-8")
    with pytest.raises(DataError, match="Failed to read"):
        persistence.load()

    # 3. Missing required fields in entry
    path.write_text(json.dumps({"hash": {"moves": [1]}}), encoding="utf-8")
    with pytest.raises(DataError, match="missing required field"):
        persistence.load()

    # 4. Invalid field types
    path.write_text(
        json.dumps({"hash": {"moves": ["invalid"], "winner": 1, "start_player": 1}}),
        encoding="utf-8",
    )
    with pytest.raises(DataError, match="invalid moves payload"):
        persistence.load()

    # 5. Invalid winner/start_player type
    path.write_text(
        json.dumps({"hash": {"moves": [1], "winner": "invalid", "start_player": 1}}),
        encoding="utf-8",
    )
    with pytest.raises(DataError, match="invalid winner/start_player"):
        persistence.load()

    # 6. Invalid policy_targets
    path.write_text(
        json.dumps(
            {
                "hash": {
                    "moves": [1],
                    "winner": 1,
                    "start_player": 1,
                    "policy_targets": "invalid",
                }
            }
        ),
        encoding="utf-8",
    )
    with pytest.raises(DataError, match="invalid policy_targets"):
        persistence.load()

    # 7. Invalid value_targets
    path.write_text(
        json.dumps(
            {
                "hash": {
                    "moves": [1],
                    "winner": 1,
                    "start_player": 1,
                    "value_targets": "invalid",
                }
            }
        ),
        encoding="utf-8",
    )
    with pytest.raises(DataError, match="invalid value_targets"):
        persistence.load()

    # 8. Non-integer metadata
    path.write_text(
        json.dumps(
            {
                "hash": {
                    "moves": [1],
                    "winner": 1,
                    "start_player": 1,
                    "visit_count": "invalid",
                }
            }
        ),
        encoding="utf-8",
    )
    with pytest.raises(DataError, match="has non-integer visit_count"):
        persistence.load()

    # 9. Non-numeric priority
    path.write_text(
        json.dumps(
            {
                "hash": {
                    "moves": [1],
                    "winner": 1,
                    "start_player": 1,
                    "priority_score": "invalid",
                }
            }
        ),
        encoding="utf-8",
    )
    with pytest.raises(DataError, match="has non-numeric priority_score"):
        persistence.load()

    # 10. Hash mismatch
    entry_data = {"moves": [1], "winner": 1, "start_player": 1}
    path.write_text(json.dumps({"wrong_hash": entry_data}), encoding="utf-8")
    with pytest.raises(DataError, match="failed hash verification"):
        persistence.load()

    # 11. Invalid key type (non-string) - though JSON keys are strings,
    # we test the internal _decode_entry or a manual dict for coverage
    path.write_text(json.dumps({"hash": entry_data}), encoding="utf-8")
    strategy = JSONFilePersistenceStrategy(path)
    # To trigger line 92 (non-string key), we'd need a non-string key in the dict
    # being passed to PersistenceStrategy methods, but load() reads from JSON (always strings).
    # We can test _decode_entry directly or mock parts if needed,
    # but let's try to pass a non-string key to save and see.
    with pytest.raises(DataError, match="Invalid entry key"):
        strategy.save({123: ExperienceEntry((1,), 1, 1)})  # type: ignore


def test_json_persistence_decode_entry_not_mapping(tmp_path: Path) -> None:
    """_decode_entry raises DataError if payload is not a mapping."""
    strategy = JSONFilePersistenceStrategy(tmp_path / "test.json")
    with pytest.raises(DataError, match="must be a mapping"):
        strategy._decode_entry("hash", "not a mapping")


def test_json_persistence_save_error(tmp_path: Path) -> None:
    """OS errors during save raise DataError."""
    # Create a directory where the file should be to cause an OSError on write_text
    storage_path = tmp_path / "read_only_dir"
    storage_path.mkdir()
    strategy = JSONFilePersistenceStrategy(storage_path)
    with pytest.raises(DataError, match="Failed to persist"):
        strategy.save({"hash": ExperienceEntry((1,), 1, 1)})


def test_in_memory_storage_basic_ops() -> None:
    """Basic store/get/has/len/iter operations."""
    storage = InMemoryExperienceStorage()
    assert not storage
    entry = ExperienceEntry((1, 2, 3), 1, 1)
    h = storage.store(entry)

    assert storage
    assert storage.has(h)
    assert storage.get(h) == entry
    assert storage.get("unknown") is None
    assert len(storage) == 1
    assert list(storage) == [entry]


def test_in_memory_storage_flush_and_clear(tmp_path: Path) -> None:
    """Flush, clear and load operations with persistence."""
    storage_path = tmp_path / "experience.json"
    strategy = JSONFilePersistenceStrategy(storage_path)
    storage = InMemoryExperienceStorage(persistence=strategy, flush_on_store=True)

    entry = ExperienceEntry((1,), 1, 1)
    storage.store(entry)

    # Check it was flushed automatically
    assert storage_path.exists()

    # Clear storage
    storage.clear()
    assert len(storage) == 0

    # Explicitly load from persistence
    storage.load()
    assert len(storage) == 1
    assert list(storage.entries())[0].moves == (1,)

    # Clear again for clean exit
    storage.clear()
    assert len(storage) == 0


def test_build_experience_storage_factory() -> None:
    """Factory correctly interprets strategy and flush settings."""
    # 1. Memory strategy
    cfg_mem = ExperienceStorageConfig(strategy="memory", flush_on_store=False)
    storage_mem = build_experience_storage(cfg_mem)
    assert isinstance(storage_mem, InMemoryExperienceStorage)
    assert storage_mem._persistence is None

    # 2. JSON strategy
    cfg_json = ExperienceStorageConfig(
        strategy="json_file", path=Path("test.json"), flush_on_store=True
    )
    storage_json = build_experience_storage(cfg_json)
    assert isinstance(storage_json, InMemoryExperienceStorage)
    assert isinstance(storage_json._persistence, JSONFilePersistenceStrategy)
    assert storage_json._flush_on_store is True

    # 3. Missing path for json_file
    with pytest.raises(ValidationError, match="experience storage path is required"):
        ExperienceStorageConfig(strategy="json_file", path=None)

    # 4. Memory strategy with path (warning branch coverage)
    cfg_warn = ExperienceStorageConfig(strategy="memory", path=Path("ignored.json"))
    assert cfg_warn.strategy == "memory"


def test_build_experience_storage_with_json(tmp_path: Path) -> None:
    """End-to-end factory build and store with JSON."""
    cfg = ExperienceStorageConfig(
        strategy="json_file",
        path=tmp_path / "store.json",
        flush_on_store=True,
    )
    storage = build_experience_storage(cfg)
    entry = ExperienceEntry(moves=(1, 2, 3), winner=1, start_player=0)
    key = storage.store(entry)
    assert storage.get(key) == entry
    assert (tmp_path / "store.json").exists()

    def test_json_persistence_save_invalid_key(tmp_path: Path) -> None:
        """save() raises DataError if a non-string key is encountered."""
        strategy = JSONFilePersistenceStrategy(tmp_path / "test.json")
        with pytest.raises(DataError, match="Invalid entry key"):
            strategy.save({123: ExperienceEntry((1,), 1, 1)})  # type: ignore

    def test_in_memory_storage_flush_no_persistence() -> None:
        """flush() does nothing if no persistence strategy is set."""
        storage = InMemoryExperienceStorage()
        storage.flush()  # Should not raise

    def test_in_memory_storage_load_no_persistence() -> None:
        """load() does nothing if no persistence strategy is set."""
        storage = InMemoryExperienceStorage()
        storage.load()  # Should not raise

    def test_build_experience_storage_factory_json_missing_path() -> None:
        """Factory check for missing path (safety check)."""
        from ml_playground.configuration.models import ExperienceStorageConfig

        # We bypass Pydantic validation by creating a dict and passing to factory if possible,
        # but the factory takes the config object.
        # Since ExperienceStorageConfig is strict, we can't easily pass None if strategy="json_file"
        # without triggering Pydantic's validator first.
        # However, we can use .model_construct() or just test the factory logic directly if we can.
        cfg = ExperienceStorageConfig.model_construct(strategy="json_file", path=None)
        with pytest.raises(ValueError, match="experience storage path is required"):
            build_experience_storage(cfg)
