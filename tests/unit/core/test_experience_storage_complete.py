from __future__ import annotations
import pytest
from pathlib import Path
from ml_playground.core.experience_storage import (
    ExperienceEntry,
    InMemoryExperienceStorage,
    JSONFilePersistenceStrategy,
    build_experience_storage,
)
from ml_playground.configuration.models import ExperienceStorageConfig


def test_experience_entry_hash():
    entry1 = ExperienceEntry(moves=(1, 2), winner=1, start_player=0)
    entry2 = ExperienceEntry(moves=(1, 2), winner=1, start_player=0)
    entry3 = ExperienceEntry(moves=(2, 1), winner=1, start_player=0)

    assert entry1.get_hash() == entry2.get_hash()
    assert entry1.get_hash() != entry3.get_hash()
    assert isinstance(entry1.get_hash(), str)


def test_in_memory_storage_basic():
    storage = InMemoryExperienceStorage()
    entry = ExperienceEntry(moves=(1, 2, 3), winner=1, start_player=0)

    key = storage.store(entry)
    assert storage.has(key)
    assert storage.get(key) == entry
    assert len(storage) == 1
    assert list(storage.entries()) == [entry]
    assert list(storage) == [entry]
    assert bool(storage) is True


def test_in_memory_storage_update():
    storage = InMemoryExperienceStorage()
    entry = ExperienceEntry(moves=(1,), winner=1, start_player=0, priority_score=1.0)
    key = storage.store(entry)

    storage.update(key, priority_score=2.0, visit_count=5)
    updated = storage.get(key)
    assert updated.priority_score == 2.0
    assert updated.visit_count == 5
    assert updated.moves == (1,)

    # Test update with missing key raises KeyError
    with pytest.raises(KeyError, match="Entry not found"):
        storage.update("nonexistent", priority_score=1.0)


def test_in_memory_storage_update_no_priority_change():
    storage = InMemoryExperienceStorage()
    entry = ExperienceEntry(moves=(1,), winner=1, start_player=0, priority_score=1.0)
    key = storage.store(entry)
    # Update visit_count only - priority remains same, no index rebuild
    storage.update(key, visit_count=10)
    assert storage.get(key).visit_count == 10
    assert storage.get(key).priority_score == 1.0


def test_in_memory_storage_priority_index():
    storage = InMemoryExperienceStorage()
    e1 = ExperienceEntry(moves=(1,), winner=1, start_player=0, priority_score=1.0)
    e2 = ExperienceEntry(moves=(2,), winner=1, start_player=0, priority_score=5.0)
    e3 = ExperienceEntry(moves=(3,), winner=1, start_player=0, priority_score=3.0)

    storage.store(e1)
    storage.store(e2)
    storage.store(e3)

    top = storage.get_by_priority(2)
    assert len(top) == 2
    assert top[0].priority_score == 5.0
    assert top[1].priority_score == 3.0

    # Test update triggers re-sort
    key1 = e1.get_hash()
    storage.update(key1, priority_score=10.0)
    top = storage.get_by_priority(1)
    assert top[0].priority_score == 10.0


def test_in_memory_storage_clear():
    storage = InMemoryExperienceStorage()
    storage.store(ExperienceEntry(moves=(1,), winner=1, start_player=0))
    assert len(storage) == 1
    storage.clear()
    assert len(storage) == 0


def test_json_persistence_roundtrip(tmp_path: Path):
    path = tmp_path / "storage.json"
    strategy = JSONFilePersistenceStrategy(path)
    entry = ExperienceEntry(moves=(1, 2), winner=1, start_player=0, priority_score=4.2)
    entries = {entry.get_hash(): entry}

    strategy.save(entries)
    loaded = strategy.load()

    assert len(loaded) == 1
    assert entry.get_hash() in loaded
    assert loaded[entry.get_hash()].moves == (1, 2)
    assert loaded[entry.get_hash()].priority_score == 4.2


def test_build_experience_storage_json(tmp_path: Path):
    path = tmp_path / "exp.json"
    cfg = ExperienceStorageConfig(strategy="json_file", path=path)
    storage = build_experience_storage(cfg)
    assert isinstance(storage, InMemoryExperienceStorage)

    entry = ExperienceEntry(moves=(1,), winner=1, start_player=0)
    storage.store(entry)
    storage.flush()
    assert path.exists()


def test_build_experience_storage_memory():
    cfg = ExperienceStorageConfig(strategy="memory")
    storage = build_experience_storage(cfg)
    assert isinstance(storage, InMemoryExperienceStorage)


def test_in_memory_storage_persistence_flush():
    # Test that flush correctly delegates to persistence save
    class MockPersistence:
        def __init__(self):
            self.saved_data = None

        def save(self, data):
            self.saved_data = dict(data)

        def load(self):
            return {}

    persistence = MockPersistence()
    storage = InMemoryExperienceStorage(persistence=persistence)
    entry = ExperienceEntry(moves=(1,), winner=1, start_player=0)
    storage.store(entry)
    storage.flush()
    assert persistence.saved_data is not None
    assert entry.get_hash() in persistence.saved_data


def test_in_memory_storage_persistence_load():
    # Test that load correctly delegates to persistence load
    entry = ExperienceEntry(moves=(1,), winner=1, start_player=0)

    class MockPersistence:
        def load(self):
            return {entry.get_hash(): entry}

        def save(self, data):
            pass

    persistence = MockPersistence()
    storage = InMemoryExperienceStorage(persistence=persistence)
    # The constructor calls load() and _rebuild_indices()
    assert storage.has(entry.get_hash())
    assert len(storage) == 1

    # Test explicit load() method
    storage.clear()
    assert len(storage) == 0
    storage.load()
    assert len(storage) == 1
    assert storage.has(entry.get_hash())
