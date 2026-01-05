from __future__ import annotations

import json
from pathlib import Path

import pytest

from ml_playground.core.experience_storage import (
    ExperienceEntry,
    JSONFilePersistenceStrategy,
)
from ml_playground.core.error_handling import DataError


def test_json_persistence_save_rejects_non_string_key(tmp_path: Path) -> None:
    """Rejects saving when entries dict contains non-string key."""
    strategy = JSONFilePersistenceStrategy(tmp_path / "store.json")
    bad_entries = {123: ExperienceEntry(moves=(1,), winner=1, start_player=0)}
    with pytest.raises(DataError, match="Invalid entry key"):
        strategy.save(bad_entries)  # type: ignore[arg-type]


def test_json_persistence_load_detects_hash_mismatch(tmp_path: Path) -> None:
    """Raises when stored hash key does not match entry payload."""
    strategy = JSONFilePersistenceStrategy(tmp_path / "store.json")
    wrong_key = "not_a_real_hash"
    payload = {
        wrong_key: {
            "moves": [1, 2],
            "winner": 1,
            "start_player": 0,
            "policy_targets": {},
            "value_targets": {},
            "first_seen_step": 0,
            "last_seen_step": 0,
            "visit_count": 0,
            "priority_score": 0.0,
        }
    }
    strategy._path.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(DataError, match="failed hash verification"):
        strategy.load()
