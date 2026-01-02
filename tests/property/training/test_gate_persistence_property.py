from __future__ import annotations

import json
from pathlib import Path
from tempfile import TemporaryDirectory

import pytest
from hypothesis import given, settings, strategies as st

from ml_playground.core.error_handling import DataError
from ml_playground.self_play.utilities import GateCounts
from ml_playground.training.gate_persistence import (
    GateSnapshot,
    load_gate_snapshot,
    save_gate_snapshot,
)


_STAGE_STRATEGY = st.none() | st.text(
    min_size=1,
    max_size=12,
    alphabet=st.characters(min_codepoint=97, max_codepoint=122),
)


@settings(max_examples=40, deadline=50, derandomize=True)
@given(
    wins=st.integers(min_value=0, max_value=1_000),
    losses=st.integers(min_value=0, max_value=1_000),
    draws=st.integers(min_value=0, max_value=1_000),
    stage=_STAGE_STRATEGY,
)
def test_gate_snapshot_round_trip(
    wins: int, losses: int, draws: int, stage: str | None
) -> None:
    """Gate snapshots round-trip through persistence."""
    with TemporaryDirectory() as tmp_dir:
        path = Path(tmp_dir) / "gate_state.json"
        snapshot = GateSnapshot(stage=stage, counts=GateCounts(wins, losses, draws))

        save_gate_snapshot(path, snapshot)
        loaded = load_gate_snapshot(path)

        assert loaded == snapshot


def test_gate_snapshot_missing_returns_none(tmp_path: Path) -> None:
    path = tmp_path / "missing_gate_state.json"
    assert load_gate_snapshot(path) is None


def test_gate_snapshot_rejects_negative_values(tmp_path: Path) -> None:
    path = tmp_path / "gate_state.json"
    payload = {"stage": "alpha", "wins": -1, "losses": 0, "draws": 0}
    path.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(DataError):
        load_gate_snapshot(path)


def test_gate_snapshot_rejects_invalid_json(tmp_path: Path) -> None:
    path = tmp_path / "gate_state.json"
    path.write_text("{invalid-json", encoding="utf-8")

    with pytest.raises(DataError):
        load_gate_snapshot(path)


def test_gate_snapshot_rejects_non_mapping_payload(tmp_path: Path) -> None:
    path = tmp_path / "gate_state.json"
    path.write_text(json.dumps(["not", "a", "mapping"]), encoding="utf-8")

    with pytest.raises(DataError):
        load_gate_snapshot(path)


def test_gate_snapshot_rejects_non_string_stage(tmp_path: Path) -> None:
    path = tmp_path / "gate_state.json"
    payload = {"stage": 1, "wins": 0, "losses": 0, "draws": 0}
    path.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(DataError):
        load_gate_snapshot(path)


def test_gate_snapshot_rejects_empty_stage(tmp_path: Path) -> None:
    path = tmp_path / "gate_state.json"
    payload = {"stage": "   ", "wins": 0, "losses": 0, "draws": 0}
    path.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(DataError):
        load_gate_snapshot(path)


def test_gate_snapshot_rejects_non_integer_counts(tmp_path: Path) -> None:
    path = tmp_path / "gate_state.json"
    payload = {"stage": "alpha", "wins": "1", "losses": 0, "draws": 0}
    path.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(DataError):
        load_gate_snapshot(path)


def test_save_gate_snapshot_rejects_negative_counts(tmp_path: Path) -> None:
    snapshot = GateSnapshot(
        stage="alpha", counts=GateCounts(wins=-1, losses=0, draws=0)
    )

    with pytest.raises(DataError):
        save_gate_snapshot(tmp_path / "gate_state.json", snapshot)
