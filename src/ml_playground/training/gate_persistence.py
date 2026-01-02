from __future__ import annotations

from dataclasses import dataclass
import json
from pathlib import Path
from typing import Any, Mapping

from ml_playground.core.error_handling import DataError
from ml_playground.self_play.utilities import GateCounts

__all__ = ["GateSnapshot", "load_gate_snapshot", "save_gate_snapshot"]


@dataclass(frozen=True)
class GateSnapshot:
    stage: str | None
    counts: GateCounts


def save_gate_snapshot(path: Path, snapshot: GateSnapshot) -> None:
    """Persist gate counts and stage state to disk."""
    stage = _coerce_stage(snapshot.stage, "gate snapshot stage")
    counts = _coerce_counts(snapshot.counts, "gate snapshot counts")
    payload = {
        "stage": stage,
        "wins": counts.wins,
        "losses": counts.losses,
        "draws": counts.draws,
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, indent=2, sort_keys=True),
        encoding="utf-8",
    )


def load_gate_snapshot(path: Path) -> GateSnapshot | None:
    """Load gate counts and stage state if present."""
    if not path.exists():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise DataError(
            f"Failed to read gate snapshot at {path}: {exc}",
            reason=f"{exc.__class__.__name__} while reading gate snapshot",
            rationale="Gate persistence requires valid JSON payloads",
        ) from exc
    return _coerce_snapshot(payload, path)


def _coerce_snapshot(payload: Any, path: Path) -> GateSnapshot:
    if not isinstance(payload, Mapping):
        raise DataError(
            f"Gate snapshot at {path} must be a mapping",
            reason="Snapshot payload is not a mapping",
            rationale="Gate persistence expects a JSON object with counters and stage state",
        )
    stage = _coerce_stage(payload.get("stage"), f"gate snapshot at {path}")
    wins = _coerce_non_negative_int(payload.get("wins"), "wins", path)
    losses = _coerce_non_negative_int(payload.get("losses"), "losses", path)
    draws = _coerce_non_negative_int(payload.get("draws"), "draws", path)
    return GateSnapshot(
        stage=stage, counts=GateCounts(wins=wins, losses=losses, draws=draws)
    )


def _coerce_stage(value: Any, label: str) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise DataError(
            f"{label} must be a string",
            reason="Stage label is not a string",
            rationale="Stage state must serialize to a human-readable label",
        )
    stripped = value.strip()
    if not stripped:
        raise DataError(
            f"{label} must be non-empty",
            reason="Stage label is empty",
            rationale="Gate persistence requires meaningful stage labels",
        )
    return stripped


def _coerce_counts(counts: GateCounts, label: str) -> GateCounts:
    for name, value in (
        ("wins", counts.wins),
        ("losses", counts.losses),
        ("draws", counts.draws),
    ):
        if value < 0:
            raise DataError(
                f"{label} has negative {name}: {value}",
                reason="Gate counts must be non-negative",
                rationale="Gate persistence tracks cumulative counters only",
            )
    return counts


def _coerce_non_negative_int(value: Any, field: str, path: Path) -> int:
    if not isinstance(value, int):
        raise DataError(
            f"Gate snapshot at {path} has non-integer {field}: {value}",
            reason="Counter value is not an integer",
            rationale="Gate persistence stores integer counters only",
        )
    if value < 0:
        raise DataError(
            f"Gate snapshot at {path} has negative {field}: {value}",
            reason="Counter value is negative",
            rationale="Gate persistence stores non-negative counters only",
        )
    return value
