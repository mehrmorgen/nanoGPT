from __future__ import annotations

from dataclasses import dataclass
import json
from pathlib import Path
from typing import Mapping, cast

from ml_playground.framework.core.di_implementations import DefaultJsonParser
from ml_playground.framework.core.error_handling import DataError
from ml_playground.framework.self_play.utilities import GateCounts

__all__ = ["GateSnapshot", "load_gate_snapshot", "save_gate_snapshot"]


@dataclass(frozen=True)
class GateSnapshot:
    stage: str | None
    counts: GateCounts


def save_gate_snapshot(path: Path, snapshot: GateSnapshot) -> None:
    """Persist gate counts and stage state to disk."""
    coercer = _GateSnapshotCoercer()
    stage = coercer.coerce_stage(snapshot.stage, "gate snapshot stage")
    counts = coercer.coerce_counts(snapshot.counts, "gate snapshot counts")
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


class _GateSnapshotCoercer:
    """Helper class to coerce gate snapshot data with strict typing."""

    def coerce(self, payload: object, path: Path) -> GateSnapshot:
        if not isinstance(payload, Mapping):
            raise DataError(
                f"Gate snapshot at {path} must be a mapping",
                reason="Snapshot payload is not a mapping",
                rationale="Gate persistence expects a JSON object with counters and stage state",
            )
        typed_payload: Mapping[str, object] = cast(Mapping[str, object], payload)

        stage = self.coerce_stage(
            typed_payload.get("stage"), f"gate snapshot at {path}"
        )
        wins = self.coerce_non_negative_int(typed_payload.get("wins", 0), "wins", path)
        losses = self.coerce_non_negative_int(
            typed_payload.get("losses", 0), "losses", path
        )
        draws = self.coerce_non_negative_int(
            typed_payload.get("draws", 0), "draws", path
        )

        return GateSnapshot(
            stage=stage, counts=GateCounts(wins=wins, losses=losses, draws=draws)
        )

    def coerce_stage(self, value: object, label: str) -> str | None:
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

    def coerce_non_negative_int(self, value: object, field: str, path: Path) -> int:
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

    def coerce_counts(self, counts: GateCounts, label: str) -> GateCounts:
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


def load_gate_snapshot(path: Path) -> GateSnapshot | None:
    """Load gate counts and stage state if present."""
    if not path.exists():
        return None
    try:
        content = path.read_text(encoding="utf-8")
        payload = DefaultJsonParser().parse_json(content)
    except (OSError, json.JSONDecodeError) as exc:
        raise DataError(
            f"Failed to read gate snapshot at {path}: {exc}",
            reason=f"{exc.__class__.__name__} while reading gate snapshot",
            rationale="Gate persistence requires valid JSON payloads",
        ) from exc
    return _GateSnapshotCoercer().coerce(payload, path)
