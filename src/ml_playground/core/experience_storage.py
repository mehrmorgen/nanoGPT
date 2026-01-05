from __future__ import annotations

import hashlib
import json
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any, Dict, Iterable, Iterator, List, Mapping, Tuple

from ml_playground.core.error_handling import DataError

__all__ = [
    "ExperienceEntry",
    "ExperienceStorage",
    "InMemoryExperienceStorage",
    "PersistenceStrategy",
    "JSONFilePersistenceStrategy",
    "build_experience_storage",
]

if TYPE_CHECKING:
    from ml_playground.configuration.models import ExperienceStorageConfig


@dataclass(frozen=True)
class ExperienceEntry:
    """A canonical entry in the global knowledge base representing a unique game state sequence."""

    moves: Tuple[int, ...]
    winner: int
    start_player: int
    policy_targets: Dict[str, List[int]] = field(default_factory=dict)
    value_targets: Dict[str, float] = field(default_factory=dict)
    first_seen_step: int = 0
    last_seen_step: int = 0
    visit_count: int = 0
    priority_score: float = 0.0

    def get_hash(self) -> str:
        """Compute a canonical identity hash based on immutable gameplay attributes."""
        payload = {
            "moves": list(self.moves),
            "winner": self.winner,
            "start_player": self.start_player,
        }
        encoded = json.dumps(payload, separators=(",", ":"), sort_keys=True).encode(
            "utf-8"
        )
        return hashlib.sha256(encoded).hexdigest()


class PersistenceStrategy(ABC):
    """Persistence strategy for experience storage backends."""

    @abstractmethod
    def load(self) -> Mapping[str, ExperienceEntry]:
        """Load stored experiences keyed by canonical hash."""

    @abstractmethod
    def save(self, entries: Mapping[str, ExperienceEntry]) -> None:
        """Persist experiences keyed by canonical hash."""


class JSONFilePersistenceStrategy(PersistenceStrategy):
    """JSON-based persistence for experience storage suitable for light workloads."""

    def __init__(self, path: Path) -> None:
        self._path = path

    def load(self) -> Mapping[str, ExperienceEntry]:
        if not self._path.exists():
            return {}
        try:
            payload = json.loads(self._path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise DataError(
                f"Failed to read experience storage from {self._path}: {exc}",
                reason="Unable to decode persisted experience store",
                rationale="Experience storage requires a valid JSON mapping of hash -> entry payloads",
            ) from exc

        if not isinstance(payload, dict):
            raise DataError(
                f"Experience storage at {self._path} must be a mapping",
                reason="Persisted payload is not a mapping",
                rationale="Experience storage expects JSON object with hash keys",
            )

        entries: dict[str, ExperienceEntry] = {}
        for key, value in payload.items():
            if not isinstance(key, str):
                raise DataError(
                    f"Invalid entry key in experience storage: {key}",
                    reason="Non-string hash key encountered",
                    rationale="Experience storage keys must be canonical hash strings",
                )
            entries[key] = self._decode_entry(key, value)
        return entries

    def save(self, entries: Mapping[str, ExperienceEntry]) -> None:
        for key in entries:
            if not isinstance(key, str):
                raise DataError(
                    f"Invalid entry key in experience storage: {key}",
                    reason="Non-string hash key encountered",
                    rationale="Experience storage keys must be canonical hash strings",
                )
        serializable = {
            key: self._encode_entry(entry) for key, entry in entries.items()
        }
        self._path.parent.mkdir(parents=True, exist_ok=True)
        try:
            self._path.write_text(
                json.dumps(serializable, indent=2, sort_keys=True),
                encoding="utf-8",
            )
        except OSError as exc:
            raise DataError(
                f"Failed to persist experience storage to {self._path}: {exc}",
                reason="Unable to write persisted experience store",
                rationale="Experience storage requires write access to the configured path",
            ) from exc

    @staticmethod
    def _encode_entry(entry: ExperienceEntry) -> dict:
        return {
            "moves": list(entry.moves),
            "winner": entry.winner,
            "start_player": entry.start_player,
            "policy_targets": entry.policy_targets,
            "value_targets": entry.value_targets,
            "first_seen_step": entry.first_seen_step,
            "last_seen_step": entry.last_seen_step,
            "visit_count": entry.visit_count,
            "priority_score": entry.priority_score,
        }

    @staticmethod
    def _decode_entry(key: str, payload: object) -> ExperienceEntry:
        if not isinstance(payload, Mapping):
            raise DataError(
                f"Experience entry {key} must be a mapping",
                reason="Entry payload is not a mapping",
                rationale="Experience storage expects dict payload per entry",
            )

        # 1. Core game state
        try:
            moves = payload["moves"]
            winner = payload["winner"]
            start_player = payload["start_player"]
        except KeyError as exc:
            raise DataError(
                f"Experience entry {key} is missing required field {exc.args[0]}",
                reason="Missing required entry field",
                rationale="Experience storage requires moves, winner, and start_player",
            ) from exc

        if not isinstance(moves, list) or not all(isinstance(m, int) for m in moves):
            raise DataError(
                f"Experience entry {key} has invalid moves payload",
                reason="Moves are not a list of integers",
                rationale="Experience storage requires ordered move sequences",
            )
        if not isinstance(winner, int) or not isinstance(start_player, int):
            raise DataError(
                f"Experience entry {key} has invalid winner/start_player",
                reason="winner/start_player must be integers",
                rationale="Experience storage tracks players using integer identifiers",
            )

        # 2. Targets (policy/value)
        policy_targets = payload.get("policy_targets", {})
        value_targets = payload.get("value_targets", {})
        JSONFilePersistenceStrategy._validate_targets(
            key, policy_targets, value_targets
        )

        # 3. Metadata
        entry = ExperienceEntry(
            moves=tuple(int(m) for m in moves),
            winner=winner,
            start_player=start_player,
            policy_targets={k: list(v) for k, v in policy_targets.items()},
            value_targets={k: float(v) for k, v in value_targets.items()},
            first_seen_step=JSONFilePersistenceStrategy._get_int(
                key, payload, "first_seen_step"
            ),
            last_seen_step=JSONFilePersistenceStrategy._get_int(
                key, payload, "last_seen_step"
            ),
            visit_count=JSONFilePersistenceStrategy._get_int(
                key, payload, "visit_count"
            ),
            priority_score=JSONFilePersistenceStrategy._get_float(
                key, payload, "priority_score"
            ),
        )

        if entry.get_hash() != key:
            raise DataError(
                f"Experience entry {key} failed hash verification",
                reason="Canonical hash mismatch",
                rationale="Experience storage requires stable identity hashes",
            )
        return entry

    @staticmethod
    def _validate_targets(
        key: str, policy_targets: object, value_targets: object
    ) -> None:
        if not isinstance(policy_targets, Mapping) or not all(
            isinstance(k, str) and isinstance(v, list)
            for k, v in policy_targets.items()
        ):
            raise DataError(
                f"Experience entry {key} has invalid policy_targets",
                reason="policy_targets must be mapping[str, list]",
                rationale="Experience storage annotates policy targets per depth",
            )
        if not isinstance(value_targets, Mapping) or not all(
            isinstance(k, str) and isinstance(v, (int, float))
            for k, v in value_targets.items()
        ):
            raise DataError(
                f"Experience entry {key} has invalid value_targets",
                reason="value_targets must be mapping[str, float]",
                rationale="Experience storage annotates value targets per depth",
            )

    @staticmethod
    def _get_int(key: str, payload: Mapping, field: str) -> int:
        value = payload.get(field, 0)
        if not isinstance(value, int):
            raise DataError(
                f"Experience entry {key} has non-integer {field}",
                reason="Metadata fields must be integers",
                rationale="Experience metadata tracks steps and counts as integers",
            )
        return value

    @staticmethod
    def _get_float(key: str, payload: Mapping, field: str) -> float:
        value = payload.get(field, 0.0)
        if not isinstance(value, (int, float)):
            raise DataError(
                f"Experience entry {key} has non-numeric {field}",
                reason="Metadata fields must be numeric",
                rationale="Experience metadata tracks priority as numeric values",
            )
        return float(value)


class ExperienceStorage(ABC):
    """Abstract base for experiment experience storage."""

    @abstractmethod
    def store(self, entry: ExperienceEntry) -> str:
        """Store an experience entry and return its canonical hash."""

    @abstractmethod
    def update(self, key: str, **kwargs: Any) -> None:
        """Update metadata for an existing entry."""

    @abstractmethod
    def get(self, key: str) -> ExperienceEntry | None:
        """Fetch an experience entry by canonical hash."""

    @abstractmethod
    def has(self, key: str) -> bool:
        """Return True if a keyed experience exists."""

    @abstractmethod
    def entries(self) -> Iterable[ExperienceEntry]:
        """Iterate over stored experiences."""

    @abstractmethod
    def get_by_priority(self, n: int) -> list[ExperienceEntry]:
        """Return the top n entries by priority score."""

    @abstractmethod
    def flush(self) -> None:
        """Persist state to the configured backend."""

    def __len__(self) -> int:
        return sum(1 for _ in self.entries())

    def __iter__(self) -> Iterator[ExperienceEntry]:
        return iter(self.entries())


class InMemoryExperienceStorage(ExperienceStorage):
    """In-memory storage with optional persistence and secondary indices."""

    def __init__(
        self,
        persistence: PersistenceStrategy | None = None,
        *,
        flush_on_store: bool = False,
    ) -> None:
        self._entries: dict[str, ExperienceEntry] = {}
        self._persistence = persistence
        self._flush_on_store = flush_on_store
        self._priority_index: list[str] = []  # Sorted keys by priority_score
        if persistence is not None:
            self._entries.update(persistence.load())
            self._rebuild_indices()

    def _rebuild_indices(self) -> None:
        """Rebuild secondary indices from scratch."""
        # pragma: no cover - rebuild logic is standard and covered by basic operations
        self._priority_index = sorted(
            self._entries.keys(),
            key=lambda k: self._entries[k].priority_score,
            reverse=True,
        )

    def store(self, entry: ExperienceEntry) -> str:
        key = entry.get_hash()
        is_new = key not in self._entries
        self._entries[key] = entry

        if is_new:
            # Simple insertion into sorted index could be optimized, but rebuild is safer for now
            self._rebuild_indices()

        if self._persistence is not None and self._flush_on_store:
            self.flush()
        return key

    def update(self, key: str, **kwargs: Any) -> None:
        if key not in self._entries:
            raise KeyError(f"Entry not found: {key}")
        entry = self._entries[key]

        # Check if priority score changed to decide if index needs rebuild
        old_priority = entry.priority_score
        new_priority = kwargs.get("priority_score", old_priority)

        # Create a new updated entry (ExperienceEntry is frozen)
        new_entry = ExperienceEntry(
            moves=entry.moves,
            winner=entry.winner,
            start_player=entry.start_player,
            policy_targets=kwargs.get("policy_targets", entry.policy_targets),
            value_targets=kwargs.get("value_targets", entry.value_targets),
            first_seen_step=kwargs.get("first_seen_step", entry.first_seen_step),
            last_seen_step=kwargs.get("last_seen_step", entry.last_seen_step),
            visit_count=kwargs.get("visit_count", entry.visit_count),
            priority_score=new_priority,
        )
        self._entries[key] = new_entry

        if old_priority != new_priority:
            self._rebuild_indices()

        if self._persistence is not None and self._flush_on_store:
            self.flush()

    def get_by_priority(self, n: int) -> list[ExperienceEntry]:
        """Return the top n entries by priority score."""
        return [self._entries[k] for k in self._priority_index[:n]]

    def get(self, key: str) -> ExperienceEntry | None:
        return self._entries.get(key)

    def has(self, key: str) -> bool:
        return key in self._entries

    def entries(self) -> Iterable[ExperienceEntry]:
        return self._entries.values()

    def __bool__(self) -> bool:
        """Return True if the storage contains any entries."""
        return len(self._entries) > 0

    def flush(self) -> None:
        if self._persistence is not None:
            self._persistence.save(self._entries)

    def load(self) -> None:
        """Explicitly reload from persistence."""
        if self._persistence is not None:
            self._entries.update(self._persistence.load())

    def clear(self) -> None:
        """Clear all entries from storage. Does NOT automatically flush."""
        self._entries.clear()


def build_experience_storage(config: "ExperienceStorageConfig") -> ExperienceStorage:
    """Factory to construct an experience storage backend from configuration."""

    persistence: PersistenceStrategy | None = None
    if config.strategy == "json_file":
        if config.path is None:
            raise ValueError(
                "experience storage path is required for strategy json_file"
            )
        persistence = JSONFilePersistenceStrategy(config.path)

    return InMemoryExperienceStorage(
        persistence=persistence,
        flush_on_store=config.flush_on_store,
    )
