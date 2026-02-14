from __future__ import annotations

import hashlib
import json
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from pathlib import Path
from typing import (
    Callable,
    Dict,
    Iterable,
    Iterator,
    List,
    Mapping,
    Tuple,
    TYPE_CHECKING,
    cast,
)

from ml_playground.framework.core.error_handling import DataError

if TYPE_CHECKING:
    from ml_playground.framework.configuration.models import ExperienceStorageConfig
__all__ = [
    "ExperienceEntry",
    "ExperienceStorage",
    "InMemoryExperienceStorage",
    "PersistenceStrategy",
    "JSONFilePersistenceStrategy",
    "build_experience_storage",
]


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

    def __init__(
        self, path: Path, loader: Callable[[str], Mapping[str, object]] | None = None
    ) -> None:
        self._path = path
        self._loader = loader or json.loads

    def load(self) -> Mapping[str, ExperienceEntry]:
        if not self._path.exists():
            return {}
        try:
            payload_obj = self._path.read_text(encoding="utf-8")
            payload = self._loader(payload_obj)
        except (OSError, json.JSONDecodeError, TypeError) as exc:
            raise DataError(
                f"Failed to read experience storage from {self._path}: {exc}",
                reason="Unable to decode persisted experience store",
                rationale="Experience storage requires a valid JSON mapping of hash -> entry payloads",
            ) from exc

        if not isinstance(payload, Mapping):
            raise DataError(
                f"Experience storage at {self._path} must be a mapping",
                reason="Persisted payload is not a mapping",
                rationale="Experience storage expects JSON object with hash keys",
            )

        typed_payload = cast(Mapping[str, object], payload)
        entries: dict[str, ExperienceEntry] = {}
        for key, raw_value in typed_payload.items():
            entries[key] = self._decode_entry(key, raw_value)
        return entries

    def save(self, entries: Mapping[str, ExperienceEntry]) -> None:
        serializable: dict[str, object] = {}
        raw_entries = cast(Mapping[object, object], entries)
        for key_obj, entry_obj in raw_entries.items():
            if not isinstance(key_obj, str):
                raise DataError(
                    f"Experience storage keys must be strings, got {key_obj!r}",
                    reason="Non-string key",
                    rationale="Experience storage keys are canonical hashes represented as strings",
                )
            if not isinstance(entry_obj, ExperienceEntry):
                raise DataError(
                    f"Invalid entry type for {key_obj}: {type(entry_obj).__name__}",
                    reason="Entries must be ExperienceEntry instances",
                    rationale="Experience storage persists canonical ExperienceEntry payloads",
                )
            serializable[key_obj] = self._encode_entry(entry_obj)
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
    def _encode_entry(entry: ExperienceEntry) -> dict[str, object]:
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
                f"Invalid entry value for {key}: {payload}",
                reason="Entry payload must be a mapping",
                rationale="Experience storage entries are stored as dicts",
            )
        payload_map = cast(Mapping[str, object], payload)
        # 1. Core game state
        try:
            moves = payload_map["moves"]
            winner = payload_map["winner"]
            start_player = payload_map["start_player"]
        except KeyError as exc:
            raise DataError(
                f"Experience entry {key} is missing required field {exc.args[0]}",
                reason="Missing required entry field",
                rationale="Experience storage requires moves, winner, and start_player",
            ) from exc

        if not isinstance(moves, list) or not all(
            isinstance(m, int) for m in cast(list[object], moves)
        ):
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
        policy_targets_obj = payload_map.get("policy_targets", {})
        value_targets_obj = payload_map.get("value_targets", {})
        policy_targets, value_targets = JSONFilePersistenceStrategy._validate_targets(
            key,
            policy_targets_obj,
            value_targets_obj,
        )

        # 3. Metadata
        typed_moves = cast(list[int], moves)
        entry = ExperienceEntry(
            moves=tuple(typed_moves),
            winner=winner,
            start_player=start_player,
            policy_targets={k: list(v) for k, v in policy_targets.items()},
            value_targets={k: v for k, v in value_targets.items()},
            first_seen_step=JSONFilePersistenceStrategy._get_int(
                key, payload_map, "first_seen_step"
            ),
            last_seen_step=JSONFilePersistenceStrategy._get_int(
                key, payload_map, "last_seen_step"
            ),
            visit_count=JSONFilePersistenceStrategy._get_int(
                key, payload_map, "visit_count"
            ),
            priority_score=JSONFilePersistenceStrategy._get_float(
                key, payload_map, "priority_score"
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
        key: str,
        policy_targets: object,
        value_targets: object,
    ) -> tuple[Dict[str, list[int]], Dict[str, float]]:
        if not isinstance(policy_targets, Mapping):
            raise DataError(
                f"Experience entry {key} has invalid policy_targets",
                reason="policy_targets must be mapping[str, list]",
                rationale="Experience storage annotates policy targets per depth",
            )
        if not isinstance(value_targets, Mapping):
            raise DataError(
                f"Experience entry {key} has invalid value_targets",
                reason="value_targets must be mapping[str, float]",
                rationale="Experience storage annotates value targets per depth",
            )
        policy_targets_map = cast(Mapping[str, object], policy_targets)
        value_targets_map = cast(Mapping[str, object], value_targets)
        typed_policy_targets: dict[str, list[int]] = {}
        for raw_key, raw_value in policy_targets_map.items():
            if not isinstance(raw_value, list):
                raise DataError(
                    f"Experience entry {key} has invalid policy_targets",
                    reason="policy_targets must be mapping[str, list]",
                    rationale="Experience storage annotates policy targets per depth",
                )
            validated_list: list[int] = []
            for element in cast(list[object], raw_value):
                if not isinstance(element, int):
                    raise DataError(
                        f"Experience entry {key} has invalid policy_targets",
                        reason="policy_targets must be mapping[str, list[int]]",
                        rationale="Experience storage annotates policy targets per depth",
                    )
                validated_list.append(element)
            typed_policy_targets[str(raw_key)] = validated_list

        typed_value_targets: dict[str, float] = {}
        for raw_key, raw_value in value_targets_map.items():
            if not isinstance(raw_value, (int, float)):
                raise DataError(
                    f"Experience entry {key} has invalid value_targets",
                    reason="value_targets must be mapping[str, float]",
                    rationale="Experience storage annotates value targets per depth",
                )
            typed_value_targets[raw_key] = float(raw_value)
        return typed_policy_targets, typed_value_targets

    @staticmethod
    def _get_int(key: str, payload: Mapping[str, object], field: str) -> int:
        value = payload.get(field, 0)
        if not isinstance(value, int):
            raise DataError(
                f"Experience entry {key} has non-integer {field}",
                reason="Metadata fields must be integers",
                rationale="Experience metadata tracks steps and counts as integers",
            )
        return value

    @staticmethod
    def _get_float(key: str, payload: Mapping[str, object], field: str) -> float:
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
    def update(self, key: str, **kwargs: object) -> None:
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

    def update(self, key: str, **kwargs: object) -> None:
        if key not in self._entries:
            raise KeyError(f"Entry not found: {key}")
        entry = self._entries[key]

        # Check if priority score changed to decide if index needs rebuild
        old_priority = entry.priority_score
        priority_obj = kwargs.get("priority_score")
        new_priority = (
            float(priority_obj)
            if isinstance(priority_obj, (int, float))
            else old_priority
        )

        def _coerce_int(name: str, default: int) -> int:
            value = kwargs.get(name)
            if isinstance(value, (int, float)):
                return int(value)
            return default

        visit_count = _coerce_int("visit_count", entry.visit_count)
        first_seen_step = _coerce_int("first_seen_step", entry.first_seen_step)
        last_seen_step = _coerce_int("last_seen_step", entry.last_seen_step)

        policy_targets = cast(
            Dict[str, List[int]],
            kwargs.get("policy_targets", entry.policy_targets),
        )
        value_targets = cast(
            Dict[str, float],
            kwargs.get("value_targets", entry.value_targets),
        )

        # Create a new updated entry (ExperienceEntry is frozen)
        new_entry = ExperienceEntry(
            moves=entry.moves,
            winner=entry.winner,
            start_player=entry.start_player,
            policy_targets=policy_targets,
            value_targets=value_targets,
            first_seen_step=first_seen_step,
            last_seen_step=last_seen_step,
            visit_count=visit_count,
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
            self._entries = dict(self._persistence.load())
            self._rebuild_indices()

    def clear(self) -> None:
        """Clear all entries from storage. Does NOT automatically flush."""
        self._entries.clear()


def build_experience_storage(config: ExperienceStorageConfig) -> ExperienceStorage:
    """Factory to construct an experience storage backend from configuration."""
    persistence: PersistenceStrategy | None = None
    if config.strategy == "json_file":
        if config.path is None:
            raise ValueError(
                "experience storage path is required for strategy json_file"
            )
        path = Path(config.path)
        persistence = JSONFilePersistenceStrategy(path)

    return InMemoryExperienceStorage(
        persistence=persistence,
        flush_on_store=config.flush_on_store,
    )
