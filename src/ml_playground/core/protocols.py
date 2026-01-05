from __future__ import annotations  # pragma: no cover

from pathlib import Path
from typing import (
    Any,
    Iterator,
    Mapping,
    MutableMapping,
    Optional,
    Protocol,
    TypeVar,
    runtime_checkable,
)

T = TypeVar("T")
TokenId = int
TokenSequence = list[TokenId]
TokenMapping = Mapping[str, TokenId]
MutableTokenMapping = MutableMapping[str, TokenId]

__all__ = [
    "TokenId",
    "TokenSequence",
    "TokenMapping",
    "MutableTokenMapping",
    "Tokenizer",
    "PersistenceStrategy",
    "ExperienceStorage",
    "CheckpointManager",
]


@runtime_checkable
class Tokenizer(Protocol):  # pragma: no cover - Protocol definition only
    """Unified interface for all tokenizers in ml_playground."""

    @property
    def name(self) -> str: ...  # pragma: no cover - Protocol signature

    @property
    def vocab_size(self) -> int: ...  # pragma: no cover - Protocol signature

    @property
    def vocab(
        self,
    ) -> Mapping[str, TokenId]: ...  # pragma: no cover - Protocol signature

    def encode(
        self, text: str
    ) -> list[TokenId]: ...  # pragma: no cover - Protocol signature
    def decode(
        self, token_ids: TokenSequence
    ) -> str: ...  # pragma: no cover - Protocol signature


@runtime_checkable
class PersistenceStrategy(Protocol):  # pragma: no cover - Protocol definition only
    """Protocol for experience storage persistence backends."""

    def load(self) -> Mapping[str, Any]: ...  # pragma: no cover - Protocol signature
    def save(
        self, entries: Mapping[str, Any]
    ) -> None: ...  # pragma: no cover - Protocol signature


@runtime_checkable
class ExperienceStorage(Protocol):  # pragma: no cover - Protocol definition only
    """Protocol for experiment experience storage."""

    def store(self, entry: Any) -> str: ...  # pragma: no cover - Protocol signature
    def get(
        self, key: str
    ) -> Optional[Any]: ...  # pragma: no cover - Protocol signature
    def has(self, key: str) -> bool: ...  # pragma: no cover - Protocol signature
    def entries(self) -> Iterator[Any]: ...  # pragma: no cover - Protocol signature
    def flush(self) -> None: ...  # pragma: no cover - Protocol signature
    def __len__(self) -> int: ...  # pragma: no cover - Protocol signature
    def __iter__(self) -> Iterator[Any]: ...  # pragma: no cover - Protocol signature


@runtime_checkable
class CheckpointManager(Protocol):  # pragma: no cover - Protocol definition only
    """Protocol for managing experiment checkpoints."""

    def save(
        self, *args: Any, **kwargs: Any
    ) -> None: ...  # pragma: no cover - Protocol signature
    def load(
        self, *args: Any, **kwargs: Any
    ) -> Optional[Any]: ...  # pragma: no cover - Protocol signature
    def get_latest_path(
        self,
    ) -> Optional[Path]: ...  # pragma: no cover - Protocol signature
    def get_best_path(
        self,
    ) -> Optional[Path]: ...  # pragma: no cover - Protocol signature
