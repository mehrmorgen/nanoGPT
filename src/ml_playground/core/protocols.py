from __future__ import annotations  # pragma: no cover

from pathlib import Path
from typing import (
    Any,
    Iterator,
    Literal,
    Mapping,
    MutableMapping,
    Optional,
    Protocol,
    Sequence,
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
    "TokenizerKind",
    "Tokenizer",
    "PersistenceStrategy",
    "ExperienceStorage",
    "CheckpointManager",
    "Telemetry",
]


@runtime_checkable
class Tokenizer(Protocol):
    """Unified interface for all tokenizers in ml_playground."""

    @property
    def name(self) -> str:
        """Name of the tokenizer."""
        ...  # pragma: no cover

    @property
    def vocab_size(self) -> int:
        """Number of tokens in the vocabulary."""
        ...  # pragma: no cover

    @property
    def vocab(self) -> Mapping[str, int]:
        """Mapping from tokens to integer ids."""
        ...  # pragma: no cover

    def encode(self, text: str) -> list[TokenId]:
        """Encode text into a sequence of token ids."""
        ...  # pragma: no cover

    def decode(self, token_ids: Sequence[TokenId]) -> str:
        """Decode a sequence of token ids into text."""
        ...  # pragma: no cover


TokenizerKind = Literal["char", "word", "tiktoken"]


@runtime_checkable
class PersistenceStrategy(Protocol):
    """Protocol for experience storage persistence strategies."""

    def load(self) -> Mapping[str, Any]:
        """Load stored experiences."""
        ...  # pragma: no cover

    def save(self, entries: Mapping[str, Any]) -> None:
        """Save experiences to the backend."""
        ...  # pragma: no cover


@runtime_checkable
class ExperienceStorage(Protocol):
    """Protocol for experience storage backends."""

    def store(self, entry: Any) -> str:
        """Store an experience entry."""
        ...  # pragma: no cover

    def get(self, key: str) -> Optional[Any]:
        """Fetch an entry by key."""
        ...  # pragma: no cover

    def has(self, key: str) -> bool:
        """Check if an entry exists."""
        ...  # pragma: no cover

    def entries(self) -> Iterator[Any]:
        """Iterate over all entries."""
        ...  # pragma: no cover

    def flush(self) -> None:
        """Persist pending changes."""
        ...  # pragma: no cover

    def __len__(self) -> int: ...  # pragma: no cover

    def __iter__(self) -> Iterator[Any]: ...  # pragma: no cover


@runtime_checkable
class CheckpointManager(Protocol):
    """Protocol for managing experiment checkpoints."""

    def save(self, *args: Any, **kwargs: Any) -> None:
        """Save a checkpoint."""
        ...  # pragma: no cover

    def load(self, *args: Any, **kwargs: Any) -> Optional[Any]:
        """Load a checkpoint."""
        ...  # pragma: no cover

    def get_latest_path(self) -> Optional[Path]:
        """Get path to the latest checkpoint."""
        ...  # pragma: no cover

    def get_best_path(self) -> Optional[Path]:
        """Get path to the best checkpoint."""
        ...  # pragma: no cover


@runtime_checkable
class Telemetry(Protocol):
    """Protocol for experiment telemetry and performance hooks."""

    def log_metric(self, name: str, value: float, step: int | None = None) -> None:
        """Log a numerical metric."""
        ...  # pragma: no cover

    def time_block(self, name: str) -> Any:
        """Context manager to time a block of code."""
        ...  # pragma: no cover
