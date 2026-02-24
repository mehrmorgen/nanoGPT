from __future__ import annotations

from pathlib import Path
from typing import (
    Any,
    ContextManager,
    Dict,
    Iterable,
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
    "MLflowRun",
    "MLflowClient",
    "OSModule",
    "PlatformModule",
    "SysModule",
    "ConfigSectionExtractor",
    "JsonParser",
    "ModuleImporter",
    "TestResultExtractor",
    "CoverageDataExtractor",
    "LitDataset",
    "LitDatasetModule",
    "LitModel",
    "LitModelModule",
    "LitTypesModule",
]


@runtime_checkable
class Tokenizer(Protocol):
    """Unified interface for all tokenizers in ml_playground."""

    @property
    def name(self) -> str:
        """Name of the tokenizer."""
        ...

    @property
    def vocab_size(self) -> int:
        """Number of tokens in the vocabulary."""
        ...

    @property
    def vocab(self) -> Mapping[str, int]:
        """Mapping from tokens to integer ids."""
        ...

    def encode(self, text: str) -> list[TokenId]:
        """Encode text into a sequence of token ids."""
        ...

    def decode(self, token_ids: Sequence[TokenId]) -> str:
        """Decode a sequence of token ids into text."""
        ...


TokenizerKind = Literal["char", "word", "tiktoken"]


@runtime_checkable
class PersistenceStrategy(Protocol):
    """Protocol for experience storage persistence strategies."""

    def load(self) -> Mapping[str, Any]:
        """Load stored experiences."""
        ...

    def save(self, entries: Mapping[str, Any]) -> None:
        """Save experiences to the backend."""
        _ = entries
        ...


@runtime_checkable
class ExperienceStorage(Protocol):
    """Protocol for experience storage backends."""

    def store(self, entry: object) -> str:
        """Store an experience entry."""
        ...

    def get(self, key: str) -> Optional[Any]:
        """Fetch an entry by key."""
        ...

    def has(self, key: str) -> bool:
        """Check if an entry exists."""
        ...

    def entries(self) -> Iterator[object]:
        """Iterate over all entries."""
        ...

    def flush(self) -> None:
        """Persist pending changes."""
        ...

    def __len__(self) -> int: ...

    def __iter__(self) -> Iterator[object]: ...


@runtime_checkable
class CheckpointManager(Protocol):
    """Protocol for managing experiment checkpoints."""

    def save(self, *args: object, **kwargs: object) -> None:
        """Save a checkpoint."""
        ...

    def load(self, *args: object, **kwargs: object) -> Optional[object]:
        """Load a checkpoint."""
        ...

    def get_latest_path(self) -> Optional[Path]:
        """Get path to the latest checkpoint."""
        ...

    def get_best_path(self) -> Optional[Path]:
        """Get path to the best checkpoint."""
        ...


@runtime_checkable
class Telemetry(Protocol):
    """Protocol for experiment telemetry and performance hooks."""

    def log_metric(self, name: str, value: float, step: int | None = None) -> None:
        """Log a numerical metric."""
        ...

    def time_block(self, name: str) -> ContextManager[Any]:
        """Context manager to time a block of code."""
        ...


@runtime_checkable
class MLflowRun(Protocol):
    """Minimal MLflow run handle."""

    def __enter__(self) -> object: ...

    def __exit__(self, *exc: object) -> bool | None: ...

    def __iter__(self) -> Iterator[object]: ...


@runtime_checkable
class MLflowClient(Protocol):
    """Protocol describing the subset of MLflow we depend on.

    This keeps the integration testable and type-checkable without relying on
    MLflow's dynamic runtime types.
    """

    def set_tracking_uri(self, _uri: str, /) -> None: ...

    def get_experiment_by_name(self, _name: str, /) -> object: ...

    def set_experiment(self, _experiment_name: str, /) -> object: ...

    def create_experiment(self, _name: str, /, **kwargs: object) -> str: ...

    def start_run(self, **kwargs: object) -> MLflowRun: ...

    def end_run(self) -> None: ...

    def log_params(self, _params: Mapping[str, object], /) -> None: ...

    def log_metrics(
        self, _metrics: Dict[str, float], /, *, step: Optional[int] = None
    ) -> None: ...

    def log_artifact(
        self, _local_path: str, /, *, artifact_path: Optional[str] = None
    ) -> None:
        _ = artifact_path
        ...

    def log_artifacts(
        self, _local_dir: str, /, *, artifact_path: Optional[str] = None
    ) -> None:
        _ = artifact_path
        ...

    def set_tag(self, _key: str, _value: object, /) -> None: ...

    def log_text(self, _text: str, _artifact_file: str, /) -> None: ...


@runtime_checkable
class OSModule(Protocol):
    def getcwd(self) -> str: ...

    def getlogin(self) -> str: ...


@runtime_checkable
class PlatformModule(Protocol):
    def platform(self) -> str: ...

    def processor(self) -> str: ...


@runtime_checkable
class SysModule(Protocol):
    version: str
    argv: list[str]


@runtime_checkable
class ConfigSectionExtractor(Protocol):
    """Protocol for extracting nested configuration sections."""

    def extract_section(
        self, config: Mapping[str, object], section: str
    ) -> Mapping[str, object]: ...

    def get_string(
        self, mapping: Mapping[str, object], key: str, default: str
    ) -> str: ...


@runtime_checkable
class JsonParser(Protocol):
    """Protocol for parsing JSON content with typed returns."""

    def parse_json(self, content: str) -> Mapping[str, object]: ...

    def parse_gate_snapshot(self, content: str) -> object: ...

    def parse_github_response(self, content: str) -> dict[str, object]: ...


@runtime_checkable
class ModuleImporter(Protocol):
    """Protocol for importing LIT modules."""

    def import_dataset_module(self) -> object: ...

    def import_model_module(self) -> object: ...

    def import_types_module(self) -> object: ...
    def import_api_module(self) -> object: ...


@runtime_checkable
class TestResultExtractor(Protocol):
    """Protocol for extracting test results."""

    def extract_overall(self, results: dict[str, object]) -> dict[str, object]: ...

    def extract_status(self, section: dict[str, object]) -> str: ...


@runtime_checkable
class CoverageDataExtractor(Protocol):
    """Protocol for extracting coverage data."""

    def extract_totals(self, coverage_data: dict[str, object]) -> dict[str, object]: ...

    def get_coverage_percent(self, totals: dict[str, object]) -> float: ...


@runtime_checkable
class LitDataset(Protocol):
    def spec(self) -> dict[str, object]: ...

    def __len__(self) -> int: ...

    def __iter__(self) -> Iterator[Mapping[str, object]]: ...


@runtime_checkable
class LitDatasetModule(Protocol):
    Dataset: type[LitDataset]


@runtime_checkable
class LitModel(Protocol):
    def input_spec(self) -> dict[str, object]: ...

    def output_spec(self) -> dict[str, object]: ...

    def predict(
        self, _inputs: Iterable[Mapping[str, object]], **kwargs: object
    ) -> list[Mapping[str, object]]: ...


@runtime_checkable
class LitModelModule(Protocol):
    Model: type[LitModel]


@runtime_checkable
class LitTypesModule(Protocol):
    def TextSegment(self) -> object: ...
