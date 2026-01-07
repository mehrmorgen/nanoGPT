from __future__ import annotations

from typing import Protocol, runtime_checkable

from ml_playground.core.logging_protocol import LoggerLike
from ml_playground.core.protocols import (
    CheckpointManager,
    ExperienceStorage,
    MLflowClient,
    MLflowRun,
    OSModule,
    PersistenceStrategy,
    PlatformModule,
    Telemetry,
    Tokenizer as RuntimeTokenizer,
)
from ml_playground.core.tokenizer_protocol import Tokenizer


class _CompliantLogger:
    def __init__(self) -> None:
        self.messages: list[tuple[str, str]] = []

    def debug(self, msg: str, *args, **kwargs) -> None:
        self.messages.append(("debug", msg))

    def info(self, msg: str, *args, **kwargs) -> None:
        self.messages.append(("info", msg))

    def warning(self, msg: str, *args, **kwargs) -> None:
        self.messages.append(("warning", msg))

    def error(self, msg: str, *args, **kwargs) -> None:
        self.messages.append(("error", msg))


class _StubTokenizer:
    def __init__(self) -> None:
        self._vocab = {"a": 0}

    @property
    def name(self) -> str:
        return "stub"

    @property
    def vocab_size(self) -> int:
        return len(self._vocab)

    @property
    def vocab(self) -> dict[str, int]:
        return self._vocab

    def encode(self, text: str) -> list[int]:
        return [self._vocab[ch] for ch in text if ch in self._vocab]

    def decode(self, token_ids: list[int]) -> str:
        return "".join("a" if tid == 0 else "?" for tid in token_ids)


def test_logger_like_accepts_structural_loggers() -> None:
    logger = _CompliantLogger()
    assert isinstance(logger, LoggerLike)

    logger.info("hello")
    logger.error("oops")
    assert logger.messages == [("info", "hello"), ("error", "oops")]


def test_logger_like_rejects_missing_methods() -> None:
    class MissingWarnLogger:
        def debug(self, msg: str, *args, **kwargs) -> None:
            pass

        def info(self, msg: str, *args, **kwargs) -> None:
            pass

        def error(self, msg: str, *args, **kwargs) -> None:
            pass

    assert not isinstance(MissingWarnLogger(), LoggerLike)


def test_logger_like_protocol_placeholders_are_noops() -> None:
    sentinel = object()
    assert LoggerLike.debug(sentinel, "msg") is None  # type: ignore[arg-type]
    assert LoggerLike.info(sentinel, "msg") is None  # type: ignore[arg-type]
    assert LoggerLike.warning(sentinel, "msg") is None  # type: ignore[arg-type]
    assert LoggerLike.error(sentinel, "msg") is None  # type: ignore[arg-type]


@runtime_checkable
class _RuntimeTokenizer(Tokenizer, Protocol):
    """Runtime-checkable wrapper for test assertions."""


def test_tokenizer_protocol_accepts_full_implementation() -> None:
    tokenizer = _StubTokenizer()
    assert isinstance(tokenizer, _RuntimeTokenizer)
    assert tokenizer.name == "stub"
    assert tokenizer.vocab_size == 1
    assert tokenizer.encode("aa") == [0, 0]
    assert tokenizer.decode([0, 0]) == "aa"


def test_tokenizer_protocol_rejects_incomplete_implementation() -> None:
    class MissingDecodeTokenizer:
        @property
        def name(self) -> str:
            return "missing-decode"

        @property
        def vocab_size(self) -> int:
            return 1

        @property
        def vocab(self) -> dict[str, int]:
            return {"a": 0}

        def encode(self, text: str) -> list[int]:
            return [0 for _ in text]

    assert not isinstance(MissingDecodeTokenizer(), _RuntimeTokenizer)


def test_tokenizer_protocol_placeholders_execute_without_error() -> None:
    tokenizer = _StubTokenizer()

    assert Tokenizer.name.fget(tokenizer) is None
    assert Tokenizer.vocab_size.fget(tokenizer) is None
    assert Tokenizer.vocab.fget(tokenizer) is None
    assert Tokenizer.encode(tokenizer, "aa") is None  # type: ignore[arg-type]
    assert Tokenizer.decode(tokenizer, [0]) is None  # type: ignore[arg-type]


class _StubStorage:
    def __init__(self) -> None:
        self._entries: dict[str, object] = {}

    def store(self, entry: object) -> str:
        key = f"key-{len(self._entries)}"
        self._entries[key] = entry
        return key

    def get(self, key: str) -> object | None:
        return self._entries.get(key)

    def has(self, key: str) -> bool:
        return key in self._entries

    def entries(self):
        return iter(self._entries.values())

    def flush(self) -> None:
        return None

    def __len__(self) -> int:
        return len(self._entries)

    def __iter__(self):
        return iter(self._entries.values())


class _StubPersistence:
    def load(self) -> dict[str, object]:
        return {}

    def save(self, entries: dict[str, object]) -> None:
        del entries


class _StubTelemetry:
    def log_metric(self, name: str, value: float, step: int | None = None) -> None:
        del name, value, step

    def time_block(self, name: str):
        del name

        class _Ctx:
            def __enter__(self) -> None:
                return None

            def __exit__(self, *exc: object) -> None:
                return None

        return _Ctx()


class _StubMLflowRun:
    def __enter__(self) -> object:
        return object()

    def __exit__(self, *exc: object) -> bool | None:
        return None

    def __iter__(self):
        return iter(())


class _StubMLflowClient:
    def set_tracking_uri(self, _uri: str, /) -> None: ...

    def get_experiment_by_name(self, _name: str, /) -> object: ...

    def set_experiment(self, _experiment_name: str, /) -> object: ...

    def create_experiment(self, _name: str, /, **kwargs: object) -> str:
        del kwargs
        return "id"

    def start_run(self, **kwargs: object) -> _StubMLflowRun:
        del kwargs
        return _StubMLflowRun()

    def end_run(self) -> None: ...

    def log_params(self, _params: dict[str, object], /) -> None: ...

    def log_metrics(
        self, _metrics: dict[str, float], /, *, step: int | None = None
    ) -> None:
        del step

    def log_artifact(
        self, _local_path: str, /, *, artifact_path: str | None = None
    ) -> None:
        del artifact_path

    def log_artifacts(
        self, _local_dir: str, /, *, artifact_path: str | None = None
    ) -> None:
        del artifact_path

    def set_tag(self, _key: str, _value: object, /) -> None: ...


class _StubOS:
    def getcwd(self) -> str:
        return "/tmp"

    def getlogin(self) -> str:
        return "user"


class _StubPlatform:
    def platform(self) -> str:
        return "test-platform"

    def processor(self) -> str:
        return "test-cpu"


def test_runtime_protocols_accept_compliant_implementations() -> None:
    """Runtime protocol wrappers accept structural implementations."""
    assert isinstance(_StubStorage(), ExperienceStorage)
    assert isinstance(_StubPersistence(), PersistenceStrategy)
    assert isinstance(_StubTelemetry(), Telemetry)
    assert isinstance(_StubMLflowRun(), MLflowRun)
    assert isinstance(_StubMLflowClient(), MLflowClient)
    assert isinstance(_StubOS(), OSModule)
    assert isinstance(_StubPlatform(), PlatformModule)


def test_runtime_protocol_placeholders_execute_without_error() -> None:
    """Protocol placeholders execute without raising for coverage."""
    sentinel = object()

    assert RuntimeTokenizer.name.fget(sentinel) is None  # type: ignore[arg-type]
    assert RuntimeTokenizer.vocab_size.fget(sentinel) is None  # type: ignore[arg-type]
    assert RuntimeTokenizer.vocab.fget(sentinel) is None  # type: ignore[arg-type]
    assert RuntimeTokenizer.encode(sentinel, "text") is None  # type: ignore[arg-type]
    assert RuntimeTokenizer.decode(sentinel, [0]) is None  # type: ignore[arg-type]

    assert PersistenceStrategy.load(sentinel) is None  # type: ignore[arg-type]
    assert PersistenceStrategy.save(sentinel, {}) is None  # type: ignore[arg-type]

    assert ExperienceStorage.store(sentinel, object()) is None  # type: ignore[arg-type]
    assert ExperienceStorage.get(sentinel, "k") is None  # type: ignore[arg-type]
    assert ExperienceStorage.has(sentinel, "k") is None  # type: ignore[arg-type]
    assert ExperienceStorage.entries(sentinel) is None  # type: ignore[arg-type]
    assert ExperienceStorage.flush(sentinel) is None  # type: ignore[arg-type]
    assert ExperienceStorage.__len__(sentinel) is None  # type: ignore[arg-type]
    assert ExperienceStorage.__iter__(sentinel) is None  # type: ignore[arg-type]

    assert CheckpointManager.save(sentinel) is None  # type: ignore[arg-type]
    assert CheckpointManager.load(sentinel) is None  # type: ignore[arg-type]
    assert CheckpointManager.get_latest_path(sentinel) is None  # type: ignore[arg-type]
    assert CheckpointManager.get_best_path(sentinel) is None  # type: ignore[arg-type]

    assert Telemetry.log_metric(sentinel, "m", 1.0) is None  # type: ignore[arg-type]
    assert Telemetry.time_block(sentinel, "t") is None  # type: ignore[arg-type]

    assert MLflowRun.__enter__(sentinel) is None  # type: ignore[arg-type]
    assert MLflowRun.__exit__(sentinel) is None  # type: ignore[arg-type]
    assert MLflowRun.__iter__(sentinel) is None  # type: ignore[arg-type]

    assert MLflowClient.set_tracking_uri(sentinel, "uri") is None  # type: ignore[arg-type]
    assert MLflowClient.get_experiment_by_name(sentinel, "name") is None  # type: ignore[arg-type]
    assert MLflowClient.set_experiment(sentinel, "exp") is None  # type: ignore[arg-type]
    assert MLflowClient.create_experiment(sentinel, "exp") is None  # type: ignore[arg-type]
    assert MLflowClient.start_run(sentinel) is None  # type: ignore[arg-type]
    assert MLflowClient.end_run(sentinel) is None  # type: ignore[arg-type]
    assert MLflowClient.log_params(sentinel, {}) is None  # type: ignore[arg-type]
    assert MLflowClient.log_metrics(sentinel, {}) is None  # type: ignore[arg-type]
    assert MLflowClient.log_artifact(sentinel, "path") is None  # type: ignore[arg-type]
    assert MLflowClient.log_artifacts(sentinel, "path") is None  # type: ignore[arg-type]
    assert MLflowClient.set_tag(sentinel, "k", "v") is None  # type: ignore[arg-type]

    assert OSModule.getcwd(sentinel) is None  # type: ignore[arg-type]
    assert OSModule.getlogin(sentinel) is None  # type: ignore[arg-type]
    assert PlatformModule.platform(sentinel) is None  # type: ignore[arg-type]
    assert PlatformModule.processor(sentinel) is None  # type: ignore[arg-type]
