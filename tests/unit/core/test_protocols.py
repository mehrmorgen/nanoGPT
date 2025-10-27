from __future__ import annotations

from typing import Any, Protocol, runtime_checkable, cast

from ml_playground.core.logging_protocol import LoggerLike
from ml_playground.core.tokenizer_protocol import Tokenizer


class _CompliantLogger:
    def __init__(self) -> None:
        self.messages: list[tuple[str, str]] = []

    def debug(self, msg: str, *args: object, **kwargs: object) -> None:
        self.messages.append(("debug", msg))

    def info(self, msg: str, *args: object, **kwargs: object) -> None:
        self.messages.append(("info", msg))

    def warning(self, msg: str, *args: object, **kwargs: object) -> None:
        self.messages.append(("warning", msg))

    def error(self, msg: str, *args: object, **kwargs: object) -> None:
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
        def debug(self, msg: str, *args: object, **kwargs: object) -> None:
            pass

        def info(self, msg: str, *args: object, **kwargs: object) -> None:
            pass

        def error(self, msg: str, *args: object, **kwargs: object) -> None:
            pass

    assert not isinstance(MissingWarnLogger(), LoggerLike)


def test_logger_like_protocol_placeholders_are_noops() -> None:
    sentinel: Any = object()
    primitive = cast(LoggerLike, sentinel)
    assert getattr(LoggerLike, "debug")(primitive, "msg") is None
    assert getattr(LoggerLike, "info")(primitive, "msg") is None
    assert getattr(LoggerLike, "warning")(primitive, "msg") is None
    assert getattr(LoggerLike, "error")(primitive, "msg") is None


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

    assert _RuntimeTokenizer.name.fget(tokenizer) is None  # type: ignore[attr-defined]
    assert _RuntimeTokenizer.vocab_size.fget(tokenizer) is None  # type: ignore[attr-defined]
    assert _RuntimeTokenizer.vocab.fget(tokenizer) is None  # type: ignore[attr-defined]
    assert _RuntimeTokenizer.encode(tokenizer, "aa") is None  # type: ignore[arg-type]
    assert _RuntimeTokenizer.decode(tokenizer, [0]) is None  # type: ignore[arg-type]
