from typing import Any, Literal, cast
from collections.abc import Mapping
from types import MappingProxyType

import numpy as np
import numpy.typing as npt
import pytest

from ml_playground.core.tokenizer import (
    CharTokenizer,
    TiktokenTokenizer,
    WordTokenizer,
    create_tokenizer,
)


class CharTokenizerTestHarness(CharTokenizer):
    """Test harness exposing lookup array maintenance for char tokenizer."""

    def invalidate_lookup_array(self) -> None:
        if hasattr(self, "_itos_array"):
            delattr(self, "_itos_array")

    def expose_lookup_array(self) -> npt.NDArray[np.object_]:
        return self._ensure_lookup_array()

    def lookup_array_length(self) -> int:
        return self._ensure_lookup_array().shape[0]


class WordTokenizerTestHarness(WordTokenizer):
    """Test harness exposing lookup array maintenance for word tokenizer."""

    def invalidate_lookup_array(self) -> None:
        if hasattr(self, "_itos_array"):
            delattr(self, "_itos_array")

    def expose_lookup_array(self) -> npt.NDArray[np.object_]:
        return self._ensure_lookup_array()

    def lookup_array_length(self) -> int:
        return self._ensure_lookup_array().shape[0]


def test_char_tokenizer_roundtrip_proto() -> None:
    """Round-trips a small char vocab including spaces."""
    vocab = {"a": 1, "b": 2, " ": 3}
    tk = CharTokenizer(vocab=vocab)
    assert tk.name == "char"
    assert tk.vocab_size == 3
    text = "ab a"
    ids = tk.encode(text)
    assert ids == [1, 2, 3, 1]
    back = tk.decode(ids)
    assert back == text
    # vocab mapping should be read-only
    v = tk.vocab
    assert isinstance(v, MappingProxyType)
    with pytest.raises(TypeError):
        cast(dict[str, int], v)["c"] = 3


def test_char_tokenizer_decode_rebuilds_lookup_array() -> None:
    """Rebuilds lookup array when vocabulary grows dynamically."""
    tok = CharTokenizer({"a": 1})
    assert tok.decode([1]) == "a"
    tok.itos[2] = "b"
    assert tok.decode([2]) == "b"
    assert tok.decode([-1, 3]) == ""


def test_word_tokenizer_decode_strips_invalid_ids_and_exposes_vocab_proxy() -> None:
    """Drops out-of-range ids and enforces read-only vocab mapping."""
    tok = WordTokenizer({"hello": 1, "world": 2})
    assert tok.decode([1, 2]) == "hello world"
    assert tok.decode([-5, 999]) == ""
    vocab_proxy = tok.vocab
    assert isinstance(vocab_proxy, MappingProxyType)
    with pytest.raises(TypeError):
        cast(dict[str, int], vocab_proxy)["new"] = 3


def test_word_tokenizer_roundtrip() -> None:
    """Ensures simple word tokenizer encode/decode symmetry."""
    tok = WordTokenizer({"hello": 1, "world": 2})
    ids = tok.encode("hello world!")
    assert ids == [1, 2, 0]


def test_word_tokenizer_roundtrip_proto() -> None:
    """Exercises protocol-based round trip plus metadata checks."""
    vocab = {"Hello": 1, ",": 2, "world": 3, "!": 4}
    tk = WordTokenizer(vocab=vocab)
    assert tk.name == "word"
    # tokenization preserves punctuation as separate tokens
    ids = tk.encode("Hello, world!")
    # decode joins with spaces by design
    assert tk.decode(ids) == "Hello , world !"
    # additional property checks to catch decorator removals
    v = tk.vocab
    assert isinstance(v, Mapping)


def test_char_tokenizer_rebuilds_missing_lookup_array() -> None:
    """Test char tokenizer rebuilds missing lookup array."""
    tok = CharTokenizerTestHarness({"a": 0})
    tok.invalidate_lookup_array()
    assert tok.decode([0]) == "a"


def test_word_tokenizer_rebuilds_missing_lookup_array() -> None:
    """Test word tokenizer rebuilds missing lookup array."""
    tok = WordTokenizerTestHarness({"hello": 0})
    tok.invalidate_lookup_array()
    assert tok.decode([0]) == "hello"


@pytest.mark.parametrize(
    ("tok_type", "kwargs", "expected_cls"),
    [
        (cast(Literal["char", "word"], "char"), {"vocab": {"x": 1}}, CharTokenizer),
        (cast(Literal["char", "word"], "word"), {"vocab": {"x": 1}}, WordTokenizer),
    ],
)
def test_create_tokenizer_factory_char_word_proto(
    tok_type: Literal["char", "word"], kwargs: dict[str, Any], expected_cls: type
) -> None:
    """Factory returns expected tokenizer subclass for char/word."""
    tk = create_tokenizer(tok_type, **kwargs)
    assert isinstance(tk, expected_cls)


def test_create_tokenizer_factory_unknown_proto() -> None:
    """Factory raises ValueError for unknown tokenizer names."""
    with pytest.raises(ValueError):
        create_tokenizer(cast(Any, "nope"))


def test_tiktoken_tokenizer_properties_with_fake_module() -> None:
    """Provide a fake tiktoken module to validate TiktokenTokenizer properties without installing tiktoken."""

    class FakeEncoder:
        def __init__(self) -> None:
            self.n_vocab = 3
            self._mergeable_ranks = {"a": 1, "b": 2, "c": 3}

        def encode(
            self, text: str, allowed_special: set[str] | None = None
        ) -> list[int]:
            return [1, 2]

        def decode(self, ids: list[int]) -> str:
            return "ab"

    class FakeTiktokenModule:
        @staticmethod
        def get_encoding(name: str) -> FakeEncoder:
            return FakeEncoder()

    tk = TiktokenTokenizer(loader=lambda: FakeTiktokenModule)
    assert tk.name == "tiktoken"
    assert tk.vocab_size == 3
    assert tk.decode(tk.encode("hi")) == "ab"
    v = tk.vocab
    # Mapping with expected keys
    assert hasattr(v, "__getitem__") and "a" in v and v["a"] == 1


def test_tiktoken_tokenizer_handles_missing_mergeable_ranks() -> None:
    """When encoder lacks mergeable ranks mapping, tokenizer should expose empty mapping."""

    class Encoder:
        n_vocab = 1
        _mergeable_ranks = None

        def encode(
            self, text: str, allowed_special: set[str] | None = None
        ) -> list[int]:
            return []

        def decode(self, ids: list[int]) -> str:
            return ""

    class Module:
        @staticmethod
        def get_encoding(name: str) -> Encoder:
            return Encoder()

    tk = TiktokenTokenizer(loader=lambda: Module)
    assert tk.vocab == MappingProxyType({})


def test_tiktoken_tokenizer_import_error_is_propagated() -> None:
    """Loader ImportError should be surfaced with helpful message."""

    def loader() -> None:
        raise ImportError("missing dependency")

    with pytest.raises(ImportError) as exc:
        TiktokenTokenizer(loader=loader)

    assert "tiktoken is required" in str(exc.value)


def test_char_tokenizer_decode_empty_vocab_returns_empty_string() -> None:
    """Test char tokenizer decode empty vocab returns empty string."""
    tok = CharTokenizer()
    assert tok.decode([0, 1]) == ""


def test_word_tokenizer_decode_empty_vocab_returns_empty_string() -> None:
    """Test word tokenizer decode empty vocab returns empty string."""
    tok = WordTokenizer()
    assert tok.decode([0, 1]) == ""


def test_char_tokenizer_ignores_negative_vocab_indices() -> None:
    """Negative vocab indices are ignored in lookup arrays."""
    tok = CharTokenizer({"a": -1})
    assert tok.decode([-1]) == ""


def test_word_tokenizer_ignores_negative_vocab_indices() -> None:
    """Negative vocab indices are ignored in lookup arrays."""
    tok = WordTokenizer({"hello": -1})
    assert tok.decode([-1]) == ""


@pytest.mark.parametrize(
    "bad", ["charz", "wordz", "tiktokenz"]
)  # avoid real tiktoken import
def test_create_tokenizer_lexicographic_non_matches_raise(bad: str) -> None:
    """Ensure strings that are lexicographically >= but not equal still raise, killing Eq->GtE mutants."""
    with pytest.raises(ValueError):
        create_tokenizer(cast(Any, bad))


def test_create_tokenizer_word_with_invalid_vocab_type() -> None:
    """create_tokenizer should raise TypeError for word tokenizer with non-mapping vocab."""
    with pytest.raises(TypeError) as exc:
        create_tokenizer("word", vocab=[1, 2, 3])  # List instead of dict

    assert "vocab must be a mapping" in str(exc.value)


def test_create_tokenizer_tiktoken_with_non_string_encoding_name() -> None:
    """create_tokenizer should raise TypeError for tiktoken with non-string encoding_name."""
    with pytest.raises(TypeError) as exc:
        create_tokenizer("tiktoken", encoding_name=123)  # Int instead of str

    assert "encoding_name must be a string" in str(exc.value)


def test_create_tokenizer_tiktoken_with_non_callable_loader() -> None:
    """create_tokenizer should raise TypeError for tiktoken with non-callable loader."""
    with pytest.raises(TypeError) as exc:
        create_tokenizer(
            "tiktoken", loader="not_callable"
        )  # String instead of callable

    assert "loader must be callable" in str(exc.value)


def test_char_tokenizer_lookup_array_rebuild_on_grow() -> None:
    """CharTokenizer should rebuild lookup array when itos grows beyond current array."""
    tok = CharTokenizerTestHarness({"a": 0})
    assert tok.lookup_array_length() == 1

    # Manually add a larger index to itos
    tok.itos[5] = "f"

    # _ensure_lookup_array should rebuild
    lookup = tok.expose_lookup_array()
    assert lookup.shape[0] >= 6


def test_word_tokenizer_lookup_array_rebuild_on_grow() -> None:
    """WordTokenizer should rebuild lookup array when itos grows beyond current array."""
    tok = WordTokenizerTestHarness({"hello": 0})
    assert tok.lookup_array_length() == 1

    # Manually add a larger index to itos
    tok.itos[5] = "world"

    # _ensure_lookup_array should rebuild
    lookup = tok.expose_lookup_array()
    assert lookup.shape[0] >= 6


def test_tiktoken_tokenizer_vocab_without_mergeable_ranks() -> None:
    """TiktokenTokenizer should return empty mapping when _mergeable_ranks is None."""

    class FakeEncoder:
        n_vocab = 10
        _mergeable_ranks = None  # No mergeable ranks

        def encode(
            self, text: str, allowed_special: set[str] | None = None
        ) -> list[int]:
            return []

        def decode(self, ids: list[int]) -> str:
            return ""

    class FakeModule:
        @staticmethod
        def get_encoding(name: str) -> FakeEncoder:
            return FakeEncoder()

    tk = TiktokenTokenizer(loader=lambda: FakeModule)
    assert tk.vocab == MappingProxyType({})


def test_char_tokenizer_encode_with_missing_chars() -> None:
    """CharTokenizer.encode should return 0 for missing characters."""
    tok = CharTokenizer({"a": 1, "b": 2})
    result = tok.encode("abc")
    assert result == [1, 2, 0]  # 'c' is missing, returns 0


def test_word_tokenizer_encode_with_missing_words() -> None:
    """WordTokenizer.encode should return 0 for missing words."""
    tok = WordTokenizer({"hello": 1, "world": 2})
    result = tok.encode("hello goodbye world")
    assert result == [1, 0, 2]  # 'goodbye' is missing, returns 0


def test_char_tokenizer_decode_with_empty_itos() -> None:
    """CharTokenizer.decode should return empty string when itos is empty."""
    tok = CharTokenizer()  # Empty vocab
    result = tok.decode([0, 1, 2])
    assert result == ""


def test_word_tokenizer_decode_with_empty_itos() -> None:
    """WordTokenizer.decode should return empty string when itos is empty."""
    tok = WordTokenizer()  # Empty vocab
    result = tok.decode([0, 1, 2])
    assert result == ""


def test_char_tokenizer_decode_with_all_out_of_range() -> None:
    """CharTokenizer.decode should return empty string when all ids are out of range."""
    tok = CharTokenizer({"a": 0})
    result = tok.decode([100, 200, 300])  # All out of range
    assert result == ""


def test_word_tokenizer_decode_with_all_out_of_range() -> None:
    """WordTokenizer.decode should return empty string when all ids are out of range."""
    tok = WordTokenizer({"hello": 0})
    result = tok.decode([100, 200, 300])  # All out of range
    assert result == ""
