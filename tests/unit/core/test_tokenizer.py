from typing import Any, Literal, cast
from types import MappingProxyType

import pytest
from hypothesis import given, strategies as st

from ml_playground.core.tokenizer import (
    CharTokenizer,
    TiktokenTokenizer,
    WordTokenizer,
    create_tokenizer,
)
from tests.unit.helpers.tokenizer_harness import TokenizerTestHarness


class CharTokenizerTestHarness(CharTokenizer, TokenizerTestHarness):
    """Test harness exposing lookup array maintenance for char tokenizer."""


class WordTokenizerTestHarness(WordTokenizer, TokenizerTestHarness):
    """Test harness exposing lookup array maintenance for word tokenizer."""


# Property-based tests for comprehensive coverage
@given(
    st.dictionaries(
        st.characters(min_codepoint=32, max_codepoint=126),
        st.integers(min_value=1, max_value=1000),
        min_size=1,
        max_size=10,
    ).filter(lambda d: len(set(d.values())) == len(d))  # Ensure unique token IDs
)
def test_char_tokenizer_roundtrip_property(vocab: dict[str, int]) -> None:
    """CharTokenizer encode/decode roundtrip preserves text for valid vocab."""
    tk = CharTokenizer(vocab=vocab)
    assert tk.name == "char"
    assert tk.vocab_size == len(vocab)

    # Test roundtrip for vocab characters
    text = "".join(vocab.keys())
    ids = tk.encode(text)
    decoded = tk.decode(ids)
    assert decoded == text

    # Test vocab is read-only
    v = tk.vocab
    assert isinstance(v, MappingProxyType)
    with pytest.raises(TypeError):
        cast(dict[str, int], v)["new"] = 1


@given(
    st.dictionaries(
        st.characters(min_codepoint=32, max_codepoint=126),
        st.integers(min_value=1, max_value=100),
        min_size=1,
        max_size=5,
    )
)
def test_char_tokenizer_lookup_array_rebuild_property(vocab: dict[str, int]) -> None:
    """CharTokenizer rebuilds lookup array when vocabulary grows dynamically."""
    tok = CharTokenizerTestHarness(vocab)
    initial_size = tok.lookup_array_length()

    # Add a token beyond current array size
    new_idx = max(vocab.values()) + 10
    tok.itos[new_idx] = "z"

    # Should rebuild lookup array
    lookup = tok.expose_lookup_array()
    assert lookup.shape[0] > initial_size
    assert tok.decode([new_idx]) == "z"


@given(
    st.dictionaries(
        st.text(
            min_size=1,
            max_size=10,
            alphabet=st.characters(min_codepoint=97, max_codepoint=122),
        ),  # lowercase letters only
        st.integers(min_value=1, max_value=1000),
        min_size=1,
        max_size=10,
    ).filter(lambda d: len(set(d.values())) == len(d))  # Ensure unique token IDs
)
def test_word_tokenizer_roundtrip_property(vocab: dict[str, int]) -> None:
    """WordTokenizer encode/decode preserves word boundaries and handles missing words."""
    tk = WordTokenizer(vocab=vocab)
    assert tk.name == "word"
    assert tk.vocab_size == len(vocab)

    # Test that individual vocab words encode correctly
    for word in vocab.keys():
        ids = tk.encode(word)
        assert len(ids) == 1
        assert ids[0] == vocab[word]
        assert tk.decode(ids) == word

    # Test missing words return 0
    missing_text = "unknownword"
    ids_with_missing = tk.encode(missing_text)
    assert 0 in ids_with_missing

    # Test vocab is read-only
    v = tk.vocab
    assert isinstance(v, MappingProxyType)
    with pytest.raises(TypeError):
        cast(dict[str, int], v)["new"] = 1


@given(
    st.dictionaries(
        st.text(min_size=1, max_size=10),
        st.integers(min_value=1, max_value=100),
        min_size=1,
        max_size=5,
    )
)
def test_word_tokenizer_lookup_array_rebuild_property(vocab: dict[str, int]) -> None:
    """WordTokenizer rebuilds lookup array when vocabulary grows dynamically."""
    tok = WordTokenizerTestHarness(vocab)
    initial_size = tok.lookup_array_length()

    # Add a token beyond current array size
    new_idx = max(vocab.values()) + 10
    tok.itos[new_idx] = "newword"

    # Should rebuild lookup array
    lookup = tok.expose_lookup_array()
    assert lookup.shape[0] > initial_size
    assert tok.decode([new_idx]) == "newword"


# Factory function tests
@pytest.mark.parametrize(
    ("tok_type", "kwargs", "expected_cls"),
    [
        ("char", {"vocab": {"x": 1}}, CharTokenizer),
        ("word", {"vocab": {"x": 1}}, WordTokenizer),
    ],
)
def test_create_tokenizer_factory_valid_types(
    tok_type: Literal["char", "word"], kwargs: dict[str, Any], expected_cls: type
) -> None:
    """Factory returns expected tokenizer subclass for valid types."""
    tk = create_tokenizer(tok_type, **kwargs)
    assert isinstance(tk, expected_cls)


@given(
    st.text(min_size=1, max_size=10).filter(
        lambda x: x not in ["char", "word", "tiktoken"]
    )
)
def test_create_tokenizer_factory_invalid_types(invalid_type: str) -> None:
    """Factory raises ValueError for unknown tokenizer names."""
    with pytest.raises(ValueError):
        create_tokenizer(invalid_type)


# Tiktoken tokenizer tests with fake module
@given(st.integers(min_value=1, max_value=100))
def test_tiktoken_tokenizer_with_fake_module(vocab_size: int) -> None:
    """TiktokenTokenizer properties with fake module."""

    class FakeEncoder:
        def __init__(self, n_vocab: int) -> None:
            self.n_vocab = n_vocab
            self._mergeable_ranks = {f"token_{i}": i for i in range(n_vocab)}

        def encode(
            self, text: str, allowed_special: set[str] | None = None
        ) -> list[int]:
            return [1, 2] if text else []

        def decode(self, ids: list[int]) -> str:
            return "decoded"

    class FakeTiktokenModule:
        @staticmethod
        def get_encoding(name: str) -> FakeEncoder:
            return FakeEncoder(vocab_size)

    tk = TiktokenTokenizer(loader=lambda: FakeTiktokenModule)
    assert tk.name == "tiktoken"
    assert tk.vocab_size == vocab_size
    assert tk.decode(tk.encode("test")) == "decoded"

    v = tk.vocab
    assert hasattr(v, "__getitem__")
    assert len(v) == vocab_size


def test_tiktoken_tokenizer_missing_mergeable_ranks() -> None:
    """TiktokenTokenizer returns empty mapping when encoder lacks mergeable ranks."""

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


def test_tiktoken_tokenizer_import_error_propagation() -> None:
    """Loader ImportError should be surfaced with helpful message."""

    def loader() -> None:
        raise ImportError("missing dependency")

    with pytest.raises(ImportError) as exc:
        TiktokenTokenizer(loader=loader)

    assert "tiktoken is required" in str(exc.value)


# Edge case tests for empty vocabularies and out-of-range handling
@given(st.lists(st.integers(min_value=-10, max_value=100), min_size=1, max_size=10))
def test_char_tokenizer_decode_edge_cases(ids: list[int]) -> None:
    """CharTokenizer.decode handles empty vocab and out-of-range IDs gracefully."""
    # Test with empty vocab
    empty_tok = CharTokenizer()
    result = empty_tok.decode(ids)
    assert result == ""

    # Test with small vocab and out-of-range IDs
    small_tok = CharTokenizer({"a": 0})
    large_ids = [100, 200, 300]  # All out of range
    result = small_tok.decode(large_ids)
    assert result == ""


@given(st.lists(st.integers(min_value=-10, max_value=100), min_size=1, max_size=10))
def test_word_tokenizer_decode_edge_cases(ids: list[int]) -> None:
    """WordTokenizer.decode handles empty vocab and out-of-range IDs gracefully."""
    # Test with empty vocab
    empty_tok = WordTokenizer()
    result = empty_tok.decode(ids)
    assert result == ""

    # Test with small vocab and out-of-range IDs
    small_tok = WordTokenizer({"hello": 0})
    large_ids = [100, 200, 300]  # All out of range
    result = small_tok.decode(large_ids)
    assert result == ""


# Missing character/word handling tests
@given(
    st.dictionaries(
        st.characters(min_codepoint=32, max_codepoint=126),
        st.integers(min_value=1, max_value=100),
        min_size=1,
        max_size=5,
    )
)
def test_char_tokenizer_encode_missing_chars(vocab: dict[str, int]) -> None:
    """CharTokenizer.encode returns 0 for missing characters."""
    tk = CharTokenizer(vocab)
    # Include missing character guaranteed not to be in vocab
    missing_char = chr(127)  # DEL character, not in printable ASCII range
    text_with_missing = "".join(vocab.keys()) + missing_char
    result = tk.encode(text_with_missing)
    assert 0 in result  # missing_char should be encoded as 0


@given(
    st.dictionaries(
        st.text(
            min_size=1,
            max_size=10,
            alphabet=st.characters(min_codepoint=97, max_codepoint=122),
        ),
        st.integers(min_value=1, max_value=100),
        min_size=1,
        max_size=5,
    ).filter(lambda d: len(set(d.values())) == len(d))  # Ensure unique token IDs
)
def test_word_tokenizer_encode_missing_words(vocab: dict[str, int]) -> None:
    """WordTokenizer.encode returns 0 for missing words."""
    tk = WordTokenizer(vocab)
    # Include missing word
    text_with_missing = " ".join(vocab.keys()) + " unknownword"
    result = tk.encode(text_with_missing)
    assert 0 in result  # 'unknownword' should be encoded as 0


# Targeted tests for uncovered edge cases
def test_char_tokenizer_build_lookup_array_empty_vocab() -> None:
    """CharTokenizer._build_lookup_array returns empty array for empty vocab."""
    tk = CharTokenizer()
    lookup = tk._build_lookup_array()
    assert lookup.shape == (0,)
    assert lookup.dtype == object


def test_word_tokenizer_build_lookup_array_empty_vocab() -> None:
    """WordTokenizer._build_lookup_array returns empty array for empty vocab."""
    tk = WordTokenizer()
    lookup = tk._build_lookup_array()
    assert lookup.shape == (0,)
    assert lookup.dtype == object


def test_char_tokenizer_build_lookup_array_negative_indices() -> None:
    """CharTokenizer._build_lookup_array skips negative indices."""
    tk = CharTokenizer({"a": -1, "b": 1})  # Include negative index
    lookup = tk._build_lookup_array()
    # Should handle negative indices gracefully
    assert lookup.shape[0] >= 2  # max index + 1
    assert tk.decode([1]) == "b"  # Positive index works
    assert tk.decode([-1]) == ""  # Negative index returns empty


def test_word_tokenizer_build_lookup_array_negative_indices() -> None:
    """WordTokenizer._build_lookup_array skips negative indices."""
    tk = WordTokenizer({"hello": -1, "world": 1})  # Include negative index
    lookup = tk._build_lookup_array()
    # Should handle negative indices gracefully
    assert lookup.shape[0] >= 2  # max index + 1
    assert tk.decode([1]) == "world"  # Positive index works
    assert tk.decode([-1]) == ""  # Negative index returns empty


def test_create_tokenizer_word_unsupported_kwargs() -> None:
    """create_tokenizer raises ValueError for unsupported kwargs to word tokenizer."""
    with pytest.raises(ValueError) as exc:
        create_tokenizer("word", vocab={"hello": 1}, unsupported_param="value")

    assert "Unsupported keyword arguments for word tokenizer" in str(exc.value)
    assert "unsupported_param" in str(exc.value)


def test_create_tokenizer_tiktoken_non_string_encoding_name() -> None:
    """create_tokenizer raises TypeError for non-string encoding_name."""
    with pytest.raises(TypeError) as exc:
        create_tokenizer("tiktoken", encoding_name=123)

    assert "encoding_name must be a string" in str(exc.value)


def test_create_tokenizer_tiktoken_non_callable_loader() -> None:
    """create_tokenizer raises TypeError for non-callable loader."""
    with pytest.raises(TypeError) as exc:
        create_tokenizer("tiktoken", loader="not_callable")

    assert "loader must be callable when provided" in str(exc.value)


def test_create_tokenizer_tiktoken_unsupported_kwargs() -> None:
    """create_tokenizer raises ValueError for unsupported kwargs to tiktoken tokenizer."""
    with pytest.raises(ValueError) as exc:
        create_tokenizer("tiktoken", unsupported_param="value")

    assert "Unsupported keyword arguments for tiktoken tokenizer" in str(exc.value)
    assert "unsupported_param" in str(exc.value)
