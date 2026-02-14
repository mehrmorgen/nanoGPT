from __future__ import annotations

from hypothesis import given, settings, strategies as st
import pytest

from ml_playground.framework.core.error_handling import DataError
from ml_playground.framework.data_pipeline.transforms.metadata import (
    validate_metadata_contract,
)


@settings(max_examples=20, deadline=None, derandomize=True)
@given(  # type: ignore[reportAny]
    tokenizer_type=st.sampled_from(["char", "word", "tiktoken"]),
    train_tokens=st.integers(min_value=0, max_value=1000),
    val_tokens=st.integers(min_value=0, max_value=1000),
)
def test_validate_metadata_contract_when_valid_then_accepts(
    tokenizer_type: str, train_tokens: int, val_tokens: int
) -> None:
    """Validate metadata contract when valid then accepts."""
    meta = {
        "meta_version": 1,
        "tokenizer_type": tokenizer_type,
        "train_tokens": train_tokens,
        "val_tokens": val_tokens,
    }
    if tokenizer_type in {"char", "word"}:
        meta["stoi"] = {"a": 0}
    else:
        meta["encoding_name"] = "cl100k_base"

    validated = validate_metadata_contract(meta)
    assert validated["tokenizer_type"] == tokenizer_type
    assert validated["train_tokens"] == train_tokens
    assert validated["val_tokens"] == val_tokens


@settings(max_examples=20, deadline=None, derandomize=True)
@given(  # type: ignore[reportAny]
    missing_key=st.sampled_from(
        ["meta_version", "tokenizer_type", "train_tokens", "val_tokens"]
    )
)
def test_validate_metadata_contract_when_missing_required_then_raises(
    missing_key: str,
) -> None:
    """Validate metadata contract when missing required then raises."""
    meta = {
        "meta_version": 1,
        "tokenizer_type": "char",
        "train_tokens": 1,
        "val_tokens": 1,
        "stoi": {"a": 0},
    }
    meta.pop(missing_key)
    with pytest.raises(DataError):
        validate_metadata_contract(meta)


@settings(max_examples=15, deadline=None, derandomize=True)
@given(  # type: ignore[reportAny]
    meta=st.one_of(st.integers(), st.lists(st.integers()))
)
def test_validate_metadata_contract_rejects_non_mapping(meta: object) -> None:
    """Validate metadata contract rejects non-mapping payloads."""
    with pytest.raises(DataError):
        validate_metadata_contract(meta)  # type: ignore[arg-type]


@settings(max_examples=10, deadline=None, derandomize=True)
@given(  # type: ignore[reportAny]
    tokenizer_type=st.sampled_from(["char", "word"])
)
def test_validate_metadata_contract_requires_vocab_mapping(
    tokenizer_type: str,
) -> None:
    """Metadata contract requires vocab mapping for char/word tokenizers."""
    meta = {
        "meta_version": 1,
        "tokenizer_type": tokenizer_type,
        "train_tokens": 1,
        "val_tokens": 1,
    }
    with pytest.raises(DataError):
        validate_metadata_contract(meta)


@settings(max_examples=10, deadline=None, derandomize=True)
@given(  # type: ignore[reportAny]
    encoding_name=st.one_of(st.none(), st.integers(), st.text(max_size=0))
)
def test_validate_metadata_contract_requires_encoding_name_for_tiktoken(
    encoding_name: object,
) -> None:
    """Metadata contract requires encoding_name for tiktoken."""
    meta = {
        "meta_version": 1,
        "tokenizer_type": "tiktoken",
        "train_tokens": 1,
        "val_tokens": 1,
        "encoding_name": encoding_name,
    }
    with pytest.raises(DataError):
        validate_metadata_contract(meta)
