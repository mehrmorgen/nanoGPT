from __future__ import annotations

import pytest

from ml_playground.framework.core.error_handling import DataError
from ml_playground.framework.data_pipeline.transforms.metadata import (
    validate_metadata_contract,
)


def test_validate_metadata_contract_rejects_non_mapping() -> None:
    """Validation rejects non-mapping metadata."""
    with pytest.raises(DataError, match="Metadata must be a mapping"):
        validate_metadata_contract("not-a-mapping")


def test_validate_metadata_contract_rejects_missing_required_fields() -> None:
    """Validation rejects metadata missing required fields."""
    with pytest.raises(DataError, match="Metadata missing required fields"):
        validate_metadata_contract({"meta_version": "1.0"})


def test_validate_metadata_contract_rejects_non_string_tokenizer_type() -> None:
    """Validation rejects non-string tokenizer_type."""
    with pytest.raises(DataError, match="Metadata missing tokenizer_type"):
        validate_metadata_contract(
            {
                "meta_version": "1.0",
                "tokenizer_type": 123,
                "train_tokens": 1000,
                "val_tokens": 200,
            }
        )


def test_validate_metadata_contract_rejects_char_tokenizer_missing_vocab() -> None:
    """Validation rejects char tokenizer without vocab mapping."""
    with pytest.raises(DataError, match="Metadata missing vocab mapping"):
        validate_metadata_contract(
            {
                "meta_version": "1.0",
                "tokenizer_type": "char",
                "train_tokens": 1000,
                "val_tokens": 200,
            }
        )


def test_validate_metadata_contract_rejects_word_tokenizer_missing_vocab() -> None:
    """Validation rejects word tokenizer without vocab mapping."""
    with pytest.raises(DataError, match="Metadata missing vocab mapping"):
        validate_metadata_contract(
            {
                "meta_version": "1.0",
                "tokenizer_type": "word",
                "train_tokens": 1000,
                "val_tokens": 200,
            }
        )


def test_validate_metadata_contract_rejects_tiktoken_missing_encoding_name() -> None:
    """Validation rejects tiktoken tokenizer without encoding_name."""
    with pytest.raises(DataError, match="Metadata missing encoding_name"):
        validate_metadata_contract(
            {
                "meta_version": "1.0",
                "tokenizer_type": "tiktoken",
                "train_tokens": 1000,
                "val_tokens": 200,
            }
        )


def test_validate_metadata_contract_rejects_tiktoken_empty_encoding_name() -> None:
    """Validation rejects tiktoken tokenizer with empty encoding_name."""
    with pytest.raises(DataError, match="Metadata missing encoding_name"):
        validate_metadata_contract(
            {
                "meta_version": "1.0",
                "tokenizer_type": "tiktoken",
                "encoding_name": "",
                "train_tokens": 1000,
                "val_tokens": 200,
            }
        )


def test_validate_metadata_contract_accepts_char_tokenizer_with_stoi() -> None:
    """Validation accepts char tokenizer with stoi mapping."""
    result = validate_metadata_contract(
        {
            "meta_version": "1.0",
            "tokenizer_type": "char",
            "stoi": {"a": 1, "b": 2},
            "train_tokens": 1000,
            "val_tokens": 200,
        }
    )
    assert result["tokenizer_type"] == "char"


def test_validate_metadata_contract_accepts_char_tokenizer_with_vocab() -> None:
    """Validation accepts char tokenizer with vocab mapping."""
    result = validate_metadata_contract(
        {
            "meta_version": "1.0",
            "tokenizer_type": "char",
            "vocab": {"a": 1, "b": 2},
            "train_tokens": 1000,
            "val_tokens": 200,
        }
    )
    assert result["tokenizer_type"] == "char"


def test_validate_metadata_contract_accepts_word_tokenizer_with_vocab() -> None:
    """Validation accepts word tokenizer with vocab mapping."""
    result = validate_metadata_contract(
        {
            "meta_version": "1.0",
            "tokenizer_type": "word",
            "vocab": {"hello": 1, "world": 2},
            "train_tokens": 1000,
            "val_tokens": 200,
        }
    )
    assert result["tokenizer_type"] == "word"


def test_validate_metadata_contract_accepts_tiktoken_with_encoding_name() -> None:
    """Validation accepts tiktoken tokenizer with encoding_name."""
    result = validate_metadata_contract(
        {
            "meta_version": "1.0",
            "tokenizer_type": "tiktoken",
            "encoding_name": "gpt2",
            "train_tokens": 1000,
            "val_tokens": 200,
        }
    )
    assert result["tokenizer_type"] == "tiktoken"
    assert result["encoding_name"] == "gpt2"


def test_validate_metadata_contract_accepts_unknown_tokenizer_type() -> None:
    """Validation accepts unknown tokenizer types without extra validation."""
    result = validate_metadata_contract(
        {
            "meta_version": "1.0",
            "tokenizer_type": "unknown",
            "train_tokens": 1000,
            "val_tokens": 200,
        }
    )
    assert result["tokenizer_type"] == "unknown"
