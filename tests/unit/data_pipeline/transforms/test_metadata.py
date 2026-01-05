from __future__ import annotations

import pytest

from ml_playground.core.error_handling import DataError
from ml_playground.data_pipeline.transforms.metadata import validate_metadata_contract


def test_validate_metadata_contract_raises_on_non_mapping() -> None:
    with pytest.raises(DataError, match="Metadata must be a mapping"):
        validate_metadata_contract(["not", "a", "mapping"])


def test_validate_metadata_contract_raises_on_missing_fields() -> None:
    with pytest.raises(DataError, match="missing required fields"):
        validate_metadata_contract({"meta_version": 1})


def test_validate_metadata_contract_raises_on_missing_vocab_for_char_tokenizer() -> (
    None
):
    with pytest.raises(DataError, match="missing vocab mapping for tokenizer"):
        validate_metadata_contract(
            {
                "meta_version": 1,
                "tokenizer_type": "char",
                "train_tokens": 100,
                "val_tokens": 20,
            }
        )


def test_validate_metadata_contract_raises_on_missing_encoding_name_for_tiktoken() -> (
    None
):
    with pytest.raises(DataError, match="missing encoding_name for tiktoken"):
        validate_metadata_contract(
            {
                "meta_version": 1,
                "tokenizer_type": "tiktoken",
                "train_tokens": 100,
                "val_tokens": 20,
            }
        )
