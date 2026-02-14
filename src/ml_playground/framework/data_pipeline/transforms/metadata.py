"""Metadata contract validation for prepared datasets."""

from __future__ import annotations

from typing import Mapping, cast

from ml_playground.framework.core.error_handling import DataError

__all__ = ["validate_metadata_contract"]


_REQUIRED_FIELDS = ("meta_version", "tokenizer_type", "train_tokens", "val_tokens")


def validate_metadata_contract(meta: object) -> dict[str, object]:
    """Validate that metadata follows the framework contract."""
    if not isinstance(meta, Mapping):
        raise DataError(
            "Metadata must be a mapping",
            reason=f"Expected mapping, observed {type(meta).__name__}",
            rationale="Prepared datasets must supply metadata as a mapping",
        )

    meta_mapping: dict[str, object] = {}
    for key_obj, value in cast(Mapping[object, object], meta).items():
        key_str = str(key_obj)
        meta_mapping[key_str] = value
    missing = [field for field in _REQUIRED_FIELDS if field not in meta_mapping]
    if missing:
        raise DataError(
            f"Metadata missing required fields: {missing}",
            reason="Metadata contract incomplete",
            rationale="Prepared datasets must include required metadata keys",
        )

    tokenizer_type_obj = meta_mapping.get("tokenizer_type")
    if not isinstance(tokenizer_type_obj, str):
        raise DataError(
            "Metadata missing tokenizer_type",
            reason=f"tokenizer_type must be str, observed {type(tokenizer_type_obj).__name__}",
            rationale="Tokenizer type selects validation path for metadata contract",
        )

    if tokenizer_type_obj in {"char", "word"}:
        vocab_obj = meta_mapping.get("stoi") or meta_mapping.get("vocab")
        if not isinstance(vocab_obj, dict):
            raise DataError(
                "Metadata missing vocab mapping for tokenizer",
                reason="stoi/vocab not present for char/word tokenizers",
                rationale="Char/word tokenizers require vocab metadata",
            )
    elif tokenizer_type_obj == "tiktoken":
        encoding_name_obj = meta_mapping.get("encoding_name")
        if not isinstance(encoding_name_obj, str) or not encoding_name_obj:
            raise DataError(
                "Metadata missing encoding_name for tiktoken",
                reason="encoding_name not present for tiktoken tokenizer",
                rationale="Tiktoken metadata must record encoding name",
            )

    return dict(meta_mapping)
