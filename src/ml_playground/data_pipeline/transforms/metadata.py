"""Metadata contract validation for prepared datasets."""

from __future__ import annotations

from typing import Any, Mapping

from ml_playground.core.error_handling import DataError

__all__ = ["validate_metadata_contract"]


_REQUIRED_FIELDS = ("meta_version", "tokenizer_type", "train_tokens", "val_tokens")


def validate_metadata_contract(meta: Mapping[str, Any]) -> dict[str, Any]:
    """Validate that metadata follows the framework contract."""
    if not isinstance(meta, Mapping):
        raise DataError(
            "Metadata must be a mapping",
            reason=f"Received {type(meta).__name__}",
            rationale="Metadata contract requires a dict-like payload",
        )

    missing = [field for field in _REQUIRED_FIELDS if field not in meta]
    if missing:
        raise DataError(
            f"Metadata missing required fields: {missing}",
            reason="Metadata contract incomplete",
            rationale="Prepared datasets must include required metadata keys",
        )

    tokenizer_type = meta.get("tokenizer_type")
    if tokenizer_type in {"char", "word"}:
        vocab = meta.get("stoi") or meta.get("vocab")
        if not isinstance(vocab, dict):
            raise DataError(
                "Metadata missing vocab mapping for tokenizer",
                reason="stoi/vocab not present for char/word tokenizers",
                rationale="Char/word tokenizers require vocab metadata",
            )
    elif tokenizer_type == "tiktoken":
        encoding_name = meta.get("encoding_name")
        if not isinstance(encoding_name, str) or not encoding_name:
            raise DataError(
                "Metadata missing encoding_name for tiktoken",
                reason="encoding_name not present for tiktoken tokenizer",
                rationale="Tiktoken metadata must record encoding name",
            )

    return dict(meta)
