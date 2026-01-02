"""Append-only dataset utilities for streaming/self-play data."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Iterable, Mapping
import pickle

import numpy as np

from ml_playground.configuration.models import DataConfig
from ml_playground.core.error_handling import DataError
from ml_playground.core.logging_protocol import LoggerLike
from ml_playground.core.file_state import diff_file_states, snapshot_file_states
from ml_playground.data_pipeline.transforms.io import write_bin_and_meta

__all__ = [
    "REQUIRED_STREAM_FIELDS",
    "append_bin_and_meta",
    "validate_streaming_records",
]

REQUIRED_STREAM_FIELDS = ("start", "winner", "moves", "policy_targets")


def validate_streaming_records(
    records: Iterable[Mapping[str, Any]],
) -> list[Mapping[str, Any]]:
    """Validate streaming record shape for self-play datasets."""
    validated: list[Mapping[str, Any]] = []
    for idx, record in enumerate(records):
        if not isinstance(record, Mapping):
            raise DataError(
                f"Streaming record at index {idx} must be a mapping",
                reason=f"Received {type(record).__name__}",
                rationale="Streaming prep relies on structured record dictionaries",
            )
        missing = [field for field in REQUIRED_STREAM_FIELDS if field not in record]
        if missing:
            raise DataError(
                f"Streaming record at index {idx} missing fields: {missing}",
                reason="Record schema incomplete",
                rationale="Streaming prep requires 'start', 'winner', 'moves', and 'policy_targets'",
            )
        validated.append(record)
    return validated


def _resolve_paths(
    ds_dir: Path, data_cfg: DataConfig | None
) -> tuple[Path, Path, Path]:
    if data_cfg is not None:
        return (
            data_cfg.train_path(ds_dir),
            data_cfg.val_path(ds_dir),
            data_cfg.meta_path(ds_dir),
        )
    return ds_dir / "train.bin", ds_dir / "val.bin", ds_dir / "meta.pkl"


def _load_meta(meta_path: Path) -> dict[str, Any]:
    try:
        with meta_path.open("rb") as handle:
            meta = pickle.load(handle)
    except (OSError, pickle.UnpicklingError, EOFError) as exc:
        raise DataError(
            f"Failed to read existing meta.pkl at {meta_path}: {exc}",
            reason=f"Unable to deserialize metadata due to {exc.__class__.__name__}",
            rationale="Streaming prep must preserve metadata to update counters",
        ) from exc
    if not isinstance(meta, dict) or "meta_version" not in meta:
        raise DataError(
            f"Invalid existing meta.pkl at {meta_path}: expected dict with 'meta_version'",
            reason="Metadata structure missing required 'meta_version' key",
            rationale="Streaming prep requires versioned metadata for safe updates",
        )
    return meta


def _refresh_metadata(
    existing: dict[str, Any],
    *,
    train_tokens_added: int,
    val_tokens_added: int,
    updates: dict[str, Any],
) -> dict[str, Any]:
    if not isinstance(existing.get("train_tokens"), int) or not isinstance(
        existing.get("val_tokens"), int
    ):
        raise DataError(
            "Existing metadata missing train/val token counts",
            reason="train_tokens/val_tokens not present in metadata",
            rationale="Streaming prep must update token counts incrementally",
        )
    refreshed = dict(existing)
    refreshed["train_tokens"] = existing["train_tokens"] + train_tokens_added
    refreshed["val_tokens"] = existing["val_tokens"] + val_tokens_added
    for key, value in updates.items():
        if key not in {"train_tokens", "val_tokens"}:
            refreshed[key] = value
    return refreshed


def append_bin_and_meta(
    ds_dir: Path,
    train: np.ndarray,
    val: np.ndarray,
    meta: dict[str, Any],
    *,
    logger: LoggerLike,
    data_cfg: DataConfig | None = None,
) -> dict[str, Any]:
    """Append tokens to existing bins and refresh metadata counts."""
    ds_dir.mkdir(parents=True, exist_ok=True)
    train_path, val_path, meta_path = _resolve_paths(ds_dir, data_cfg)
    before = snapshot_file_states([train_path, val_path, meta_path])

    if not meta_path.exists():
        write_bin_and_meta(ds_dir, train, val, meta, logger=logger, data_cfg=data_cfg)
        return meta

    existing_meta = _load_meta(meta_path)

    with train_path.open("ab") as handle:
        handle.write(train.tobytes())
    with val_path.open("ab") as handle:
        handle.write(val.tobytes())

    updated_meta = _refresh_metadata(
        existing_meta,
        train_tokens_added=int(train.size),
        val_tokens_added=int(val.size),
        updates=meta,
    )
    tmp_meta = meta_path.with_name("." + meta_path.name + ".tmp")
    try:
        with tmp_meta.open("wb") as handle:
            pickle.dump(updated_meta, handle)
        tmp_meta.replace(meta_path)
    finally:
        tmp_meta.unlink(missing_ok=True)

    created, updated, skipped = diff_file_states(
        [train_path, val_path, meta_path], before
    )
    try:
        logger.info(f"[streaming] Created: {list(created) if created else '[]'}")
        logger.info(f"[streaming] Updated: {list(updated) if updated else '[]'}")
        logger.info(f"[streaming] Skipped: {list(skipped) if skipped else '[]'}")
    except (OSError, ValueError, TypeError):
        pass

    return updated_meta
