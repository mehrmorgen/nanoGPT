from __future__ import annotations

import pickle
from pathlib import Path
from typing import Iterable, Mapping, cast

from ml_playground.framework.configuration.models import PreparerConfig
from ml_playground.framework.core.tokenizer import CharTokenizer
from ml_playground.framework.data_pipeline.transforms.io import (
    diff_file_states,
    snapshot_file_states,
    write_bin_and_meta,
)
from ml_playground.framework.data_pipeline.transforms.tokenization import (
    prepare_with_tokenizer,
)
from ml_playground.framework.experiment_registry.protocol import (
    Preparer as _PreparerProto,
    PrepareReport,
)


class CopyStage0Preparer(_PreparerProto):
    """Prepare deterministic Stage 0 data for one-symbol learned copying."""

    def prepare(self, cfg: PreparerConfig) -> PrepareReport:  # type: ignore[override]
        extras = cast(Mapping[str, object], getattr(cfg, "extras", {}) or {})

        dataset_dir_override = extras.get("dataset_dir_override")
        if isinstance(dataset_dir_override, (str, Path)):
            ds_dir = Path(dataset_dir_override)
        else:
            ds_dir = Path(__file__).resolve().parent / "datasets"

        total_symbols = cast(int, extras.get("total_symbols", 640))
        if total_symbols <= 0:
            raise ValueError("total_symbols must be a positive integer")

        symbol = _resolve_symbol(cfg, extras)

        ds_dir.mkdir(parents=True, exist_ok=True)
        outputs = [ds_dir / "train.bin", ds_dir / "val.bin", ds_dir / "meta.pkl"]

        if _artifacts_look_valid(outputs, symbol=symbol, total_symbols=total_symbols):
            messages = (
                f"[copy_stage0] dataset already prepared at {ds_dir}; skipping.",
                "[copy_stage0.outputs.created] []",
                "[copy_stage0.outputs.updated] []",
                f"[copy_stage0.outputs.skipped] {[str(path) for path in outputs]}",
            )
            return PrepareReport(
                created_files=tuple(),
                updated_files=tuple(),
                skipped_files=tuple(outputs),
                messages=messages,
            )

        pre = snapshot_file_states(outputs)

        data = symbol * total_symbols
        tokenizer = CharTokenizer()
        train_arr, val_arr, meta, _ = prepare_with_tokenizer(data, tokenizer)

        # Ensure stale-but-versioned artifacts are replaced with deterministic Stage 0 data.
        for path in outputs:
            path.unlink(missing_ok=True)
        write_bin_and_meta(ds_dir, train_arr, val_arr, meta, logger=cfg.logger)

        created, updated, skipped = diff_file_states(outputs, pre)
        created_paths = [Path(path) for path in created]
        updated_paths = [Path(path) for path in updated]
        skipped_paths = [Path(path) for path in skipped]

        messages = (
            f"[copy_stage0] prepared deterministic one-symbol dataset at {ds_dir}",
            f"[copy_stage0.outputs.created] {[str(path) for path in created_paths]}",
            f"[copy_stage0.outputs.updated] {[str(path) for path in updated_paths]}",
            f"[copy_stage0.outputs.skipped] {[str(path) for path in skipped_paths]}",
        )

        return PrepareReport(
            created_files=tuple(created_paths),
            updated_files=tuple(updated_paths),
            skipped_files=tuple(skipped_paths),
            messages=messages,
        )


def _artifacts_look_valid(
    outputs: Iterable[Path], *, symbol: str, total_symbols: int
) -> bool:
    output_list = list(outputs)
    if not all(path.exists() and path.stat().st_size > 0 for path in output_list):
        return False

    train_path, val_path, meta_path = output_list
    try:
        with meta_path.open("rb") as f:
            meta_obj = pickle.load(f)
    except (OSError, pickle.UnpicklingError, EOFError):
        return False
    if not isinstance(meta_obj, dict):
        return False

    meta = cast(dict[str, object], meta_obj)
    stoi_obj = meta.get("stoi")
    if not isinstance(stoi_obj, dict):
        return False
    stoi = cast(dict[str, object], stoi_obj)
    if stoi != {symbol: 0}:
        return False

    if meta.get("tokenizer_type") != "char":
        return False
    vocab_size = _coerce_int(meta.get("vocab_size"))
    if vocab_size != 1:
        return False

    expected_train = int(total_symbols * 0.9)
    expected_val = total_symbols - expected_train
    train_tokens = _coerce_int(meta.get("train_tokens"))
    if train_tokens != expected_train:
        return False
    val_tokens = _coerce_int(meta.get("val_tokens"))
    if val_tokens != expected_val:
        return False

    # Artifacts are stored as uint16 arrays.
    if train_path.stat().st_size != expected_train * 2:
        return False
    if val_path.stat().st_size != expected_val * 2:
        return False
    return True


def _resolve_symbol(cfg: PreparerConfig, extras: Mapping[str, object]) -> str:
    raw_text_path = cfg.raw_text_path
    if isinstance(raw_text_path, Path):
        raw_text = _read_raw_text(raw_text_path, cfg)
        return _extract_single_symbol(raw_text)

    symbol = cast(str, extras.get("symbol", "A"))
    if not isinstance(symbol, str) or len(symbol) != 1:
        raise ValueError("symbol must be a single character")
    return symbol


def _read_raw_text(path: Path, cfg: PreparerConfig) -> str:
    reader = cfg.read_text_fn
    if reader is not None:
        return reader(path)
    return path.read_text(encoding="utf-8")


def _extract_single_symbol(raw_text: str) -> str:
    normalized = raw_text.strip()
    if not normalized:
        raise ValueError(
            "copy_stage0 raw_text_path must contain at least one non-whitespace character"
        )
    unique_chars = set(normalized)
    if len(unique_chars) != 1:
        raise ValueError(
            "copy_stage0 raw_text_path must contain exactly one unique symbol"
        )
    return normalized[0]


def _coerce_int(value: object) -> int | None:
    try:
        return int(cast(int, value))
    except (TypeError, ValueError):
        return None
