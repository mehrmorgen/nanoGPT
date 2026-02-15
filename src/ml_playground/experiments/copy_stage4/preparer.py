from __future__ import annotations

import pickle
from pathlib import Path
from typing import Mapping, cast

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


class CopyStage4Preparer(_PreparerProto):
    """Prepare deterministic Stage 4 variable-length copy data."""

    def prepare(self, cfg: PreparerConfig) -> PrepareReport:  # type: ignore[override]
        extras = cast(Mapping[str, object], getattr(cfg, "extras", {}) or {})
        ds_dir = _resolve_dataset_dir(extras)

        symbols = _resolve_symbols(cfg, extras)
        sos_symbol = _resolve_marker(extras, key="sos_symbol", default="^")
        eos_symbol = _resolve_marker(extras, key="eos_symbol", default="~")
        _validate_markers(symbols=symbols, sos_symbol=sos_symbol, eos_symbol=eos_symbol)

        min_len = _coerce_positive_int(extras.get("min_length"), default=2)
        max_len = _coerce_positive_int(extras.get("max_length"), default=10)
        if min_len > max_len:
            raise ValueError("min_length must be <= max_length")
        total_sequences = _coerce_positive_int(extras.get("total_sequences"), default=128)

        lengths = [min_len + (idx % (max_len - min_len + 1)) for idx in range(total_sequences)]
        total_tokens = sum(length + 2 for length in lengths)

        ds_dir.mkdir(parents=True, exist_ok=True)
        outputs = [ds_dir / "train.bin", ds_dir / "val.bin", ds_dir / "meta.pkl"]

        expected_tokens = tuple(sorted((symbols[0], symbols[1], sos_symbol, eos_symbol)))
        if _artifacts_look_valid(outputs, stage="copy_stage4", expected_tokens=expected_tokens, total_tokens=total_tokens):
            return PrepareReport(
                created_files=tuple(),
                updated_files=tuple(),
                skipped_files=tuple(outputs),
                messages=(f"[copy_stage4] dataset already prepared at {ds_dir}; skipping.",),
            )

        pre = snapshot_file_states(outputs)
        data = _build_stage4_stream(
            symbols=symbols,
            sos_symbol=sos_symbol,
            eos_symbol=eos_symbol,
            lengths=lengths,
        )

        train_arr, val_arr, meta, _ = prepare_with_tokenizer(data, CharTokenizer())
        meta["stage"] = "copy_stage4"
        meta["task"] = "variable_length_copy"
        meta["min_length"] = min_len
        meta["max_length"] = max_len

        for path in outputs:
            path.unlink(missing_ok=True)
        write_bin_and_meta(ds_dir, train_arr, val_arr, meta, logger=cfg.logger)

        created, updated, skipped = diff_file_states(outputs, pre)
        return PrepareReport(
            created_files=tuple(Path(path) for path in created),
            updated_files=tuple(Path(path) for path in updated),
            skipped_files=tuple(Path(path) for path in skipped),
            messages=(f"[copy_stage4] prepared deterministic variable-length dataset at {ds_dir}",),
        )


def _resolve_dataset_dir(extras: Mapping[str, object]) -> Path:
    dataset_dir_override = extras.get("dataset_dir_override")
    if isinstance(dataset_dir_override, (str, Path)):
        return Path(dataset_dir_override)
    return Path(__file__).resolve().parent / "datasets"


def _resolve_symbols(cfg: PreparerConfig, extras: Mapping[str, object]) -> tuple[str, str]:
    raw_text_path = cfg.raw_text_path
    if isinstance(raw_text_path, Path):
        text = _read_raw_text(raw_text_path, cfg).strip()
        unique = sorted(set(text))
        if len(unique) != 2:
            raise ValueError("copy_stage4 raw_text_path must contain exactly two unique symbols")
        return (unique[0], unique[1])

    symbol_pair = cast(str, extras.get("symbols", "AB"))
    if not isinstance(symbol_pair, str) or len(symbol_pair) != 2:
        raise ValueError("symbols must be a two-character string, e.g. 'AB'")
    unique = sorted(set(symbol_pair))
    if len(unique) != 2:
        raise ValueError("copy_stage4 requires two distinct payload symbols")
    return (unique[0], unique[1])


def _read_raw_text(path: Path, cfg: PreparerConfig) -> str:
    reader = cfg.read_text_fn
    if reader is not None:
        return reader(path)
    return path.read_text(encoding="utf-8")


def _resolve_marker(extras: Mapping[str, object], *, key: str, default: str) -> str:
    value = extras.get(key, default)
    if not isinstance(value, str) or len(value) != 1:
        raise ValueError(f"{key} must be a single character")
    return value


def _validate_markers(
    *, symbols: tuple[str, str], sos_symbol: str, eos_symbol: str
) -> None:
    if sos_symbol == eos_symbol:
        raise ValueError("sos_symbol and eos_symbol must be distinct")
    if sos_symbol in symbols or eos_symbol in symbols:
        raise ValueError("sos/eos markers must differ from payload symbols")


def _coerce_positive_int(value: object, *, default: int) -> int:
    if value is None:
        return default
    try:
        result = int(cast(int, value))
    except (TypeError, ValueError) as exc:
        raise ValueError("Expected a positive integer") from exc
    if result <= 0:
        raise ValueError("Expected a positive integer")
    return result


def _build_stage4_stream(
    *,
    symbols: tuple[str, str],
    sos_symbol: str,
    eos_symbol: str,
    lengths: list[int],
) -> str:
    samples: list[str] = []
    for seq_idx, length in enumerate(lengths):
        payload = "".join(symbols[(seq_idx + token_idx) % 2] for token_idx in range(length))
        samples.append(f"{sos_symbol}{payload}{eos_symbol}")
    return "".join(samples)


def _artifacts_look_valid(
    outputs: list[Path], *, stage: str, expected_tokens: tuple[str, ...], total_tokens: int
) -> bool:
    if not all(path.exists() and path.stat().st_size > 0 for path in outputs):
        return False

    train_path, val_path, meta_path = outputs
    try:
        with meta_path.open("rb") as f:
            meta_obj = pickle.load(f)
    except (OSError, pickle.UnpicklingError, EOFError):
        return False
    if not isinstance(meta_obj, dict):
        return False

    meta = cast(dict[str, object], meta_obj)
    if meta.get("stage") != stage:
        return False
    if meta.get("tokenizer_type") != "char":
        return False

    stoi_obj = meta.get("stoi")
    if not isinstance(stoi_obj, dict):
        return False
    stoi = cast(dict[str, object], stoi_obj)
    if tuple(sorted(stoi.keys())) != expected_tokens:
        return False

    vocab_size = _coerce_optional_int(meta.get("vocab_size"))
    if vocab_size != len(expected_tokens):
        return False

    expected_train = int(total_tokens * 0.9)
    expected_val = total_tokens - expected_train
    if _coerce_optional_int(meta.get("train_tokens")) != expected_train:
        return False
    if _coerce_optional_int(meta.get("val_tokens")) != expected_val:
        return False

    if train_path.stat().st_size != expected_train * 2:
        return False
    if val_path.stat().st_size != expected_val * 2:
        return False
    return True


def _coerce_optional_int(value: object) -> int | None:
    try:
        return int(cast(int, value))
    except (TypeError, ValueError):
        return None
