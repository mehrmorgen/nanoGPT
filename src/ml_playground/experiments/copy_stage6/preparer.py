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


class CopyStage6Preparer(_PreparerProto):
    """Prepare deterministic Stage 6 Tic-Tac-Toe policy traces."""

    def prepare(self, cfg: PreparerConfig) -> PrepareReport:  # type: ignore[override]
        extras = cast(Mapping[str, object], getattr(cfg, "extras", {}) or {})
        ds_dir = _resolve_dataset_dir(extras)
        repeat_records = _coerce_positive_int(extras.get("repeat_records"), default=24)

        ds_dir.mkdir(parents=True, exist_ok=True)
        outputs = [ds_dir / "train.bin", ds_dir / "val.bin", ds_dir / "meta.pkl"]

        base_records = _build_tictactoe_records()
        data = "|".join(base_records * repeat_records) + "|"
        expected_tokens = tuple(sorted(set(data)))
        total_tokens = len(data)

        if _artifacts_look_valid(outputs, stage="copy_stage6", expected_tokens=expected_tokens, total_tokens=total_tokens):
            return PrepareReport(
                created_files=tuple(),
                updated_files=tuple(),
                skipped_files=tuple(outputs),
                messages=(f"[copy_stage6] dataset already prepared at {ds_dir}; skipping.",),
            )

        pre = snapshot_file_states(outputs)
        train_arr, val_arr, meta, _ = prepare_with_tokenizer(data, CharTokenizer())
        meta["stage"] = "copy_stage6"
        meta["task"] = "tictactoe_policy_and_outcome"
        meta["record_count"] = len(base_records) * repeat_records

        for path in outputs:
            path.unlink(missing_ok=True)
        write_bin_and_meta(ds_dir, train_arr, val_arr, meta, logger=cfg.logger)

        created, updated, skipped = diff_file_states(outputs, pre)
        return PrepareReport(
            created_files=tuple(Path(path) for path in created),
            updated_files=tuple(Path(path) for path in updated),
            skipped_files=tuple(Path(path) for path in skipped),
            messages=(f"[copy_stage6] prepared deterministic tic-tac-toe dataset at {ds_dir}",),
        )


def _resolve_dataset_dir(extras: Mapping[str, object]) -> Path:
    dataset_dir_override = extras.get("dataset_dir_override")
    if isinstance(dataset_dir_override, (str, Path)):
        return Path(dataset_dir_override)
    return Path(__file__).resolve().parent / "datasets"


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


def _build_tictactoe_records() -> list[str]:
    # Record schema: T<board><move><outcome>
    # board uses 9 chars from {X,O,.}; move is 0-8; outcome is W/D/L from X perspective.
    return [
        "T.........4D",  # empty board: center first move is strong.
        "TX........4D",
        "T....X....0D",
        "TX..O....4D",
        "TX..O.X..6W",
        "TXX.O.O..2W",
        "TXOXOOXX.8D",
        "TXXOO.X..5W",
        "TXOX.OX..8W",
        "TXXOOOXX.8L",
        "TXX.OO..X6W",
        "T..X.O..X4D",
    ]


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
