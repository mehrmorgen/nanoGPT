from __future__ import annotations

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

        ds_dir.mkdir(parents=True, exist_ok=True)
        outputs = [ds_dir / "train.bin", ds_dir / "val.bin", ds_dir / "meta.pkl"]

        if _artifacts_look_valid(outputs):
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

        total_symbols = cast(int, extras.get("total_symbols", 640))
        if total_symbols <= 0:
            raise ValueError("total_symbols must be a positive integer")

        symbol = cast(str, extras.get("symbol", "A"))
        if not isinstance(symbol, str) or len(symbol) != 1:
            raise ValueError("symbol must be a single character")

        data = symbol * total_symbols
        tokenizer = CharTokenizer()
        train_arr, val_arr, meta, _ = prepare_with_tokenizer(data, tokenizer)

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


def _artifacts_look_valid(outputs: Iterable[Path]) -> bool:
    for path in outputs:
        if not path.exists() or path.stat().st_size == 0:
            return False
    return True


def artifacts_look_valid(outputs: Iterable[Path]) -> bool:
    return _artifacts_look_valid(outputs)
