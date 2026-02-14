from __future__ import annotations

from pathlib import Path
from typing import Mapping, cast

import numpy as np
from ml_playground.framework.configuration.models import PreparerConfig
from ml_playground.framework.data_pipeline.transforms.tokenization import (
    create_standardized_metadata,
    split_train_val,
)
from ml_playground.framework.data_pipeline.transforms.io import (
    coerce_seed_policy,
    diff_file_states,
    seed_text_file_with_policy,
    snapshot_file_states,
    write_bin_and_meta,
)
from ml_playground.framework.core.tokenizer import TiktokenTokenizer
from ml_playground.framework.experiment_registry.protocol import (
    Preparer as _PreparerProto,
    PrepareReport,
)
from ml_playground.framework.core.error_handling import (
    validate_file_exists,
    ProgressReporter,
)


class BundestagTiktokenPreparer(_PreparerProto):
    def prepare(self, cfg: PreparerConfig) -> PrepareReport:  # type: ignore[override]
        extras = cast(Mapping[str, object], getattr(cfg, "extras", {}) or {})
        base_dir_override = extras.get("dataset_dir_override")
        if isinstance(base_dir_override, (str, Path)):
            exp_dir = Path(base_dir_override)
        else:
            exp_dir = Path(__file__).resolve().parent
        ds_dir = exp_dir / "datasets"
        ds_dir.mkdir(parents=True, exist_ok=True)
        outputs = [ds_dir / "train.bin", ds_dir / "val.bin", ds_dir / "meta.pkl"]

        pre = snapshot_file_states(outputs)

        input_file_path = ds_dir / "input.txt"
        bundled = Path(__file__).parent / "input.txt"
        candidates = [
            Path("/datasets/Bundestag.csv"),
            ds_dir / "input.txt",
            exp_dir / "input.txt",
            exp_dir / "page1.txt",
            bundled,
        ]
        seed_policy_input: object | None = extras.get("seed_policy")
        seed_policy = coerce_seed_policy(seed_policy_input)

        seed_text_file_with_policy(input_file_path, candidates, policy=seed_policy)

        validate_file_exists(input_file_path, "Input text file")

        data = input_file_path.read_text(encoding="utf-8")
        train_text, val_text = split_train_val(data)

        logger = cfg.logger
        progress = ProgressReporter(logger, total_steps=4)

        progress.start("Starting Bundestag tiktoken preparation")

        tokenizer = TiktokenTokenizer(encoding_name="gpt2")

        progress.update(1, "Encoding training data")
        train_ids = tokenizer.encode(train_text)
        progress.update(1, "Encoding validation data")
        val_ids = tokenizer.encode(val_text)

        train_ids_arr: np.ndarray = np.array(train_ids, dtype=np.uint16)
        val_ids_arr: np.ndarray = np.array(val_ids, dtype=np.uint16)

        progress.update(1, "Creating metadata")
        meta = create_standardized_metadata(
            tokenizer=tokenizer, train_tokens=len(train_ids), val_tokens=len(val_ids)
        )

        write_bin_and_meta(ds_dir, train_ids_arr, val_ids_arr, meta, logger=cfg.logger)

        progress.finish("Bundestag tiktoken preparation completed")

        created, updated, skipped = diff_file_states(outputs, pre)
        created_paths = [Path(path) for path in created]
        updated_paths = [Path(path) for path in updated]
        skipped_paths = [Path(path) for path in skipped]

        msgs = (
            f"[bundestag_tiktoken] prepared dataset at {ds_dir}",
            f"[bundestag_tiktoken.outputs.created] {[str(p) for p in created_paths]}",
            f"[bundestag_tiktoken.outputs.updated] {[str(p) for p in updated_paths]}",
            f"[bundestag_tiktoken.outputs.skipped] {[str(p) for p in skipped_paths]}",
        )
        return PrepareReport(
            created_files=tuple(created_paths),
            updated_files=tuple(updated_paths),
            skipped_files=tuple(skipped_paths),
            messages=msgs,
        )
