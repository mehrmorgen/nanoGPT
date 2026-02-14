from __future__ import annotations

from pathlib import Path
from typing import Iterable, Mapping, cast

from ml_playground.framework.configuration.models import PreparerConfig
from ml_playground.framework.data_pipeline.transforms.tokenization import (
    prepare_with_tokenizer,
)
from ml_playground.framework.data_pipeline.transforms.io import (
    coerce_seed_policy,
    diff_file_states,
    seed_text_file_with_policy,
    snapshot_file_states,
    write_bin_and_meta,
)
from ml_playground.framework.core.tokenizer import CharTokenizer
from ml_playground.framework.experiment_registry.protocol import (
    Preparer as _PreparerProto,
    PrepareReport,
)
from ml_playground.framework.core.error_handling import validate_file_exists


class BundestagCharPreparer(_PreparerProto):
    def __init__(self, base_dir: Path | None = None) -> None:
        self._base_dir = base_dir

    def prepare(self, cfg: PreparerConfig) -> PrepareReport:  # type: ignore[override]
        extras = cast(Mapping[str, object], getattr(cfg, "extras", {}) or {})
        base_dir_override = extras.get("dataset_dir_override")
        if isinstance(base_dir_override, (str, Path)):
            exp_dir = Path(base_dir_override)
        elif self._base_dir is not None:
            exp_dir = self._base_dir
        else:
            exp_dir = Path(__file__).resolve().parent

        ds_dir = exp_dir / "datasets"
        ds_dir.mkdir(parents=True, exist_ok=True)
        outputs = [ds_dir / "train.bin", ds_dir / "val.bin", ds_dir / "meta.pkl"]

        if _artifacts_look_valid(outputs):
            msgs = (
                f"[bundestag_char] dataset already prepared at {ds_dir}; skipping.",
                "[bundestag_char.outputs.created] []",
                "[bundestag_char.outputs.updated] []",
                f"[bundestag_char.outputs.skipped] {[str(p) for p in outputs]}",
            )
            return PrepareReport(
                created_files=tuple(),
                updated_files=tuple(),
                skipped_files=tuple(outputs),
                messages=msgs,
            )

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

        raw_path = cfg.raw_text_path or input_file_path
        data = Path(raw_path).read_text(encoding="utf-8")

        tokenizer_type = cfg.tokenizer_type
        if tokenizer_type != "char":
            raise ValueError(
                "BundestagCharPreparer only supports char tokenizer configured via prepare.tokenizer_type"
            )
        tokenizer = CharTokenizer()  # Let prepare_with_tokenizer build the vocab

        train_arr, val_arr, meta, tok_result = prepare_with_tokenizer(data, tokenizer)
        # Type narrow tok_result to CharTokenizer if needed for variable assignment
        if not isinstance(tok_result, CharTokenizer):
            raise TypeError("Expected CharTokenizer")
        tokenizer = tok_result

        write_bin_and_meta(ds_dir, train_arr, val_arr, meta, logger=cfg.logger)

        created, updated, skipped = diff_file_states(outputs, pre)
        created_paths = [Path(path) for path in created]
        updated_paths = [Path(path) for path in updated]
        skipped_paths = [Path(path) for path in skipped]

        msgs = (
            f"[bundestag_char] prepared dataset at {ds_dir}",
            f"[bundestag_char.outputs.created] {[str(p) for p in created_paths]}",
            f"[bundestag_char.outputs.updated] {[str(p) for p in updated_paths]}",
            f"[bundestag_char.outputs.skipped] {[str(p) for p in skipped_paths]}",
        )

        return PrepareReport(
            created_files=tuple(created_paths),
            updated_files=tuple(updated_paths),
            skipped_files=tuple(skipped_paths),
            messages=msgs,
        )


def _artifacts_look_valid(outputs: Iterable[Path]) -> bool:
    for path in outputs:
        if not path.exists():
            return False
        if path.stat().st_size == 0:
            return False
    return True


def artifacts_look_valid(outputs: Iterable[Path]) -> bool:
    return _artifacts_look_valid(outputs)
