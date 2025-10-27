from __future__ import annotations

from pathlib import Path
from typing import Iterable


try:
    Path(".")._flavour  # type: ignore[attr-defined]
except AttributeError:
    setattr(type(Path(".")), "_flavour", object())

from ml_playground.configuration.models import PreparerConfig
from ml_playground.core.file_state import diff_file_states, snapshot_file_states
from ml_playground.experiments.protocol import (
    Preparer as _PreparerProto,
    PrepareReport,
)


class BundestagQwen15bLoraMpsPreparer(_PreparerProto):
    """Minimal preparer to satisfy CLI integration for this preset.

    This preset rides on a generic HF+PEFT integration for training/sampling.
    For preparation, we currently only ensure the configured dataset directory
    exists so downstream steps can locate it. A richer pipeline can be added
    later to actually build tokenizer/artifacts from raw data.
    """

    def prepare(self, cfg: PreparerConfig) -> PrepareReport:  # type: ignore[override]
        # Determine dataset directory: use local folder under this preset
        extras = getattr(cfg, "extras", {}) or {}
        base_dir_override = extras.get("dataset_dir_override")
        exp_dir = (
            Path(base_dir_override)
            if base_dir_override is not None
            else Path(__file__).resolve().parent
        )
        ds_dir = (exp_dir / "datasets").resolve()

        # Track side-effects (creation/updates) for user feedback
        tracked: list[Path] = [ds_dir]
        before = snapshot_file_states(tracked)

        # Ensure dataset directory exists
        ds_dir.mkdir(parents=True, exist_ok=True)

        created, updated, skipped = diff_file_states(tracked, before)
        msgs = (
            f"[bundestag_qwen15b_lora_mps] ensured dataset directory at {ds_dir}",
            f"[bundestag_qwen15b_lora_mps.outputs.created] {[str(p) for p in created]}",
            f"[bundestag_qwen15b_lora_mps.outputs.updated] {[str(p) for p in updated]}",
            f"[bundestag_qwen15b_lora_mps.outputs.skipped] {[str(p) for p in skipped]}",
        )
        return PrepareReport(
            created_files=tuple(created),
            updated_files=tuple(updated),
            skipped_files=tuple(skipped),
            messages=msgs,
        )


def _snapshot(paths: Iterable[Path]) -> dict[Path, tuple[bool, float, int]]:
    return snapshot_file_states(paths)


def _diff(paths: Iterable[Path], before: dict[Path, tuple[bool, float, int]]):
    created, updated, skipped = diff_file_states(paths, before)
    return list(created), list(updated), list(skipped)
