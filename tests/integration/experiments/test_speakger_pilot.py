from __future__ import annotations

from pathlib import Path
from typing import Any, cast

from ml_playground.experiments.speakger.preparer import SpeakGerPreparer
from ml_playground.framework.configuration.models import PreparerConfig


def test_speakger_preparer_creates_dataset_dir(tmp_path: Path) -> None:
    base_dir = tmp_path / "speakger"
    base_dir.mkdir()

    cfg = PreparerConfig(
        tokenizer_type="tiktoken",
        logger=cast(Any, None),
        extras={"dataset_dir_override": str(base_dir)},
    )

    report = SpeakGerPreparer().prepare(cfg)
    ds_dir = base_dir / "datasets"

    assert ds_dir.exists()
    assert ds_dir in report.skipped_files
    assert any("speakger" in msg for msg in report.messages)
