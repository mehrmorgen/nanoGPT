from __future__ import annotations

import logging
from pathlib import Path
from typing import cast

from ml_playground.configuration.models import PreparerConfig
from ml_playground.experiments.bundestag_qwen15b_lora_mps.preparer import (
    BundestagQwen15bLoraMpsPreparer,
    diff_paths,
    snapshot_paths,
)


def test_bundestag_qwen15b_preparer_creates_dataset_dir(tmp_path: Path) -> None:
    """BundestagQwen15bLoraMpsPreparer should ensure dataset directory exists."""
    exp_dir = tmp_path / "bundestag_qwen15b_lora_mps"
    exp_dir.mkdir()

    preparer = BundestagQwen15bLoraMpsPreparer()

    cfg = PreparerConfig(
        tokenizer_type="tiktoken",
        logger=logging.getLogger(__name__),
        extras={"dataset_dir_override": str(exp_dir)},
    )

    report = preparer.prepare(cfg)

    # Check that dataset directory was created
    ds_dir = exp_dir / "datasets"
    assert ds_dir.exists()
    assert ds_dir.is_dir()

    # Check report
    assert len(report.messages) > 0
    assert any("bundestag_qwen15b_lora_mps" in msg for msg in report.messages)
    assert len(report.created_files) > 0 or len(report.skipped_files) > 0


def test_bundestag_qwen15b_preparer_handles_existing_dir(tmp_path: Path) -> None:
    """BundestagQwen15bLoraMpsPreparer should handle existing dataset directory."""
    exp_dir = tmp_path / "bundestag_qwen15b_lora_mps"
    exp_dir.mkdir()
    ds_dir = exp_dir / "datasets"
    ds_dir.mkdir()

    preparer = BundestagQwen15bLoraMpsPreparer()

    cfg = PreparerConfig(
        tokenizer_type="tiktoken",
        logger=logging.getLogger(__name__),
        extras={"dataset_dir_override": str(exp_dir)},
    )

    report = preparer.prepare(cfg)

    # Directory should still exist
    assert ds_dir.exists()
    assert ds_dir.is_dir()

    # Should report as skipped since it already existed
    assert len(report.skipped_files) > 0


def test_bundestag_qwen15b_preparer_snapshot_handles_oserror(tmp_path: Path) -> None:
    """_snapshot should handle OSError when Path.exists raises."""
    probe = tmp_path / "probe"
    probe.mkdir()

    class BrokenPath(type(probe)):  # type: ignore[misc]
        def exists(self):  # type: ignore[override]
            raise OSError("boom")

    err_path = BrokenPath(probe)

    result = snapshot_paths([err_path])

    assert err_path in result
    assert result[err_path] == (False, 0.0, 0)


def test_bundestag_qwen15b_preparer_diff_handles_oserror(tmp_path: Path) -> None:
    """_diff should treat OSError during stat as a creation event when path now exists."""
    target = tmp_path / "datasets"
    target.mkdir()

    class FlakyPath(type(target)):  # type: ignore[misc]
        def exists(self):  # type: ignore[override]
            return True

        def stat(self):  # type: ignore[override]
            raise OSError("stat failed")

    flaky = FlakyPath(target)
    before: dict[Path, tuple[bool, float, int]] = {flaky: (False, 0.0, 0)}

    created, updated, skipped = diff_paths([flaky], before)

    assert flaky in created
    assert not updated
    assert not skipped


def test_bundestag_qwen15b_preparer_diff_handles_missing_path(tmp_path: Path) -> None:
    """_diff should no-op when the path is absent."""
    missing = tmp_path / "missing"
    before: dict[Path, tuple[bool, float, int]] = {missing: (False, 0.0, 0)}

    created, updated, skipped = diff_paths([missing], before)

    assert not created
    assert not updated
    assert not skipped


def test_bundestag_qwen15b_preparer_diff_oserror_when_missing() -> None:
    """_diff should skip when OSError occurs and the path disappears."""

    class TogglePath(Path):
        _flavour = Path(".")._flavour  # type: ignore[attr-defined]

        def __new__(cls) -> TogglePath:  # type: ignore[override]
            self = Path.__new__(cls, "ghost")
            return cast(TogglePath, self)

        def __init__(self) -> None:  # type: ignore[override]
            self._calls: int = 0

        def __hash__(self) -> int:  # ensure hashing doesn't depend on private internals
            return hash("TogglePath:ghost")

        def __str__(
            self,
        ) -> str:  # avoid accessing private fields in pathlib when stringifying
            return "ghost"

        def exists(self) -> bool:  # type: ignore[override]
            self._calls += 1
            return self._calls == 1

        def stat(self):  # type: ignore[override]
            raise OSError("stat failed")

    ghost_path: Path = cast(Path, TogglePath())
    before: dict[Path, tuple[bool, float, int]] = {ghost_path: (False, 0.0, 0)}

    created, updated, skipped = diff_paths([ghost_path], before)

    assert not created
    assert not updated
    assert not skipped


def test_bundestag_qwen15b_preparer_detects_file_updates(tmp_path: Path) -> None:
    """BundestagQwen15bLoraMpsPreparer should detect when files are updated."""
    # Create a file
    test_file = tmp_path / "test.txt"
    test_file.write_text("original")

    # Take snapshot
    before = snapshot_paths([test_file])

    # Modify the file
    import time

    time.sleep(0.01)  # Ensure mtime changes
    test_file.write_text("modified content")

    # Check diff
    created, updated, _ = diff_paths([test_file], before)

    # Should detect as updated
    assert test_file in updated or test_file in created
