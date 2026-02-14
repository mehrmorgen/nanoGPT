"""Unit tests for checkpoint_manager.py branch coverage.

Tests uncovered branches in load_best_checkpoint for empty best_checkpoints,
add_safe_globals exception handling, and dependency edge cases.
Uses DI fakes instead of mocks per project policy.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Iterable, Mapping

import pytest

import torch

from ml_playground.framework.training.checkpointing.checkpoint_manager import (
    Checkpoint,
    CheckpointManager,
    CheckpointDependencies,
    CheckpointError,
    CheckpointLoadError,
)


class FakeLogger:
    """Logger that captures messages."""

    def __init__(self) -> None:
        self.messages: list[tuple[str, str]] = []

    def info(self, msg: object, *args: object, **kwargs: Any) -> None:
        self.messages.append(("info", str(msg)))

    def error(self, msg: object, *args: object, **kwargs: Any) -> None:
        self.messages.append(("error", str(msg)))

    def debug(self, msg: object, *args: object, **kwargs: Any) -> None:
        pass

    def warning(self, msg: object, *args: object, **kwargs: Any) -> None:
        pass


class FakeStatResult:
    """Fake stat result for path_stat."""

    def __init__(self, mtime: float) -> None:
        self.st_mtime = mtime


class FakeCkptInfo:
    """Fake checkpoint info for testing (replaces private _CkptInfo)."""

    def __init__(
        self, path: Path, metric: float, iter_num: int, created_at: float
    ) -> None:
        self.path = path
        self.metric = metric
        self.iter_num = iter_num
        self.created_at = created_at


def test_load_best_checkpoint_empty_after_discover(tmp_path: Path) -> None:
    """Test CheckpointError when best_checkpoints empty after _discover_existing (581->589)."""

    def fake_torch_load(path: str, **kwargs: Any) -> Mapping[str, Any]:
        return {"model": {}, "optimizer": {}, "iter_num": 1}

    def fake_path_stat(path: Path) -> os.stat_result:
        return os.stat_result((0, 0, 0, 0, 0, 0, 0, 0, 0.0, 0.0))  # type: ignore[return-value]

    def fake_path_unlink(path: Path) -> None:
        pass

    deps = CheckpointDependencies(
        torch_load=fake_torch_load,
        add_safe_globals=None,
        path_stat=fake_path_stat,
        path_unlink=fake_path_unlink,
        posix_path_cls=None,
    )

    manager = CheckpointManager(
        out_dir=tmp_path,
        keep_last=1,
        keep_best=1,
        deps=deps,
    )

    logger = FakeLogger()

    # Should raise CheckpointError (no best checkpoints found)
    with pytest.raises(CheckpointError, match="No best checkpoints"):
        manager.load_best_checkpoint("cpu", logger)


def test_load_best_checkpoint_add_safe_globals_exception(tmp_path: Path) -> None:
    """Test exception handling in add_safe_globals (594->601)."""
    add_globals_called = False

    def fake_add_safe_globals(_globals_list: Iterable[object]) -> None:
        nonlocal add_globals_called
        add_globals_called = True
        raise RuntimeError("Cannot add globals")

    def fake_torch_load(path: str, **kwargs: Any) -> Mapping[str, Any]:
        return {
            "model": {},
            "optimizer": {},
            "model_args": {},
            "iter_num": 100,
            "best_val_loss": 0.5,
            "config": {},
        }

    def fake_path_stat(p: Path) -> Any:
        return FakeStatResult(1.0)

    def fake_path_unlink(path: Path) -> None:
        pass

    deps = CheckpointDependencies(
        torch_load=fake_torch_load,
        add_safe_globals=fake_add_safe_globals,
        path_stat=fake_path_stat,
        path_unlink=fake_path_unlink,
        posix_path_cls=Path,  # Not None to trigger add_safe_globals call
    )

    manager = CheckpointManager(
        out_dir=tmp_path,
        keep_last=1,
        keep_best=1,
        deps=deps,
    )

    # Create fake best checkpoint entry using FakeCkptInfo
    fake_entry = FakeCkptInfo(tmp_path / "best_ckpt.pt", 0.5, 100, 1.0)
    manager.best_checkpoints = [fake_entry]  # type: ignore[list-item]

    logger = FakeLogger()

    # Should complete despite RuntimeError in add_safe_globals
    result = manager.load_best_checkpoint("cpu", logger)
    assert result is not None
    assert add_globals_called


def test_load_best_checkpoint_posix_path_cls_none(tmp_path: Path) -> None:
    """Test when posix_path_cls is None (596->601)."""
    add_globals_called = False

    def fake_add_safe_globals(_globals_list: Iterable[object]) -> None:
        nonlocal add_globals_called
        add_globals_called = True

    def fake_torch_load(path: str, **kwargs: Any) -> Mapping[str, Any]:
        return {
            "model": {},
            "optimizer": {},
            "model_args": {},
            "iter_num": 100,
            "best_val_loss": 0.5,
            "config": {},
        }

    def fake_path_stat(p: Path) -> Any:
        return FakeStatResult(1.0)

    def fake_path_unlink(path: Path) -> None:
        pass

    deps = CheckpointDependencies(
        torch_load=fake_torch_load,
        add_safe_globals=fake_add_safe_globals,  # Callable but should not be called
        path_stat=fake_path_stat,
        path_unlink=fake_path_unlink,
        posix_path_cls=None,  # None - should skip add_safe_globals
    )

    manager = CheckpointManager(
        out_dir=tmp_path,
        keep_last=1,
        keep_best=1,
        deps=deps,
    )

    # Create fake best checkpoint entry using FakeCkptInfo
    fake_entry = FakeCkptInfo(tmp_path / "best_ckpt.pt", 0.5, 100, 1.0)
    manager.best_checkpoints = [fake_entry]  # type: ignore[list-item]

    logger = FakeLogger()

    # Should complete without calling add_safe_globals
    result = manager.load_best_checkpoint("cpu", logger)
    assert result is not None
    assert not add_globals_called  # add_safe_globals should not be called


def test_load_best_checkpoint_add_safe_globals_not_callable(tmp_path: Path) -> None:
    """Test when add_safe_globals is not callable."""

    def fake_torch_load(path: str, **kwargs: Any) -> Mapping[str, Any]:
        return {
            "model": {},
            "optimizer": {},
            "model_args": {},
            "iter_num": 100,
            "best_val_loss": 0.5,
            "config": {},
        }

    def fake_path_stat(p: Path) -> Any:
        return FakeStatResult(1.0)

    def fake_path_unlink(path: Path) -> None:
        pass

    # Use a string as add_safe_globals (not callable)
    deps = CheckpointDependencies(
        torch_load=fake_torch_load,
        add_safe_globals="not_callable",  # type: ignore[arg-type]  # Not callable
        path_stat=fake_path_stat,
        path_unlink=fake_path_unlink,
        posix_path_cls=Path,
    )

    manager = CheckpointManager(
        out_dir=tmp_path,
        keep_last=1,
        keep_best=1,
        deps=deps,
    )

    # Create fake best checkpoint entry using FakeCkptInfo
    fake_entry = FakeCkptInfo(tmp_path / "best_ckpt.pt", 0.5, 100, 1.0)
    manager.best_checkpoints = [fake_entry]  # type: ignore[list-item]

    logger = FakeLogger()

    # Should complete without calling add_safe_globals (it's not callable)
    result = manager.load_best_checkpoint("cpu", logger)
    assert result is not None


def test_load_best_checkpoint_torch_load_error(tmp_path: Path) -> None:
    """Test CheckpointLoadError when torch_load raises exception."""

    def fake_torch_load(path: str, **kwargs: Any) -> Mapping[str, Any]:
        raise OSError("File not found")

    def fake_path_stat(p: Path) -> Any:
        return FakeStatResult(1.0)

    def fake_path_unlink(path: Path) -> None:
        pass

    deps = CheckpointDependencies(
        torch_load=fake_torch_load,
        add_safe_globals=None,
        path_stat=fake_path_stat,
        path_unlink=fake_path_unlink,
        posix_path_cls=None,
    )

    manager = CheckpointManager(
        out_dir=tmp_path,
        keep_last=1,
        keep_best=1,
        deps=deps,
    )

    # Create fake best checkpoint entry using FakeCkptInfo
    fake_entry = FakeCkptInfo(tmp_path / "best_ckpt.pt", 0.5, 100, 1.0)
    manager.best_checkpoints = [fake_entry]  # type: ignore[list-item]

    logger = FakeLogger()

    # Should raise CheckpointLoadError
    with pytest.raises(CheckpointLoadError, match="Failed to load"):
        manager.load_best_checkpoint("cpu", logger)


# ---------------------------------------------------------------------------
# _expect_mapping: non-string key (line 169)
# ---------------------------------------------------------------------------


def test_checkpoint_from_payload_non_string_key() -> None:
    """Line 169: _expect_mapping raises on non-string key in a mapping field."""
    # model field must be a mapping with string keys
    payload: dict[object, object] = {
        "model": {123: "bad_key_value"},  # non-string key
        "optimizer": {},
        "model_args": {},
        "iter_num": 1,
        "best_val_loss": 0.5,
        "config": {},
    }
    with pytest.raises(CheckpointError, match="non-string key"):
        Checkpoint.from_payload(payload)  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# save_checkpoint: domain naming (lines 350, 360)
# ---------------------------------------------------------------------------


def _make_manager_with_deps(
    tmp_path: Path,
    *,
    naming_policy: str = "standard",
    counter_label: str | None = None,
    strict_naming: bool = True,
    keep_last: int = 2,
    keep_best: int = 2,
) -> CheckpointManager:
    """Helper to create a CheckpointManager with fake deps."""

    def fake_torch_load(path: str, **kwargs: Any) -> Mapping[str, Any]:
        return {"model": {}, "optimizer": {}, "iter_num": 1}

    def fake_path_stat(path: Path) -> Any:
        return FakeStatResult(1.0)

    unlinked: list[Path] = []

    def fake_path_unlink(path: Path) -> None:
        unlinked.append(path)

    deps = CheckpointDependencies(
        torch_load=fake_torch_load,
        add_safe_globals=None,
        path_stat=fake_path_stat,
        path_unlink=fake_path_unlink,
        posix_path_cls=None,
    )

    return CheckpointManager(
        out_dir=tmp_path,
        keep_last=keep_last,
        keep_best=keep_best,
        naming_policy=naming_policy,
        counter_label=counter_label,
        strict_naming=strict_naming,
        deps=deps,
    )


def _make_checkpoint() -> Checkpoint:
    return Checkpoint(
        model={},
        optimizer={},
        model_args={},
        iter_num=1,
        best_val_loss=0.5,
        config={},
        ema=None,
    )


def test_save_checkpoint_domain_naming_best(tmp_path: Path) -> None:
    """Line 360: domain naming for best checkpoint produces labeled filename."""
    manager = _make_manager_with_deps(
        tmp_path, naming_policy="domain", counter_label="epoch"
    )
    logger = FakeLogger()
    checkpoint = _make_checkpoint()
    path = manager.save_checkpoint(
        checkpoint, metric=0.5, iter_num=1, logger=logger, is_best=True
    )
    assert "ckpt_best_epoch_" in path.name
    assert path.exists()


def test_save_checkpoint_domain_naming_last(tmp_path: Path) -> None:
    """Line 350/360 complement: domain naming for last checkpoint."""
    manager = _make_manager_with_deps(
        tmp_path, naming_policy="domain", counter_label="step"
    )
    logger = FakeLogger()
    checkpoint = _make_checkpoint()
    path = manager.save_checkpoint(
        checkpoint, metric=0.5, iter_num=1, logger=logger, is_best=False
    )
    assert "ckpt_last_step_" in path.name
    assert path.exists()


# ---------------------------------------------------------------------------
# save_checkpoint: sidecar unlink via path_unlink (line 429)
# ---------------------------------------------------------------------------


def test_save_checkpoint_best_prune_sidecar_via_path_unlink(tmp_path: Path) -> None:
    """Line 429: sidecar deleted via path_unlink."""
    unlinked: list[Path] = []

    def fake_torch_load(path: str, **kwargs: Any) -> Mapping[str, Any]:
        return {}

    def fake_path_stat(path: Path) -> Any:
        return FakeStatResult(1.0)

    def fake_path_unlink(path: Path) -> None:
        unlinked.append(path)
        # Actually delete if it exists
        if path.exists():
            path.unlink()

    deps = CheckpointDependencies(
        torch_load=fake_torch_load,
        add_safe_globals=None,
        path_stat=fake_path_stat,
        path_unlink=fake_path_unlink,
        posix_path_cls=None,
    )

    manager = CheckpointManager(
        out_dir=tmp_path,
        keep_last=1,
        keep_best=1,
        deps=deps,
    )

    logger = FakeLogger()
    checkpoint = _make_checkpoint()

    # Save two best checkpoints so the first gets pruned
    path1 = manager.save_checkpoint(
        checkpoint, metric=0.9, iter_num=1, logger=logger, is_best=True
    )
    # Create sidecar for first checkpoint
    sidecar = path1.with_suffix(path1.suffix + ".json")
    sidecar.write_text("{}")

    _path2 = manager.save_checkpoint(
        checkpoint, metric=0.1, iter_num=2, logger=logger, is_best=True
    )

    # Sidecar should have been unlinked via fake_path_unlink
    assert sidecar in unlinked


# ---------------------------------------------------------------------------
# _parse_last_counter: domain label mismatch (lines 451-458)
# ---------------------------------------------------------------------------


def test_parse_last_counter_domain_label_mismatch_strict(tmp_path: Path) -> None:
    """Lines 451-456: strict_naming raises on label mismatch in last checkpoint."""
    # Create a checkpoint file with wrong label
    wrong_file = tmp_path / "ckpt_last_wrong_00000001.pt"
    wrong_file.write_bytes(b"")

    with pytest.raises(CheckpointError, match="Unexpected counter label"):
        _make_manager_with_deps(
            tmp_path,
            naming_policy="domain",
            counter_label="epoch",
            strict_naming=True,
        )


def test_parse_last_counter_domain_label_mismatch_nonstrict(tmp_path: Path) -> None:
    """Lines 457-458: non-strict falls back to unlabeled parsing on label mismatch."""
    wrong_file = tmp_path / "ckpt_last_wrong_00000001.pt"
    wrong_file.write_bytes(b"")

    # Should not raise; falls back to unlabeled parsing
    manager = _make_manager_with_deps(
        tmp_path,
        naming_policy="domain",
        counter_label="epoch",
        strict_naming=False,
    )
    assert len(manager.last_checkpoints) == 1


# ---------------------------------------------------------------------------
# _parse_best_counter: domain label mismatch (lines 481-482)
# ---------------------------------------------------------------------------


def test_parse_best_counter_domain_label_mismatch_strict(tmp_path: Path) -> None:
    """Lines 481-482: strict_naming raises on label mismatch in best checkpoint."""
    wrong_file = tmp_path / "ckpt_best_wrong_00000001_0.500000.pt"
    wrong_file.write_bytes(b"")

    with pytest.raises(CheckpointError, match="Unexpected counter label"):
        _make_manager_with_deps(
            tmp_path,
            naming_policy="domain",
            counter_label="epoch",
            strict_naming=True,
        )


def test_parse_best_counter_domain_label_mismatch_nonstrict(tmp_path: Path) -> None:
    """Branch 481->498: non-strict falls back to unlabeled parsing on label mismatch.

    When strict_naming=False and the label doesn't match, the code falls back to
    unlabeled parsing where parts[2] is treated as the iteration counter. We use a
    numeric label ("99999999") so that the fallback can parse it as an integer.
    """
    # Label "99999999" != "epoch", but is parseable as int for unlabeled fallback
    wrong_file = tmp_path / "ckpt_best_99999999_00000001_0.500000.pt"
    wrong_file.write_bytes(b"")

    # Should not raise; falls back to unlabeled parsing
    manager = _make_manager_with_deps(
        tmp_path,
        naming_policy="domain",
        counter_label="epoch",
        strict_naming=False,
    )
    assert len(manager.best_checkpoints) == 1


# ---------------------------------------------------------------------------
# load_best_checkpoint: discover finds checkpoints (581->589)
# ---------------------------------------------------------------------------


def test_load_best_checkpoint_discover_finds_checkpoints(tmp_path: Path) -> None:
    """Branch 581->589: load_best_checkpoint discovers checkpoints from disk.

    The manager must have an empty best_checkpoints list when load_best_checkpoint
    is called, so that the internal _discover_existing() call finds the file.
    """

    saved_data = {
        "model": {},
        "optimizer": {},
        "model_args": {},
        "iter_num": 100,
        "best_val_loss": 0.5,
        "config": {},
    }

    def fake_torch_load(path: str, **kwargs: Any) -> Mapping[str, Any]:
        return saved_data

    def fake_path_stat(path: Path) -> Any:
        return FakeStatResult(1.0)

    def fake_path_unlink(path: Path) -> None:
        pass

    deps = CheckpointDependencies(
        torch_load=fake_torch_load,
        add_safe_globals=None,
        path_stat=fake_path_stat,
        path_unlink=fake_path_unlink,
        posix_path_cls=None,
    )

    # Create manager with empty directory first
    manager = CheckpointManager(
        out_dir=tmp_path,
        keep_last=1,
        keep_best=1,
        deps=deps,
    )
    assert len(manager.best_checkpoints) == 0

    # Now place a best checkpoint file on disk AFTER construction
    best_file = tmp_path / "ckpt_best_00000100_0.500000.pt"
    torch.save(saved_data, best_file)

    logger = FakeLogger()
    # load_best_checkpoint should call _discover_existing and find the file
    result = manager.load_best_checkpoint("cpu", logger)
    assert result is not None
    assert len(manager.best_checkpoints) == 1
