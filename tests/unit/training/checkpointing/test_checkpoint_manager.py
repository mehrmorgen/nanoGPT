from __future__ import annotations
from pathlib import Path

import pytest
import torch

from ml_playground.core.error_handling import CheckpointError
from ml_playground.training.checkpointing.checkpoint_manager import (
    Checkpoint,
    CheckpointManager,
    _atomic_save,
)


class _FakeLogger:
    def __init__(self) -> None:
        self.warnings: list[str] = []
        self.infos: list[str] = []
        self.errors: list[str] = []

    def warning(self, msg: str, *args: object, **kwargs: object) -> None:
        self.warnings.append(msg)

    def info(self, msg: str, *args: object, **kwargs: object) -> None:
        self.infos.append(msg)

    def error(self, msg: str, *args: object, **kwargs: object) -> None:
        self.errors.append(msg)


class _FakeDeps:
    def __init__(self, *, missing_ok: bool = True) -> None:
        self.unlink_calls: list[Path] = []
        self.stat_calls: list[Path] = []
        self.unlink_supports_missing_ok = missing_ok

    def path_stat(self, path: Path):
        self.stat_calls.append(path)

        class Stat:
            st_mtime = 1.0

        return Stat()

    def path_unlink(self, path: Path) -> None:
        self.unlink_calls.append(path)


@pytest.fixture
def tmp_out(tmp_path: Path) -> Path:
    out = tmp_path / "ckpts"
    out.mkdir(parents=True, exist_ok=True)
    return out


def test_strict_domain_counter_label_required(tmp_out: Path) -> None:
    with pytest.raises(CheckpointError, match="counter label"):
        CheckpointManager(
            out_dir=tmp_out,
            keep_last=1,
            keep_best=0,
            naming_policy="domain",
            counter_label=None,
        )


def test_strict_naming_raises_on_unexpected_last_filename(tmp_out: Path) -> None:
    bad = tmp_out / "ckpt_last_other_1.pt"
    bad.write_text("x")
    deps = _FakeDeps()
    with pytest.raises(CheckpointError, match="Unexpected checkpoint name"):
        CheckpointManager(
            out_dir=tmp_out,
            keep_last=1,
            keep_best=0,
            naming_policy="domain",
            counter_label="games",
            strict_naming=True,
            deps=deps,  # type: ignore[arg-type]
        )


def test_strict_naming_raises_on_unexpected_best_filename(tmp_out: Path) -> None:
    bad = tmp_out / "ckpt_best_other_1.pt"
    bad.write_text("x")
    deps = _FakeDeps()
    with pytest.raises(CheckpointError, match="Unexpected checkpoint name"):
        CheckpointManager(
            out_dir=tmp_out,
            keep_last=0,
            keep_best=1,
            naming_policy="domain",
            counter_label="games",
            strict_naming=True,
            deps=deps,  # type: ignore[arg-type]
        )


def test_parse_last_counter_raises_on_non_int(tmp_out: Path) -> None:
    bad = tmp_out / "ckpt_last_abc.pt"
    bad.write_text("x")
    deps = _FakeDeps()
    with pytest.raises(CheckpointError, match="Could not parse iteration"):
        CheckpointManager(out_dir=tmp_out, keep_last=1, keep_best=0, deps=deps)  # type: ignore[arg-type]


def test_parse_best_counter_raises_on_non_numeric(tmp_out: Path) -> None:
    bad = tmp_out / "ckpt_best_0001_notnum.pt"
    bad.write_text("x")
    deps = _FakeDeps()
    with pytest.raises(CheckpointError, match="Could not parse metric"):
        CheckpointManager(out_dir=tmp_out, keep_last=0, keep_best=1, deps=deps)  # type: ignore[arg-type]


def test_atomic_save_with_atomic_true(tmp_path: Path) -> None:
    """Test _atomic_save with atomic=True."""
    obj = {"test": "data"}
    path = tmp_path / "test.pt"

    _atomic_save(obj, path, atomic=True)

    # File should exist
    assert path.exists()

    # Temporary file should not exist
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    assert not tmp_path.exists()


def test_atomic_save_with_atomic_false(tmp_path: Path) -> None:
    """Test _atomic_save with atomic=False."""
    obj = {"test": "data"}
    path = tmp_path / "test.pt"

    _atomic_save(obj, path, atomic=False)

    # File should exist
    assert path.exists()


def test_checkpoint_manager_init_with_steps_policy(tmp_path: Path) -> None:
    """Test CheckpointManager initialization with steps naming policy."""
    out_dir = tmp_path / "ckpts"
    out_dir.mkdir()

    manager = CheckpointManager(
        out_dir=out_dir,
        keep_last=5,
        keep_best=3,
        naming_policy="steps",
        counter_label=None,
    )

    assert manager.out_dir == out_dir
    assert manager.keep_last == 5
    assert manager.keep_best == 3
    assert manager.naming_policy == "steps"
    assert manager.counter_label is None


def test_checkpoint_manager_init_with_domain_policy_and_label(tmp_path: Path) -> None:
    """Test CheckpointManager initialization with domain policy and label."""
    out_dir = tmp_path / "ckpts"
    out_dir.mkdir()

    manager = CheckpointManager(
        out_dir=out_dir,
        keep_last=2,
        keep_best=1,
        naming_policy="domain",
        counter_label="train",
    )

    assert manager.out_dir == out_dir
    assert manager.keep_last == 2
    assert manager.keep_best == 1
    assert manager.naming_policy == "domain"
    assert manager.counter_label == "train"


def test_checkpoint_manager_save_checkpoint_best_only(tmp_path: Path) -> None:
    """Test save_checkpoint with is_best=True and keep_best=1."""
    out_dir = tmp_path / "ckpts"
    out_dir.mkdir()

    manager = CheckpointManager(
        out_dir=out_dir,
        keep_last=0,  # Don't keep any last checkpoints
        keep_best=1,
        naming_policy="steps",
        counter_label=None,
    )

    checkpoint = Checkpoint(
        model={"param": torch.tensor([1.0])},
        optimizer={"state": {}},
        model_args={},
        iter_num=100,
        best_val_loss=0.5,
        config={},
        ema=None,
    )

    manager.save_checkpoint(checkpoint, "ckpt", 0.5, 100, _FakeLogger(), is_best=True)

    # Should have created best checkpoint
    files = list(out_dir.glob("*.pt"))
    assert len(files) == 1
    assert "best" in files[0].name


def test_checkpoint_manager_save_checkpoint_last_only(tmp_path: Path) -> None:
    """Test save_checkpoint with is_best=False and keep_last=1."""
    out_dir = tmp_path / "ckpts"
    out_dir.mkdir()

    manager = CheckpointManager(
        out_dir=out_dir,
        keep_last=1,
        keep_best=0,  # Don't keep any best checkpoints
        naming_policy="steps",
        counter_label=None,
    )

    checkpoint = Checkpoint(
        model={"param": torch.tensor([1.0])},
        optimizer={"state": {}},
        model_args={},
        iter_num=50,
        best_val_loss=0.8,
        config={},
        ema=None,
    )

    manager.save_checkpoint(checkpoint, "ckpt", 0.8, 50, _FakeLogger(), is_best=False)

    # Should have created last checkpoint
    files = list(out_dir.glob("*.pt"))
    assert len(files) == 1
    assert "last" in files[0].name


def test_checkpoint_manager_save_checkpoint_both(tmp_path: Path) -> None:
    """Test save_checkpoint with both is_best=True and keep_last=1."""
    out_dir = tmp_path / "ckpts"
    out_dir.mkdir()

    manager = CheckpointManager(
        out_dir=out_dir,
        keep_last=1,
        keep_best=1,
        naming_policy="steps",
        counter_label=None,
    )

    checkpoint = Checkpoint(
        model={"param": torch.tensor([1.0])},
        optimizer={"state": {}},
        model_args={},
        iter_num=75,
        best_val_loss=0.3,
        config={},
        ema=None,
    )

    manager.save_checkpoint(checkpoint, "ckpt", 0.3, 75, _FakeLogger(), is_best=True)

    # Should have created only best checkpoint (is_best=True means no last checkpoint)
    files = list(out_dir.glob("*.pt"))
    assert len(files) == 1
    assert "best" in files[0].name


def test_checkpoint_manager_cleanup_old_checkpoints(tmp_path: Path) -> None:
    """Test that old checkpoints are cleaned up when limits are exceeded."""
    out_dir = tmp_path / "ckpts"
    out_dir.mkdir()

    manager = CheckpointManager(
        out_dir=out_dir,
        keep_last=2,  # Keep only 2 last checkpoints
        keep_best=0,
        naming_policy="steps",
        counter_label=None,
    )

    # Save 3 checkpoints
    for i in range(3):
        checkpoint = Checkpoint(
            model={"param": torch.tensor([float(i)])},
            optimizer={"state": {}},
            model_args={},
            iter_num=i * 10,
            best_val_loss=1.0,
            config={},
            ema=None,
        )
        manager.save_checkpoint(
            checkpoint, "ckpt", 1.0, i * 10, _FakeLogger(), is_best=False
        )

    # Should only keep 2 files
    files = list(out_dir.glob("*.pt"))
    assert len(files) == 2


def test_checkpoint_manager_load_checkpoint_best(tmp_path: Path) -> None:
    """Test load_checkpoint with best policy."""
    out_dir = tmp_path / "ckpts"
    out_dir.mkdir()

    manager = CheckpointManager(
        out_dir=out_dir,
        keep_last=1,
        keep_best=1,
        naming_policy="steps",
        counter_label=None,
    )

    # Save a best checkpoint
    checkpoint = Checkpoint(
        model={"param": torch.tensor([42.0])},
        optimizer={"state": {"lr": 0.01}},
        model_args={},
        iter_num=100,
        best_val_loss=0.1,
        config={"test": True},
        ema=None,
    )

    manager.save_checkpoint(checkpoint, "ckpt", 0.1, 100, _FakeLogger(), is_best=True)

    # Load the checkpoint
    loaded = manager.load_best_checkpoint("cpu", _FakeLogger())

    assert loaded is not None
    assert loaded.iter_num == 100
    assert loaded.best_val_loss == 0.1
    assert loaded.config == {"test": True}


def test_checkpoint_manager_load_checkpoint_latest(tmp_path: Path) -> None:
    """Test load_checkpoint with latest policy."""
    out_dir = tmp_path / "ckpts"
    out_dir.mkdir()

    manager = CheckpointManager(
        out_dir=out_dir,
        keep_last=2,
        keep_best=0,
        naming_policy="steps",
        counter_label=None,
    )

    # Save multiple checkpoints
    for i in range(3):
        checkpoint = Checkpoint(
            model={"param": torch.tensor([float(i)])},
            optimizer={"state": {}},
            model_args={},
            iter_num=i * 10,
            best_val_loss=1.0,
            config={},
            ema=None,
        )
        manager.save_checkpoint(
            checkpoint, "ckpt", 1.0, i * 10, _FakeLogger(), is_best=False
        )

    # Load the latest checkpoint
    loaded = manager.load_latest_checkpoint("cpu", _FakeLogger())

    assert loaded is not None
    assert loaded.iter_num == 20  # Latest iteration number


def test_checkpoint_manager_load_checkpoint_none_exist(tmp_path: Path) -> None:
    """Test load_checkpoint when no checkpoints exist."""
    out_dir = tmp_path / "ckpts"
    out_dir.mkdir()

    manager = CheckpointManager(
        out_dir=out_dir,
        keep_last=1,
        keep_best=1,
        naming_policy="steps",
        counter_label=None,
    )

    # Try to load when no checkpoints exist - should raise error
    with pytest.raises(CheckpointError, match="No best checkpoints discovered"):
        manager.load_best_checkpoint("cpu", _FakeLogger())

    with pytest.raises(CheckpointError, match="No last checkpoints discovered"):
        manager.load_latest_checkpoint("cpu", _FakeLogger())


def test_checkpoint_manager_load_checkpoint_corrupted_file(tmp_path: Path) -> None:
    """Test load_checkpoint with corrupted checkpoint file."""
    out_dir = tmp_path / "ckpts"
    out_dir.mkdir()

    manager = CheckpointManager(
        out_dir=out_dir,
        keep_last=1,
        keep_best=1,
        naming_policy="steps",
        counter_label=None,
    )

    # Create a corrupted checkpoint file
    corrupt_file = out_dir / "ckpt_best_00000010_0.500000.pt"
    corrupt_file.write_text("not a valid checkpoint")

    # Try to load - should raise CheckpointLoadError
    from ml_playground.core.error_handling import CheckpointLoadError

    with pytest.raises(CheckpointLoadError, match="Failed to load best checkpoint"):
        manager.load_best_checkpoint("cpu", _FakeLogger())


def test_checkpoint_manager_domain_naming_with_counter_label(tmp_path: Path) -> None:
    """Test domain naming policy with counter label."""
    out_dir = tmp_path / "ckpts"
    out_dir.mkdir()

    manager = CheckpointManager(
        out_dir=out_dir,
        keep_last=1,
        keep_best=1,
        naming_policy="domain",
        counter_label="train",
    )

    checkpoint = Checkpoint(
        model={"param": torch.tensor([1.0])},
        optimizer={"state": {}},
        model_args={},
        iter_num=50,
        best_val_loss=0.5,
        config={},
        ema=None,
    )

    manager.save_checkpoint(checkpoint, "ckpt", 0.5, 50, _FakeLogger(), is_best=True)

    # Check filename contains domain label
    files = list(out_dir.glob("*.pt"))
    assert len(files) == 1
    assert "train" in files[0].name


def test_checkpoint_manager_save_checkpoint_with_ema(tmp_path: Path) -> None:
    """Test save_checkpoint with EMA data."""
    out_dir = tmp_path / "ckpts"
    out_dir.mkdir()

    manager = CheckpointManager(
        out_dir=out_dir,
        keep_last=1,
        keep_best=0,
        naming_policy="steps",
        counter_label=None,
    )

    ema_shadow = {"param": torch.tensor([0.5])}

    checkpoint = Checkpoint(
        model={"param": torch.tensor([1.0])},
        optimizer={"state": {}},
        model_args={},
        iter_num=100,
        best_val_loss=0.5,
        config={},
        ema=ema_shadow,
    )

    manager.save_checkpoint(checkpoint, "ckpt", 0.5, 100, _FakeLogger(), is_best=False)

    # Load and verify EMA data is preserved
    loaded = manager.load_latest_checkpoint("cpu", _FakeLogger())
    assert loaded is not None
    assert loaded.ema == ema_shadow


def test_checkpoint_manager_atomic_save_enabled(tmp_path: Path) -> None:
    """Test that atomic save is used when enabled."""
    out_dir = tmp_path / "ckpts"
    out_dir.mkdir()

    manager = CheckpointManager(
        out_dir=out_dir,
        keep_last=1,
        keep_best=0,
        naming_policy="steps",
        counter_label=None,
        atomic=True,  # Correct parameter name
    )

    checkpoint = Checkpoint(
        model={"param": torch.tensor([1.0])},
        optimizer={"state": {}},
        model_args={},
        iter_num=10,
        best_val_loss=1.0,
        config={},
        ema=None,
    )

    manager.save_checkpoint(checkpoint, "ckpt", 1.0, 10, _FakeLogger(), is_best=False)

    # File should exist and no temp files should remain
    files = list(out_dir.glob("*"))
    pt_files = [f for f in files if f.suffix == ".pt"]
    tmp_files = [f for f in files if ".tmp" in f.name]

    assert len(pt_files) == 1
    assert len(tmp_files) == 0


def test_checkpoint_manager_list_checkpoints_empty(tmp_path: Path) -> None:
    """Test that checkpoint lists are empty when directory is empty."""
    out_dir = tmp_path / "ckpts"
    out_dir.mkdir()

    manager = CheckpointManager(
        out_dir=out_dir,
        keep_last=1,
        keep_best=1,
        naming_policy="steps",
        counter_label=None,
    )

    # Check that lists are empty
    assert len(manager.last_checkpoints) == 0
    assert len(manager.best_checkpoints) == 0


def test_checkpoint_manager_list_checkpoints_with_files(tmp_path: Path) -> None:
    """Test that checkpoints are discovered from files."""
    out_dir = tmp_path / "ckpts"
    out_dir.mkdir()

    manager = CheckpointManager(
        out_dir=out_dir,
        keep_last=1,
        keep_best=1,
        naming_policy="steps",
        counter_label=None,
    )

    # Create some checkpoint files manually
    (out_dir / "ckpt_last_00000010.pt").write_text("fake")
    (out_dir / "ckpt_best_00000010_0.500000.pt").write_text("fake")
    (out_dir / "not_checkpoint.txt").write_text("fake")  # Should be ignored

    # Re-discover checkpoints
    manager._discover_existing()

    assert len(manager.last_checkpoints) == 1
    assert len(manager.best_checkpoints) == 1
    assert manager.last_checkpoints[0].iter_num == 10
    assert manager.best_checkpoints[0].metric == 0.5
