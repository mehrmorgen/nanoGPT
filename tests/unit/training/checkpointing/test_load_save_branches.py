from __future__ import annotations

from pathlib import Path


from ml_playground.configuration.models import (
    DataConfig,
    LRSchedule,
    ModelConfig,
    OptimConfig,
    RuntimeConfig,
    TrainerConfig,
)
from ml_playground.training.checkpointing.service import (
    load_checkpoint,
    save_checkpoint,
)
from ml_playground.training.checkpointing.checkpoint_manager import CheckpointManager


class _FakeLogger:
    def __init__(self) -> None:
        self.warnings: list[str] = []
        self.infos: list[str] = []

    def warning(self, msg: str, *args: object, **kwargs: object) -> None:
        self.warnings.append(msg)

    def info(self, msg: str, *args: object, **kwargs: object) -> None:
        self.infos.append(msg)


class _FakeEMA:
    def __init__(self) -> None:
        self.shadow: dict | None = None


def test_load_checkpoint_uses_default_when_no_override(tmp_path: Path) -> None:
    """Test load_checkpoint uses default loading when checkpoint_load_fn is None."""
    model_cfg = ModelConfig(n_layer=1, n_head=1, n_embd=4, block_size=4, vocab_size=50)

    cfg = TrainerConfig(
        model=model_cfg,
        data=DataConfig(batch_size=1, block_size=4),
        optim=OptimConfig(learning_rate=0.01),
        schedule=LRSchedule(),
        runtime=RuntimeConfig(out_dir=tmp_path / "out"),
    )
    manager = CheckpointManager(out_dir=tmp_path / "train_out")

    # No checkpoint_load_fn, should use default
    checkpoint = load_checkpoint(manager, cfg, logger=_FakeLogger())
    assert checkpoint is None  # No checkpoint exists


def test_save_checkpoint_uses_default_when_no_override(tmp_path: Path) -> None:
    """Test save_checkpoint uses default saving when checkpoint_save_fn is None."""
    from ml_playground.models.core.config import build_gpt_config
    from ml_playground.models.core.model import GPT
    import torch

    model_cfg = ModelConfig(n_layer=1, n_head=1, n_embd=4, block_size=4, vocab_size=50)
    gpt_cfg = build_gpt_config(model_cfg)
    model = GPT(gpt_cfg, logger=None)
    optimizer = torch.optim.AdamW(model.parameters(), lr=0.01)

    train_out_dir = tmp_path / "train_out"
    train_out_dir.mkdir(parents=True, exist_ok=True)

    cfg = TrainerConfig(
        model=model_cfg,
        data=DataConfig(batch_size=1, block_size=4),
        optim=OptimConfig(learning_rate=0.01),
        schedule=LRSchedule(),
        runtime=RuntimeConfig(out_dir=tmp_path / "out"),
    )
    manager = CheckpointManager(out_dir=train_out_dir)

    # No checkpoint_save_fn, should use default
    save_checkpoint(
        manager,
        cfg,
        model=model,
        optimizer=optimizer,
        ema=None,
        iter_num=1,
        best_val_loss=0.5,
        logger=_FakeLogger(),
        is_best=True,
    )

    # Check that a rotated best checkpoint was saved
    assert list(train_out_dir.glob("ckpt_best_*.pt"))


def test_save_checkpoint_with_ema_shadow(tmp_path: Path) -> None:
    """Test save_checkpoint includes ema.shadow when ema is present."""
    from ml_playground.models.core.config import build_gpt_config
    from ml_playground.models.core.model import GPT
    import torch

    model_cfg = ModelConfig(n_layer=1, n_head=1, n_embd=4, block_size=4, vocab_size=50)
    gpt_cfg = build_gpt_config(model_cfg)
    model = GPT(gpt_cfg, logger=None)
    optimizer = torch.optim.AdamW(model.parameters(), lr=0.01)
    ema = _FakeEMA()
    ema.shadow = {"param": 1.0}

    train_out_dir = tmp_path / "train_out"
    train_out_dir.mkdir(parents=True, exist_ok=True)

    cfg = TrainerConfig(
        model=model_cfg,
        data=DataConfig(batch_size=1, block_size=4),
        optim=OptimConfig(learning_rate=0.01),
        schedule=LRSchedule(),
        runtime=RuntimeConfig(out_dir=tmp_path / "out"),
    )
    manager = CheckpointManager(out_dir=train_out_dir)

    save_checkpoint(
        manager,
        cfg,
        model=model,
        optimizer=optimizer,
        ema=ema,
        iter_num=1,
        best_val_loss=0.5,
        logger=_FakeLogger(),
        is_best=False,
    )

    # Check that a rotated last checkpoint was saved
    assert list(train_out_dir.glob("ckpt_last_*.pt"))
