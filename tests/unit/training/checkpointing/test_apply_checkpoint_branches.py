from __future__ import annotations


import torch

from ml_playground.configuration.models import (
    ModelConfig,
)
from ml_playground.models.core.config import build_gpt_config
from ml_playground.models.core.model import GPT
from ml_playground.training.checkpointing.checkpoint_manager import Checkpoint
from ml_playground.training.checkpointing.service import apply_checkpoint


class _FakeLogger:
    def __init__(self) -> None:
        self.warnings: list[str] = []

    def warning(self, msg: str, *args: object, **kwargs: object) -> None:
        self.warnings.append(msg)


class _FakeEMA:
    def __init__(self) -> None:
        self.shadow: dict | None = None


def test_apply_checkpoint_with_ema_and_checkpoint_ema() -> None:
    """Test apply_checkpoint when both ema and checkpoint.ema are truthy."""
    model_cfg = ModelConfig(n_layer=1, n_head=1, n_embd=4, block_size=4, vocab_size=50)
    gpt_cfg = build_gpt_config(model_cfg)
    model = GPT(gpt_cfg, logger=None)
    optimizer = torch.optim.AdamW(model.parameters(), lr=0.01)
    ema = _FakeEMA()

    checkpoint = Checkpoint(
        model=model.state_dict(),
        optimizer=optimizer.state_dict(),
        model_args={},
        iter_num=5,
        best_val_loss=0.3,
        config={},
        ema={"param": 1.0},
    )

    iter_num, best = apply_checkpoint(
        checkpoint, model=model, optimizer=optimizer, ema=ema
    )

    assert iter_num == 5
    assert best == 0.3
    assert ema.shadow == checkpoint.ema
