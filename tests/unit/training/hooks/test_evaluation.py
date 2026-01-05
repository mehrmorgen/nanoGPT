from __future__ import annotations

from typing import cast

import torch

from ml_playground.configuration.models import TrainerConfig, ModelConfig
from ml_playground.data_pipeline.sampling.batches import SimpleBatches
from ml_playground.models.core.model import GPT
from ml_playground.training.hooks.evaluation import run_evaluation


class MockLogger:
    def __init__(self):
        self.infos = []

    def info(self, msg):
        self.infos.append(msg)


def test_run_evaluation_basic() -> None:
    model_cfg = ModelConfig(
        n_layer=1, n_head=1, n_embd=32, block_size=16, vocab_size=100
    )
    model = GPT(model_cfg, logger=None)

    class MockConfig:
        def __init__(self):
            class Runtime:
                def __init__(self):
                    self.eval_iters = 1

            self.runtime = Runtime()

    class MockBatches:
        def get_batch(self, split: str):
            return torch.zeros((1, 16), dtype=torch.long), torch.zeros(
                (1, 16), dtype=torch.long
            )

    def mock_estimate_loss(model, batches, iters, ctx):
        return {"train": 0.5, "val": 0.6}

    logger = MockLogger()

    losses = run_evaluation(
        cast(TrainerConfig, MockConfig()),
        logger=logger,
        iter_num=100,
        lr=0.001,
        raw_model=model,
        batches=cast(SimpleBatches, MockBatches()),
        ctx=None,
        estimate_loss_fn=mock_estimate_loss,
    )

    assert losses == {"train": 0.5, "val": 0.6}
    assert any(
        "step 100: train loss 0.5000, val loss 0.6000" in m for m in logger.infos
    )
