from __future__ import annotations

import torch

from ml_playground.models.utils.estimator import estimate_loss
from ml_playground.configuration.models import ModelConfig
from ml_playground.models.core.model import GPT
from tests.unit.training._helpers import LoggerStub, SimpleBatchesStub


def test_estimate_loss_computes_train_and_val_metrics() -> None:
    """estimate_loss should compute train and validation metrics correctly."""

    cfg = ModelConfig(
        n_layer=1,
        n_head=1,
        n_embd=16,
        block_size=8,
        dropout=0.0,
        vocab_size=10,
    )

    class FakeGPT(GPT):
        def __init__(self) -> None:
            super().__init__(cfg, LoggerStub())

        def forward(  # type: ignore[override]
            self, x: torch.Tensor, targets: torch.Tensor | None = None
        ) -> tuple[torch.Tensor, torch.Tensor]:
            logits = torch.nn.functional.one_hot(
                x, num_classes=self.config.vocab_size
            ).to(torch.float32)
            loss = torch.tensor(0.5, dtype=torch.float32)
            return logits, loss

    class FakeBatches(SimpleBatchesStub):
        def get_batch(self, split: str) -> tuple[torch.Tensor, torch.Tensor]:
            del split
            batch_size, seq_len, vocab_size = 2, 3, 10
            x = torch.randint(0, vocab_size, (batch_size, seq_len), device="cpu")
            y = torch.randint(0, vocab_size, (batch_size, seq_len), device="cpu")
            return x, y

    model = FakeGPT()
    batches = FakeBatches()

    # Test the function
    results = estimate_loss(
        model=model,
        batches=batches,
        eval_iters=2,
        ctx=torch.no_grad(),
    )

    # Should return dict with train and val losses
    assert "train" in results
    assert "val" in results
    assert isinstance(results["train"], float)
    assert isinstance(results["val"], float)
    assert results["train"] >= 0.0
    assert results["val"] >= 0.0
