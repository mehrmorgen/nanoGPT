from __future__ import annotations

import torch

from ml_playground.models.utils.estimator import estimate_loss


def test_estimate_loss_computes_train_and_val_metrics() -> None:
    """estimate_loss should compute train and validation metrics correctly."""

    # Create a simple mock model that returns predictable outputs
    class MockModel(torch.nn.Module):
        def __init__(self):
            super().__init__()
            self.linear = torch.nn.Linear(10, 1)

        def forward(self, x, targets=None):
            # Return logits and loss if targets provided
            logits = self.linear(x.view(x.size(0) * x.size(1), -1)).view(
                x.size(0), x.size(1), -1
            )
            if targets is not None:
                loss = torch.nn.functional.cross_entropy(
                    logits.view(-1, logits.size(-1)), targets.view(-1)
                )
                return logits, loss
            return logits, None

    # Create mock batches function
    def mock_get_batch(split):
        batch_size, seq_len, vocab_size = 2, 3, 10
        x = torch.randint(0, vocab_size, (batch_size, seq_len))
        y = torch.randint(0, vocab_size, (batch_size, seq_len))
        return x, y

    # Create mock batches object
    class MockBatches:
        def get_batch(self, split):
            return mock_get_batch(split)

    model = MockModel()
    batches = MockBatches()

    # Test the function
    results = estimate_loss(
        model=model,
        batches=batches,
        eval_iters=2,
        ctx=torch.no_grad(),
        logger=None,
    )

    # Should return dict with train and val losses
    assert "train" in results
    assert "val" in results
    assert isinstance(results["train"], float)
    assert isinstance(results["val"], float)
    assert results["train"] >= 0.0
    assert results["val"] >= 0.0
