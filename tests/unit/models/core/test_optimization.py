from __future__ import annotations

from typing import Any, Sequence

import torch

from ml_playground.models.core import optimization


class _TinyModel(torch.nn.Module):
    def __init__(self) -> None:
        super().__init__()
        self.linear = torch.nn.Linear(4, 4, bias=True)
        self.scalar = torch.nn.Parameter(torch.tensor(1.0))


class _ListLogger:
    def __init__(self) -> None:
        self.messages: list[str] = []

    def info(self, message: str, *args: Any) -> None:
        self.messages.append(message % args if args else message)


def test_configure_optimizers_uses_default_factory_and_logs() -> None:
    model = _TinyModel()
    logger = _ListLogger()

    optimizer = optimization.configure_optimizers(
        model,
        weight_decay=0.1,
        learning_rate=0.01,
        betas=(0.9, 0.95),
        device_type="cpu",
        logger=logger,
    )

    assert isinstance(optimizer, torch.optim.AdamW)
    assert optimizer.defaults["lr"] == 0.01
    assert optimizer.defaults["betas"] == (0.9, 0.95)
    assert any("decayed parameter tensors" in msg for msg in logger.messages)
    assert any("non-decayed parameter tensors" in msg for msg in logger.messages)


def test_configure_optimizers_accepts_custom_factory() -> None:
    model = _TinyModel()
    captured: dict[str, Any] = {}

    class DummyOptimizer(torch.optim.Optimizer):
        def __init__(self, params):  # type: ignore[override]
            super().__init__(params, {})

        def step(self, closure=None):  # type: ignore[override]
            return None

        def zero_grad(self, set_to_none: bool = True):  # type: ignore[override]
            return None

    def factory(
        params: optimization.ParamGroups,
        *,
        lr: float,
        betas: Sequence[float],
        fused: bool | None = None,
    ) -> DummyOptimizer:
        captured["params"] = params
        captured["lr"] = lr
        captured["betas"] = tuple(betas)
        captured["fused"] = fused
        flat_params: list[torch.nn.Parameter] = []
        for group in params:
            flat_params.extend(group["params"])
        return DummyOptimizer(flat_params)

    optimizer = optimization.configure_optimizers(
        model,
        weight_decay=0.2,
        learning_rate=0.001,
        betas=(0.8, 0.88),
        device_type="cpu",
        factory=factory,
    )

    assert isinstance(optimizer, torch.optim.Optimizer)
    assert captured["lr"] == 0.001
    assert captured["betas"] == (0.8, 0.88)
    assert captured["fused"] is None


def test_configure_optimizers_sets_fused_for_cuda() -> None:
    """CUDA device type requests fused optimizer when supported."""
    model = _TinyModel()
    captured: dict[str, Any] = {}

    class DummyOptimizer(torch.optim.Optimizer):
        def __init__(self, params):  # type: ignore[override]
            super().__init__(params, {})

        def step(self, closure=None):  # type: ignore[override]
            return None

        def zero_grad(self, set_to_none: bool = True):  # type: ignore[override]
            return None

    def factory(
        params: optimization.ParamGroups,
        *,
        lr: float,
        betas: Sequence[float],
        fused: bool | None = None,
    ) -> DummyOptimizer:
        captured["fused"] = fused
        flat_params: list[torch.nn.Parameter] = []
        for group in params:
            flat_params.extend(group["params"])
        return DummyOptimizer(flat_params)

    optimizer = optimization.configure_optimizers(
        model,
        weight_decay=0.1,
        learning_rate=0.01,
        betas=(0.9, 0.95),
        device_type="cuda",
        factory=factory,
    )

    assert isinstance(optimizer, torch.optim.Optimizer)
    assert captured["fused"] is True
