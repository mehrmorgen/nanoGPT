from __future__ import annotations

from typing import Any, Iterable, Sequence

import torch
import torch.nn as nn

from ml_playground.models.core import optimization


def _make_tiny_model() -> nn.Module:
    return nn.Sequential(
        nn.Linear(4, 4, bias=True),
        nn.ReLU(),
    )


class _ListLogger:
    def __init__(self) -> None:
        self.messages: list[str] = []

    def info(self, message: str, *args: Any) -> None:
        formatted = message % args if args else message
        self.messages.append(formatted)


def test_configure_optimizers_uses_default_factory_and_logs() -> None:
    model = _make_tiny_model()
    logger = _ListLogger()
    optimizer = optimization.configure_optimizers(
        model,
        weight_decay=0.1,
        learning_rate=0.01,
        betas=(0.9, 0.95),
        device_type="cuda",
        logger=logger,
    )

    assert isinstance(optimizer, torch.optim.AdamW)
    assert optimizer.defaults["lr"] == 0.01
    assert optimizer.defaults["betas"] == (0.9, 0.95)
    assert len(optimizer.param_groups) == 2
    assert optimizer.param_groups[0]["weight_decay"] == 0.1
    assert optimizer.param_groups[1]["weight_decay"] == 0.0
    assert any("decayed parameter tensors" in msg for msg in logger.messages)
    assert any("non-decayed parameter tensors" in msg for msg in logger.messages)


def test_configure_optimizers_accepts_custom_factory() -> None:
    model = _make_tiny_model()
    captured: dict[str, Any] = {}

    def factory(
        params: Iterable[torch.nn.Parameter] | optimization.ParamGroups,
        *,
        lr: float,
        betas: Sequence[float],
        fused: bool | None = None,
    ) -> torch.optim.Optimizer:
        captured["params"] = params
        captured["lr"] = lr
        captured["betas"] = tuple(betas)
        captured["fused"] = fused
        adamw_kwargs: dict[str, Any] = {"lr": lr, "betas": tuple(betas)}
        if fused is not None:
            adamw_kwargs["fused"] = fused
        return torch.optim.AdamW(params, **adamw_kwargs)

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
