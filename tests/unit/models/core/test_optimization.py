from __future__ import annotations

from typing import Any, Iterable, Sequence, cast

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


def _flatten_params(
    groups: optimization.ParamGroups | Iterable[torch.nn.Parameter],
) -> list[torch.nn.Parameter]:
    if not isinstance(groups, Sequence):
        return list(cast(Iterable[torch.nn.Parameter], groups))

    if len(groups) > 0 and isinstance(groups[0], dict):
        param_groups = cast(optimization.ParamGroups, groups)
        flat: list[torch.nn.Parameter] = []
        for group in param_groups:
            flat.extend(cast(Sequence[torch.nn.Parameter], group["params"]))
        return flat

    return list(cast(Sequence[torch.nn.Parameter], groups))


def test_configure_optimizers_uses_default_factory_and_logs() -> None:
    model = _TinyModel()
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
    model = _TinyModel()
    captured: dict[str, Any] = {}

    class DummyOptimizer(torch.optim.Optimizer):
        def __init__(self, params: Iterable[torch.nn.Parameter]) -> None:
            super().__init__(list(params), {})

        def step(self, closure: Any = None) -> None:  # type: ignore[override]
            del closure

        def zero_grad(self, set_to_none: bool = True) -> None:  # type: ignore[override]
            del set_to_none

    def factory(
        params: Iterable[torch.nn.Parameter] | optimization.ParamGroups,
        *,
        lr: float,
        betas: Sequence[float],
        fused: bool | None = None,
    ) -> DummyOptimizer:
        captured["params"] = params
        captured["lr"] = lr
        captured["betas"] = tuple(betas)
        captured["fused"] = fused
        return DummyOptimizer(_flatten_params(params))

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
