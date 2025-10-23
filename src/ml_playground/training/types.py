"""Shared training-time protocols and type aliases."""

from __future__ import annotations

from typing import Any, Callable, Protocol

import torch


__all__ = ["TensorboardWriter", "VectorizeFn", "ScaledLoss", "OptimizerLike"]


class TensorboardWriter(Protocol):
    """Protocol capturing the subset of TensorBoard writer methods we rely on."""

    def add_scalar(
        self,
        tag: str,  # noqa: F841
        scalar_value: float,  # noqa: F841
        global_step: int | None = None,  # noqa: F841
        *,
        walltime: float | None = None,  # noqa: F841
        new_style: bool = False,  # noqa: F841
        double_precision: bool = False,  # noqa: F841
    ) -> None: ...

    def close(self) -> None: ...


VectorizeFn = Callable[
    [Callable[[torch.Tensor, torch.Tensor], torch.Tensor]],
    Callable[[torch.Tensor, torch.Tensor], torch.Tensor],
]


class ScaledLoss(Protocol):
    """Protocol for scaled losses returned by ``GradScaler.scale``."""

    def backward(self) -> None: ...


class OptimizerLike(Protocol):
    """Structural protocol for optimizer objects used by the trainer."""

    param_groups: list[dict[str, Any]]

    def state_dict(self) -> dict[str, Any]: ...

    def load_state_dict(self, state_dict: dict[str, Any]) -> None: ...

    def zero_grad(self, *, set_to_none: bool = True) -> None: ...  # noqa: F841

    def step(self) -> Any: ...
