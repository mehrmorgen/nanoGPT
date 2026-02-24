"""Shared training-time protocols and type aliases."""

from __future__ import annotations

from typing import Any, Callable, Literal, Protocol

import torch


__all__ = [
    "TensorboardWriter",
    "VectorizeFn",
    "ScaledLoss",
    "OptimizerLike",
    "BatchProvider",
]


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

    param_groups: list[dict[str, object]]

    state_dict: Callable[..., Any]
    load_state_dict: Callable[..., Any]
    zero_grad: Callable[..., Any]
    step: Callable[..., Any]


class BatchProvider(Protocol):
    """Protocol for objects that yield training batches."""

    def get_batch(
        self, split: Literal["train", "val"]
    ) -> tuple[torch.Tensor, torch.Tensor]: ...
