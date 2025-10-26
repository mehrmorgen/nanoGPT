from __future__ import annotations

from contextlib import nullcontext
from typing import Any, Iterable

import torch
from torch import optim

from ml_playground.configuration.models import DeviceKind, ModelConfig
from ml_playground.core.logging_protocol import LoggerLike
from ml_playground.models.core.model import GPT
from ml_playground.training.ema import EMA
from ml_playground.training.hooks.evaluation import SimpleBatches
from ml_playground.training.types import TensorboardWriter


class LoggerStub(LoggerLike):
    """Minimal LoggerLike implementation for unit tests."""

    def __init__(self) -> None:
        self.infos: list[str] = []
        self.debugs: list[str] = []
        self.warnings: list[str] = []
        self.errors: list[str] = []
        self.messages: list[str] = []

    def info(self, msg: str, *args: object, **kwargs: object) -> None:
        del kwargs
        message = msg % args if args else msg
        self.infos.append(message)
        self.messages.append(message)

    def debug(self, msg: str, *args: object, **kwargs: object) -> None:
        del kwargs
        message = msg % args if args else msg
        self.debugs.append(message)
        self.messages.append(message)

    def warning(self, msg: str, *args: object, **kwargs: object) -> None:
        del kwargs
        message = msg % args if args else msg
        self.warnings.append(message)
        self.messages.append(message)

    def error(self, msg: str, *args: object, **kwargs: object) -> None:
        del kwargs
        message = msg % args if args else msg
        self.errors.append(message)
        self.messages.append(message)


class TensorboardWriterStub(TensorboardWriter):
    """TensorboardWriter that records scalar writes for assertions."""

    def __init__(self) -> None:
        self.entries: list[tuple[str, float, int]] = []

    def add_scalar(
        self,
        tag: str,
        scalar_value: float,
        global_step: int | None = None,
        *,
        walltime: float | None = None,
        new_style: bool = False,
        double_precision: bool = False,
    ) -> None:
        del walltime, new_style, double_precision
        self.entries.append((tag, scalar_value, global_step or 0))

    def close(self) -> None:
        pass


class SimpleBatchesStub(SimpleBatches):
    """SimpleBatches implementation that produces zero tensors."""

    def __init__(self, device: DeviceKind = "cpu") -> None:
        self.device: DeviceKind = device

    def get_batch(self, split: str) -> tuple[torch.Tensor, torch.Tensor]:
        del split
        return torch.zeros((1, 1), device=self.device), torch.zeros(
            (1, 1), device=self.device
        )


def make_minimal_gpt() -> GPT:
    cfg = ModelConfig(
        n_layer=1,
        n_head=1,
        n_embd=4,
        block_size=4,
        dropout=0.0,
        vocab_size=50,
    )
    logger = LoggerStub()
    return GPT(cfg, logger)


def make_optimizer(
    parameters: Iterable[torch.nn.Parameter] | None = None,
) -> optim.Optimizer:
    params = (
        list(parameters)
        if parameters is not None
        else [torch.nn.Parameter(torch.zeros(1))]
    )
    return optim.SGD(params, lr=0.0)


def autocast_context() -> Any:
    return nullcontext()


def make_ema(
    model: GPT | None = None,
    *,
    decay: float = 0.999,
    device: DeviceKind = "cpu",
) -> EMA:
    base_model = model or make_minimal_gpt()
    return EMA(base_model, decay=decay, device=device)
