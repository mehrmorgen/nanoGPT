from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
import logging
from typing import Callable, ContextManager, Protocol, Any

import torch

from ml_playground.framework.configuration.models import RuntimeConfig
from ml_playground.framework.core.runtime_context import RuntimeContext, runtime_context

__all__ = [
    "HookEvent",
    "ExperimentHook",
    "ExperimentHookContext",
    "ExperimentHooks",
    "build_hook_context",
]


class HookEvent(str, Enum):
    PREPARE = "prepare"
    TRAIN = "train"
    SAMPLE = "sample"
    CUSTOM = "custom"


@dataclass(frozen=True)
class ExperimentHookContext:
    experiment: str
    runtime: RuntimeContext


class ExperimentHook(Protocol):
    def handle(self, event: HookEvent, context: ExperimentHookContext) -> None: ...


@dataclass(slots=True)
class ExperimentHooks:
    hooks: list[ExperimentHook] = field(default_factory=list)

    def register(self, hook: ExperimentHook) -> None:
        self.hooks.append(hook)

    def run(self, event: HookEvent, context: ExperimentHookContext) -> None:
        for hook in self.hooks:
            hook.handle(event, context)


def build_hook_context(
    experiment: str,
    runtime: RuntimeConfig,
    *,
    logger_name: str | None = None,
    logger_level: int = logging.INFO,
    stream_handler_factory: Callable[[], logging.Handler] | None = None,
    cuda_available_fn: Callable[[], bool] | None = None,
    cuda_manual_seed_fn: Callable[[int], None] | None = None,
    autocast_factory: Callable[[str, torch.dtype], ContextManager[Any]] | None = None,
) -> ExperimentHookContext:
    """Build hook context with the standard runtime context helper."""
    name = logger_name or f"ml_playground.experiment.{experiment}"
    runtime_ctx = runtime_context(
        runtime,
        logger_name=name,
        logger_level=logger_level,
        stream_handler_factory=stream_handler_factory,
        cuda_available_fn=cuda_available_fn,
        cuda_manual_seed_fn=cuda_manual_seed_fn,
        autocast_factory=autocast_factory,
    )
    return ExperimentHookContext(experiment=experiment, runtime=runtime_ctx)
