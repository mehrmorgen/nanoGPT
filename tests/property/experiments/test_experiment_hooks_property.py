from __future__ import annotations

from contextlib import nullcontext
import io
import logging
from pathlib import Path

from typing import Any, cast
from hypothesis import given, settings, strategies as st

from ml_playground.framework.configuration.models import RuntimeConfig
from ml_playground.framework.core.runtime_context import RuntimeContext
from ml_playground.experiments.hooks import (
    ExperimentHookContext,
    ExperimentHooks,
    ExperimentHook,
    HookEvent,
    build_hook_context,
)


class _Recorder:
    def __init__(self, calls: list[HookEvent]) -> None:
        self._calls = calls

    def handle(self, event: HookEvent, context: ExperimentHookContext) -> None:
        del context
        self._calls.append(event)


@settings(max_examples=30, deadline=None, derandomize=True)
@given(  # type: ignore[reportAny]
    count=st.integers(min_value=1, max_value=8)
)
def test_experiment_hooks_run_invokes_all(count: int) -> None:
    """Hook runner invokes each registered hook exactly once."""
    calls: list[HookEvent] = []
    hooks = ExperimentHooks()
    for _ in range(count):
        hooks.register(_Recorder(calls))

    context = ExperimentHookContext(
        experiment="demo",
        runtime=RuntimeContext(
            device_type="cpu",
            autocast_context=nullcontext(),
            logger=logging.getLogger("test"),
        ),
    )
    hooks.run(HookEvent.TRAIN, context)

    assert calls == [HookEvent.TRAIN] * count


def test_build_hook_context_uses_experiment_logger_name() -> None:
    config = RuntimeConfig(out_dir=Path("out"))
    stream = io.StringIO()
    context = build_hook_context(
        "demo",
        config,
        logger_level=logging.INFO,
        stream_handler_factory=lambda: logging.StreamHandler(stream),
    )
    # LoggerLike does not have 'name', but real loggers do.
    assert cast(Any, context.runtime.logger).name == "ml_playground.experiment.demo"


def test_experiment_hook_protocol_placeholder_executes() -> None:
    """ExperimentHook protocol placeholder executes without error."""
    sentinel = object()
    context = ExperimentHookContext(
        experiment="demo",
        runtime=RuntimeContext(
            device_type="cpu",
            autocast_context=nullcontext(),
            logger=logging.getLogger("test"),
        ),
    )
    assert ExperimentHook.handle(sentinel, HookEvent.CUSTOM, context) is None  # type: ignore[arg-type]
