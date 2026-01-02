from __future__ import annotations

from contextlib import nullcontext
from pathlib import Path
from typing import Any, ContextManager
import logging
from tempfile import TemporaryDirectory

import torch
from hypothesis import HealthCheck, assume, given, settings, strategies as st

from ml_playground.configuration.models import RuntimeConfig
from ml_playground.core.runtime_context import runtime_context


@settings(
    max_examples=20,
    deadline=50,
    derandomize=True,
    suppress_health_check=[HealthCheck.function_scoped_fixture],
)
@given(
    device=st.sampled_from(["cpu", "cuda", "mps"]),
    dtype=st.sampled_from(["float32", "bfloat16", "float16"]),
    seed=st.integers(min_value=0, max_value=10_000),
)
def test_runtime_context_when_device_then_sets_device_type(
    device: str,
    dtype: str,
    seed: int,
) -> None:
    """Runtime context when device selected then sets device type."""
    assume(not (device == "mps" and dtype == "float16"))
    with TemporaryDirectory() as tmp_dir:
        runtime = RuntimeConfig(
            out_dir=Path(tmp_dir),
            device=device,
            dtype=dtype,
            seed=seed,
        )

        autocast_calls: list[tuple[str, torch.dtype]] = []

        def _autocast_factory(
            device_type: str, pt_dtype: torch.dtype
        ) -> ContextManager[Any]:
            autocast_calls.append((device_type, pt_dtype))
            return nullcontext()

        ctx = runtime_context(
            runtime,
            logger_name=f"ml_playground.runtime.{device}.{dtype}.{seed}",
            autocast_factory=_autocast_factory,
        )

        expected_device_type = "cuda" if device == "cuda" else "cpu"
        assert ctx.device_type == expected_device_type
        if expected_device_type == "cpu":
            assert autocast_calls == []
        else:
            assert autocast_calls != []
            assert autocast_calls[-1][0] == expected_device_type


@settings(
    max_examples=20,
    deadline=50,
    derandomize=True,
    suppress_health_check=[HealthCheck.function_scoped_fixture],
)
@given(seed=st.integers(min_value=0, max_value=10_000))
def test_runtime_context_when_cuda_available_then_seeds_cuda(seed: int) -> None:
    """Runtime context when cuda available then seeds cuda."""
    with TemporaryDirectory() as tmp_dir:
        runtime = RuntimeConfig(out_dir=Path(tmp_dir), device="cuda", seed=seed)

        called: list[int] = []

        def _cuda_available() -> bool:
            return True

        def _cuda_seed(value: int) -> None:
            called.append(value)

        runtime_context(
            runtime,
            logger_name=f"ml_playground.runtime.seed.{seed}",
            cuda_available_fn=_cuda_available,
            cuda_manual_seed_fn=_cuda_seed,
            autocast_factory=lambda *_: nullcontext(),
        )

        assert called == [seed]


@settings(
    max_examples=10,
    deadline=50,
    derandomize=True,
    suppress_health_check=[HealthCheck.function_scoped_fixture],
)
@given(device=st.sampled_from(["cpu", "cuda"]))
def test_runtime_context_when_logger_configured_then_sets_formatter(
    device: str,
) -> None:
    """Runtime context when logger configured then sets formatter."""
    with TemporaryDirectory() as tmp_dir:
        runtime = RuntimeConfig(out_dir=Path(tmp_dir), device=device)

        class _Handler(logging.Handler):
            def __init__(self) -> None:
                super().__init__()
                self.formatter: Any = None

            def setFormatter(self, formatter: Any) -> None:  # noqa: N802 - logging API
                self.formatter = formatter

            def emit(self, record: Any) -> None:
                return None

        handler = _Handler()

        def _handler_factory() -> Any:
            return handler

        ctx = runtime_context(
            runtime,
            logger_name=f"ml_playground.runtime.log.{device}",
            stream_handler_factory=_handler_factory,
            autocast_factory=lambda *_: nullcontext(),
        )

        assert ctx.logger.handlers
        assert handler.formatter is not None
