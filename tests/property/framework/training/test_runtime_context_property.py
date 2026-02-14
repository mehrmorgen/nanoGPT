from __future__ import annotations

import logging
from contextlib import nullcontext
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any, ContextManager, cast

import torch
from hypothesis import HealthCheck, assume, given, settings, strategies as st

from ml_playground.framework.configuration.models import (
    DeviceKind,
    DTypeKind,
    RuntimeConfig,
)
from ml_playground.framework.core.runtime_context import runtime_context


@settings(
    max_examples=20,
    deadline=None,
    derandomize=True,
    suppress_health_check=[HealthCheck.function_scoped_fixture],
)
@given(  # type: ignore[reportAny]
    device=st.sampled_from(["cpu", "cuda", "mps"]),
    dtype=st.sampled_from(["float32", "bfloat16", "float16"]),
    seed=st.integers(min_value=0, max_value=10_000),
)
def test_runtime_context_when_device_then_sets_device_type(
    device: DeviceKind,
    dtype: DTypeKind,
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
            logger_name=f"ml_playground.framework.runtime.{device}.{dtype}.{seed}",
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
    deadline=None,
    derandomize=True,
    suppress_health_check=[HealthCheck.function_scoped_fixture],
)
@given(  # type: ignore[reportAny]
    seed=st.integers(min_value=0, max_value=10_000)
)
def test_runtime_context_when_cuda_available_then_seeds_cuda(seed: int) -> None:
    """Runtime context when cuda available then seeds cuda."""
    with TemporaryDirectory() as tmp_dir:
        runtime = RuntimeConfig(out_dir=Path(tmp_dir), device="cuda", seed=seed)

        called: list[int] = []

        def _cuda_available() -> bool:
            return True

        def _cuda_seed(value: int) -> None:
            called.append(value)

        class MockBackendObj:
            def __init__(self) -> None:
                self._precision = "highest"
                self._allow_tf32 = False

            @property
            def fp32_precision(self) -> str:
                return self._precision

            @fp32_precision.setter
            def fp32_precision(self, value: str) -> None:
                self._precision = value

            @property
            def allow_tf32(self) -> bool:
                return self._allow_tf32

            @allow_tf32.setter
            def allow_tf32(self, value: bool) -> None:
                self._allow_tf32 = value

        class MockCudaBackends:
            def __init__(self) -> None:
                self.matmul = MockBackendObj()

        class MockCudnnBackends:
            def __init__(self) -> None:
                self._precision = "highest"
                self._allow_tf32 = False

            @property
            def fp32_precision(self) -> str:
                return self._precision

            @fp32_precision.setter
            def fp32_precision(self, value: str) -> None:
                self._precision = value

            @property
            def allow_tf32(self) -> bool:
                return self._allow_tf32

            @allow_tf32.setter
            def allow_tf32(self, value: bool) -> None:
                self._allow_tf32 = value

        class MockBackends:
            def __init__(self) -> None:
                self.cuda = MockCudaBackends()
                self.cudnn = MockCudnnBackends()

        class MockCuda:
            def __init__(self) -> None:
                pass

            def is_available(self) -> bool:
                return False

            def manual_seed(self, seed: int) -> None:
                pass

        class MockTorchModule:
            def __init__(self) -> None:
                self.backends = MockBackends()
                self.cuda = MockCuda()

            def manual_seed(self, seed: int) -> None:
                pass

            def autocast(
                self, *, device_type: str, dtype: torch.dtype
            ) -> ContextManager[Any]:
                return nullcontext()

        fake_torch = MockTorchModule()

        runtime_context(
            runtime,
            logger_name=f"ml_playground.framework.runtime.seed.{seed}",
            cuda_available_fn=_cuda_available,
            cuda_manual_seed_fn=_cuda_seed,
            autocast_factory=lambda *_: nullcontext(),
            torch_module=fake_torch,
        )

        assert called == [seed]


@settings(
    max_examples=10,
    deadline=None,
    derandomize=True,
    suppress_health_check=[HealthCheck.function_scoped_fixture],
)
@given(  # type: ignore[reportAny]
    device=st.sampled_from(["cpu", "cuda"])
)
def test_runtime_context_when_logger_configured_then_sets_formatter(
    device: DeviceKind,
) -> None:
    """Runtime context when logger configured then sets formatter."""
    with TemporaryDirectory() as tmp_dir:
        runtime = RuntimeConfig(out_dir=Path(tmp_dir), device=device)

        class _Handler(logging.Handler):
            def __init__(self) -> None:
                super().__init__()
                self.formatter: Any = None

            def setFormatter(self, fmt: Any) -> None:  # noqa: N802 - logging API
                self.formatter = fmt

            def emit(self, record: Any) -> None:
                return None

        handler = _Handler()

        def _handler_factory() -> Any:
            return handler

        # Clear stale handlers so setup_logging installs the fresh one
        logger_name = f"ml_playground.framework.runtime.log.{device}"
        logging.getLogger(logger_name).handlers.clear()

        ctx = runtime_context(
            runtime,
            logger_name=logger_name,
            stream_handler_factory=_handler_factory,
            autocast_factory=lambda *_: nullcontext(),
        )

        assert ctx.logger is not None
        # We cast to any to access handlers which is not in LoggerLike protocol
        assert cast(Any, ctx.logger).handlers
        assert handler.formatter is not None
