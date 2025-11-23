"""Test device module."""

from __future__ import annotations

from dataclasses import dataclass
from types import SimpleNamespace
from typing import Any, Callable

from hypothesis import given, settings
from hypothesis import strategies as st

# Import the module directly to ensure it's loaded
import ml_playground.runtime.device as rt_device


@st.composite
def device_types(draw: st.DrawFn) -> str:
    """Generate valid device types."""
    return draw(st.sampled_from(["cpu", "cuda", "mps"]))


@st.composite
def dtypes(draw: st.DrawFn) -> str:
    """Generate valid dtype strings."""
    return draw(st.sampled_from(["float32", "float16", "bfloat16"]))


@given(
    device_type=device_types(),
    dtype=dtypes(),
    seed=st.integers(min_value=0, max_value=2**31 - 1),
)
@settings(max_examples=10, deadline=None, derandomize=True)
def test_global_device_setup(device_type: str, dtype: str, seed: int) -> None:
    """Test global_device_setup with various parameters."""
    # Patch torch module to avoid actual device setup
    import ml_playground.runtime.device as device_module

    @dataclass
    class _FakeCuda:
        is_available_fn: Callable[[], bool]

        def is_available(self) -> bool:  # pragma: no cover - trivial
            return self.is_available_fn()

        def manual_seed_all(self, _seed: int) -> None:  # pragma: no cover - trivial
            return None

    @dataclass
    class _FakeMps:
        is_available_fn: Callable[[], bool]

        def is_available(self) -> bool:  # pragma: no cover - trivial
            return self.is_available_fn()

    @dataclass
    class _FakeBackends:
        mps: _FakeMps
        cuda: Any
        cudnn: Any

    class _FakeGenerator:
        def manual_seed(self, _seed: int) -> None:  # pragma: no cover - trivial
            return None

    class _FakeTorch:
        def __init__(self, cuda_available: bool, mps_available: bool) -> None:
            self.cuda = _FakeCuda(is_available_fn=lambda: cuda_available)
            self.backends = _FakeBackends(
                mps=_FakeMps(is_available_fn=lambda: mps_available),
                cuda=SimpleNamespace(matmul=SimpleNamespace(allow_tf32=True)),
                cudnn=SimpleNamespace(allow_tf32=True),
            )
            self._last_device: Any | None = None

        def device(self, name: str) -> SimpleNamespace:
            self._last_device = name
            return SimpleNamespace(type=name, index=None)

        def manual_seed(self, _seed: int) -> None:  # pragma: no cover - trivial
            return None

        Generator = _FakeGenerator

    original_torch = getattr(device_module, "torch", None)
    device_module.torch = _FakeTorch(
        cuda_available=device_type == "cuda",
        mps_available=device_type == "mps",
    )

    try:
        # Should not raise an exception
        rt_device.global_device_setup(device_type, dtype, seed)
    finally:
        if original_torch is not None:
            device_module.torch = original_torch


def test_device_function_exists() -> None:
    """Test that global_device_setup function is available."""
    assert hasattr(rt_device, "global_device_setup")
    assert callable(rt_device.global_device_setup)
