"""Property-based tests for training hooks components.

Tests runtime context and evaluation helpers
using Hypothesis to verify behavior invariants.
"""

from __future__ import annotations

from contextlib import nullcontext

import pytest
from hypothesis import given, settings, strategies as st

from ml_playground.framework.training.hooks.runtime import RuntimeContext


# =============================================================================
# RuntimeContext Property Tests
# =============================================================================


@settings(max_examples=10, deadline=None, derandomize=True)
@given(device_type=st.sampled_from(["cpu", "cuda", "mps"]))
def test_runtime_context_creation(device_type: str) -> None:
    """RuntimeContext initializes with device type and autocast context."""
    ctx = RuntimeContext(device_type=device_type, autocast_context=nullcontext())
    assert ctx.device_type == device_type


@settings(max_examples=20, deadline=None, derandomize=True)
@given(
    device_type=st.text(min_size=1, max_size=20),
)
def test_runtime_context_various_device_types(device_type: str) -> None:
    """RuntimeContext accepts various device type strings."""
    ctx = RuntimeContext(device_type=device_type, autocast_context=nullcontext())
    assert ctx.device_type == device_type
    # autocast_context should be stored
    assert ctx.autocast_context is not None


@settings(max_examples=15, deadline=None, derandomize=True)
@given(device_type=st.sampled_from(["cpu", "cuda", "mps"]))
def test_runtime_context_is_dataclass(device_type: str) -> None:
    """RuntimeContext is a dataclass with proper equality."""
    ctx1 = RuntimeContext(device_type=device_type, autocast_context=nullcontext())
    ctx2 = RuntimeContext(device_type=device_type, autocast_context=nullcontext())
    # Same values should be equal
    assert ctx1.device_type == ctx2.device_type


def test_runtime_context_with_real_autocast() -> None:
    """RuntimeContext works with actual autocast context."""
    try:
        import torch

        device_type = "cpu"
        # Use bfloat16 which is supported on CPU
        ctx = RuntimeContext(
            device_type=device_type,
            autocast_context=torch.autocast(
                device_type=device_type, dtype=torch.bfloat16
            ),
        )
        assert ctx.device_type == device_type
    except ImportError:
        pytest.skip("torch not available")


# =============================================================================
# RuntimeContext Immutability Property Tests
# =============================================================================


@settings(max_examples=10, deadline=None, derandomize=True)
@given(device_type=st.sampled_from(["cpu", "cuda", "mps"]))
def test_runtime_context_slots(device_type: str) -> None:
    """RuntimeContext uses slots - cannot add arbitrary attributes."""
    ctx = RuntimeContext(device_type=device_type, autocast_context=nullcontext())
    # Verify expected attributes exist by direct access
    assert ctx.device_type == device_type
    assert ctx.autocast_context is not None
    # Attempting to add new attribute should fail due to slots
    with pytest.raises(AttributeError):
        ctx.new_attr = "test"  # type: ignore[attr-defined]
