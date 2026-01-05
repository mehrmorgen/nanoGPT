"""Additional coverage tests for optimization module."""

from __future__ import annotations

from typing import Any, Sequence

import torch

from ml_playground.models.core import optimization


class _CoverageModel(torch.nn.Module):
    """Model with mixed parameter types for comprehensive testing."""

    def __init__(self) -> None:
        super().__init__()
        # Parameters that should be decayed (dim >= 2)
        self.linear1 = torch.nn.Linear(4, 8, bias=True)
        self.linear2 = torch.nn.Linear(8, 4, bias=True)
        # Parameters that should NOT be decayed (dim < 2)
        self.bias = torch.nn.Parameter(torch.zeros(4))
        self.scalar_param = torch.nn.Parameter(torch.tensor(1.0))


class _ListLogger:
    def __init__(self) -> None:
        self.messages: list[str] = []

    def info(self, message: str, *args: Any) -> None:
        self.messages.append(message % args if args else message)


def test_configure_optimizers_cuda_device_enables_fused() -> None:
    """Test that CUDA device enables fused parameter."""
    model = _CoverageModel()

    optimizer = optimization.configure_optimizers(
        model,
        weight_decay=0.1,
        learning_rate=0.01,
        betas=(0.9, 0.95),
        device_type="cuda",
    )

    # Should have fused=True for CUDA
    assert optimizer.defaults.get("fused") is True


def test_configure_optimizers_cpu_device_no_fused() -> None:
    """Test that CPU device sets fused=None."""
    model = _CoverageModel()

    optimizer = optimization.configure_optimizers(
        model,
        weight_decay=0.1,
        learning_rate=0.01,
        betas=(0.9, 0.95),
        device_type="cpu",
    )

    # Should have fused=None for CPU
    assert optimizer.defaults.get("fused") is None


def test_configure_optimizers_uses_correct_learning_rate() -> None:
    """configure_optimizers should set the correct learning rate."""
    model = _CoverageModel()
    configured = optimization.configure_optimizers(
        model,
        weight_decay=0.1,
        learning_rate=0.02,
        betas=(0.9, 0.95),
    )
    assert configured.param_groups[0]["lr"] == 0.02


def test_configure_optimizers_mps_device_no_fused() -> None:
    """Test that MPS device sets fused=None."""
    model = _CoverageModel()

    optimizer = optimization.configure_optimizers(
        model,
        weight_decay=0.1,
        learning_rate=0.01,
        betas=(0.9, 0.95),
        device_type="mps",
    )

    # Should have fused=None for MPS
    assert optimizer.defaults.get("fused") is None


def test_configure_optimizers_zero_weight_decay() -> None:
    """Test configuration with zero weight decay."""
    model = _CoverageModel()

    optimizer = optimization.configure_optimizers(
        model,
        weight_decay=0.0,  # No weight decay
        learning_rate=0.01,
        betas=(0.9, 0.95),
        device_type="cpu",
    )

    # Should have zero weight decay for all parameters
    for group in optimizer.param_groups:
        assert group["weight_decay"] == 0.0


def test_configure_optimizers_model_with_no_grad_parameters() -> None:
    """Test model where all parameters have requires_grad=False."""
    model = torch.nn.Linear(4, 4)
    for param in model.parameters():
        param.requires_grad = False

    optimizer = optimization.configure_optimizers(
        model,
        weight_decay=0.1,
        learning_rate=0.01,
        betas=(0.9, 0.95),
        device_type="cpu",
    )

    # Should create optimizer even with no trainable parameters
    assert isinstance(optimizer, torch.optim.AdamW)


def test_configure_optimizers_model_with_only_1d_parameters() -> None:
    """Test model with only 1D parameters (no weight decay)."""
    model = torch.nn.Module()
    model.param1 = torch.nn.Parameter(torch.randn(4))
    model.param2 = torch.nn.Parameter(torch.randn(8))

    optimizer = optimization.configure_optimizers(
        model,
        weight_decay=0.1,
        learning_rate=0.01,
        betas=(0.9, 0.95),
        device_type="cpu",
    )

    # All parameters should have zero weight decay (1D parameters have dim < 2)
    # Should have 2 groups: one empty (no 2D params), one with weight_decay=0.0
    assert len(optimizer.param_groups) == 2
    # First group should be empty (no 2D parameters)
    assert len(optimizer.param_groups[0]["params"]) == 0
    # Second group should have weight_decay=0.0 (1D parameters)
    assert optimizer.param_groups[1]["weight_decay"] == 0.0
    assert len(optimizer.param_groups[1]["params"]) == 2  # Both 1D parameters


def test_configure_optimizers_model_with_only_2d_parameters() -> None:
    """Test model with only 2D parameters (weight decay applied)."""
    model = torch.nn.Module()
    model.linear1 = torch.nn.Linear(4, 8)
    model.linear2 = torch.nn.Linear(8, 4)

    optimizer = optimization.configure_optimizers(
        model,
        weight_decay=0.1,
        learning_rate=0.01,
        betas=(0.9, 0.95),
        device_type="cpu",
    )

    # Linear layers have both weights (2D) and bias (1D)
    # Should have 2 groups: one with weight_decay=0.1, one with weight_decay=0.0
    assert len(optimizer.param_groups) == 2
    # First group should have weight decay (2D weights)
    assert optimizer.param_groups[0]["weight_decay"] == 0.1
    # Second group should have zero weight decay (1D biases)
    assert optimizer.param_groups[1]["weight_decay"] == 0.0


def test_configure_optimizers_custom_factory_with_fused_override() -> None:
    """Test custom factory that handles fused parameter differently."""
    model = _CoverageModel()
    captured_calls = []

    def custom_factory(
        params: optimization.ParamGroups,
        *,
        lr: float,
        betas: Sequence[float],
        fused: bool | None = None,
    ) -> torch.optim.Optimizer:
        captured_calls.append({"fused": fused, "lr": lr})
        # Custom logic: always use fused=False regardless of device
        return torch.optim.AdamW(params, lr=lr, betas=tuple(betas), fused=False)

    optimization.configure_optimizers(
        model,
        weight_decay=0.1,
        learning_rate=0.01,
        betas=(0.9, 0.95),
        device_type="cuda",
        factory=custom_factory,
    )

    # Custom factory should have been called with fused=True (from CUDA logic)
    assert len(captured_calls) == 1
    assert captured_calls[0]["fused"] is True


def test_configure_optimizers_without_logger() -> None:
    """Test that function works without logger (no logging)."""
    model = _CoverageModel()

    optimizer = optimization.configure_optimizers(
        model,
        weight_decay=0.1,
        learning_rate=0.01,
        betas=(0.9, 0.95),
        device_type="cpu",
        logger=None,  # No logger
    )

    # Should work without logger
    assert isinstance(optimizer, torch.optim.AdamW)


def test_configure_optimizers_parameter_separation() -> None:
    """Test that 1D and 2D+ parameters are properly separated."""
    model = _CoverageModel()

    optimizer = optimization.configure_optimizers(
        model,
        weight_decay=0.1,
        learning_rate=0.01,
        betas=(0.9, 0.95),
        device_type="cpu",
    )

    # Should have exactly 2 parameter groups (decayed and non-decayed)
    assert len(optimizer.param_groups) == 2

    # First group should have weight decay (2D+ parameters)
    assert optimizer.param_groups[0]["weight_decay"] == 0.1

    # Second group should have zero weight decay (1D parameters)
    assert optimizer.param_groups[1]["weight_decay"] == 0.0


def test_adamw_factory_protocol_compliance() -> None:
    """Test that the default factory complies with the _AdamWFactory protocol."""
    model = _CoverageModel()

    # This should not raise any type errors
    optimizer = optimization.configure_optimizers(
        model,
        weight_decay=0.1,
        learning_rate=0.01,
        betas=(0.9, 0.95),
        device_type="cpu",
    )

    # Verify it's a proper AdamW optimizer
    assert isinstance(optimizer, torch.optim.AdamW)
    assert optimizer.defaults["lr"] == 0.01
    assert optimizer.defaults["betas"] == (0.9, 0.95)
    # weight_decay is not in defaults - it's set per parameter group
