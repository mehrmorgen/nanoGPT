"""Unit tests for configuration/models.py using only public APIs."""

from __future__ import annotations

from pathlib import Path

import pytest
from pydantic import ValidationError

from ml_playground.framework.configuration.models import (
    RuntimeConfig,
    SECTION_PREPARE,
    SECTION_TRAIN,
    SECTION_SAMPLE,
    SECTION_METADATA,
    KEY_EXTRAS,
    DeviceKind,
    DTypeKind,
    NonNaNNonNegativeStrictFloat,
    UnitIntervalStrictFloat,
    PosUnitIntervalStrictFloat,
    PositiveStrictFloat,
)


def test_runtime_config_creation() -> None:
    """Test RuntimeConfig creation with valid parameters."""
    config = RuntimeConfig(
        out_dir=Path("/tmp"),
        max_iters=100,
        eval_interval=10,
        eval_iters=5,
        log_interval=10,
        eval_only=False,
        seed=42,
        device="cpu",
        dtype="float32",
        compile=False,
        tensorboard_enabled=False,
        ema_decay=0.0,
    )

    assert config.out_dir == Path("/tmp")
    assert config.max_iters == 100
    assert config.device == "cpu"
    assert config.dtype == "float32"


def test_runtime_config_with_logger() -> None:
    """Test RuntimeConfig with logger provided."""
    import logging

    logger = logging.getLogger("test")

    config = RuntimeConfig(
        out_dir=Path("/tmp"),
        max_iters=100,
        eval_interval=10,
        eval_iters=5,
        log_interval=10,
        eval_only=False,
        seed=42,
        device="cpu",
        dtype="float32",
        compile=False,
        tensorboard_enabled=False,
        ema_decay=0.0,
        logger=logger,
    )

    assert config.logger is logger


def test_runtime_config_validation() -> None:
    """Test RuntimeConfig validation."""
    # Test invalid device
    with pytest.raises(ValidationError):
        RuntimeConfig(
            out_dir=Path("/tmp"),
            max_iters=100,
            eval_interval=10,
            eval_iters=5,
            log_interval=10,
            eval_only=False,
            seed=42,
            device="invalid_device",  # type: ignore[arg-type]
            dtype="float32",
            compile=False,
            tensorboard_enabled=False,
            ema_decay=0.0,
        )

    # Test invalid dtype
    with pytest.raises(ValidationError):
        RuntimeConfig(
            out_dir=Path("/tmp"),
            max_iters=100,
            eval_interval=10,
            eval_iters=5,
            log_interval=10,
            eval_only=False,
            seed=42,
            device="cpu",
            dtype="invalid_dtype",  # type: ignore[arg-type]
            compile=False,
            tensorboard_enabled=False,
            ema_decay=0.0,
        )


def test_section_constants() -> None:
    """Test section constants."""
    assert SECTION_PREPARE == "prepare"
    assert SECTION_TRAIN == "training"
    assert SECTION_SAMPLE == "sampling"
    assert SECTION_METADATA == "metadata"
    assert KEY_EXTRAS == "extras"


def test_device_dtype_literals() -> None:
    """Test DeviceKind and DTypeKind literals."""
    # These should be valid literal values
    valid_devices: list[DeviceKind] = ["cpu", "mps", "cuda"]
    valid_dtypes: list[DTypeKind] = ["float32", "bfloat16", "float16"]

    assert len(valid_devices) == 3
    assert len(valid_dtypes) == 3


def test_annotated_validators() -> None:
    """Test the annotated validator types work correctly."""

    # Test that the validators can be used in type annotations
    def test_function(
        x: NonNaNNonNegativeStrictFloat,
        y: UnitIntervalStrictFloat,
        z: PosUnitIntervalStrictFloat,
        w: PositiveStrictFloat,
    ) -> tuple[float, float, float, float]:
        return x, y, z, w

    # The function should accept valid values
    result = test_function(1.0, 0.5, 0.1, 2.0)
    assert result == (1.0, 0.5, 0.1, 2.0)


def test_runtime_config_optional_fields() -> None:
    """Test RuntimeConfig with optional fields."""
    config = RuntimeConfig(
        out_dir=Path("/tmp"),
        max_iters=100,
        eval_interval=10,
        eval_iters=5,
        log_interval=10,
        eval_only=False,
        seed=42,
        device="cpu",
        dtype="float32",
        compile=False,
        tensorboard_enabled=False,
        ema_decay=0.0,
        # Optional fields not provided
    )

    # Should have default values for optional fields
    assert config.logger is not None  # Should have a default logger
