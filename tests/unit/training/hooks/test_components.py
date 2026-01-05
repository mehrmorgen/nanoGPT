from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, cast

import pytest
import torch
from torch.amp.grad_scaler import GradScaler

from ml_playground.configuration.models import TrainerConfig, ModelConfig
from ml_playground.training.ema import EMA
from ml_playground.models.core.model import GPT
from ml_playground.training.hooks.runtime import RuntimeContext
from ml_playground.training.hooks.components import initialize_components


def _make_config(
    compile: bool = False,
    ema_decay: float = 0.0,
) -> Any:
    class MockRuntime:
        def __init__(self):
            self.compile = compile
            self.ema_decay = ema_decay
            self.dtype = "float32"
            self.device = "cpu"

    class MockConfig:
        def __init__(self):
            self.runtime = MockRuntime()

    return MockConfig()


def test_initialize_components_basic(tmp_path: Path) -> None:
    model = cast(GPT, torch.nn.Module())
    cfg = _make_config()
    runtime = RuntimeContext(
        device_type="cpu",
        autocast_context=torch.amp.autocast("cpu", enabled=False),
        logger=logging.getLogger("test"),
    )

    result = initialize_components(
        model,
        cast(TrainerConfig, cfg),
        runtime,
        log_dir=str(tmp_path),
    )

    assert len(result) == 3
    compiled_model, scaler, ema = result

    assert compiled_model is model
    assert isinstance(scaler, GradScaler)
    assert ema is None


def test_initialize_components_with_ema(tmp_path: Path) -> None:
    model_cfg = ModelConfig(
        n_layer=1, n_head=1, n_embd=32, block_size=16, vocab_size=100
    )
    model = GPT(model_cfg, logger=logging.getLogger("test"))
    cfg = _make_config(ema_decay=0.99)
    runtime = RuntimeContext(
        device_type="cpu",
        autocast_context=torch.amp.autocast("cpu", enabled=False),
        logger=logging.getLogger("test"),
    )

    result = initialize_components(
        model,
        cast(TrainerConfig, cfg),
        runtime,
        log_dir=str(tmp_path),
    )
    _, _, ema = result

    assert isinstance(ema, EMA)
    assert ema.decay == 0.99


def test_initialize_components_with_compile(tmp_path: Path) -> None:
    model = cast(GPT, torch.nn.Module())
    cfg = _make_config(compile=True)
    runtime = RuntimeContext(
        device_type="cpu",
        autocast_context=torch.amp.autocast("cpu", enabled=False),
        logger=logging.getLogger("test"),
    )

    def mock_compile(m: GPT) -> GPT:
        return m

    result = initialize_components(
        model,
        cast(TrainerConfig, cfg),
        runtime,
        log_dir=str(tmp_path),
        compile_fn=mock_compile,
    )
    compiled_model, _, _ = result

    assert compiled_model is model


def test_initialize_components_compile_unavailable(tmp_path: Path) -> None:
    model = cast(GPT, torch.nn.Module())
    cfg = _make_config(compile=True)
    runtime = RuntimeContext(
        device_type="cpu",
        autocast_context=torch.amp.autocast("cpu", enabled=False),
        logger=logging.getLogger("test"),
    )

    class MockTorch:
        pass  # No compile attr

    with pytest.raises(RuntimeError, match="torch.compile requested but unavailable"):
        initialize_components(
            model,
            cast(TrainerConfig, cfg),
            runtime,
            log_dir=str(tmp_path),
            torch_module=MockTorch(),
        )
