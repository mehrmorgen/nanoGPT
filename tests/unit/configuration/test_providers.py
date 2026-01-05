from __future__ import annotations
from pathlib import Path
from typing import Any
from ml_playground.configuration.models import (
    DataConfig,
    LRSchedule,
    ModelConfig,
    OptimConfig,
    PreparerConfig,
    RuntimeConfig,
    SamplerConfig,
    SampleConfig,
    TrainerConfig,
)
from ml_playground.configuration.providers import get_default_providers


def test_get_default_providers():
    providers = get_default_providers()
    assert "pool_size_provider" in providers
    assert "read_text_fn" in providers
    assert "tokenizer_factory" in providers
    assert "telemetry" in providers

    # Test read_text_fn
    # We can't easily test real file read without filesystem,
    # but we can verify it's a callable.
    assert callable(providers["read_text_fn"])


def test_config_provider_injection_already_set():
    # Test that _inject_providers does not overwrite already set fields
    def mock_read(p: Path) -> str:
        return "mock"

    def other_read(p: Path) -> str:
        return "other"

    ctx = {"providers": {"read_text_fn": mock_read, "telemetry": None}}

    # PreparerConfig - needs raw_text_path as Path
    cfg = PreparerConfig.model_validate(
        {"raw_text_path": Path("path"), "read_text_fn": other_read}, context=ctx
    )
    assert cfg.read_text_fn == other_read


def test_trainer_config_provider_injection_already_set():
    def mock_save(m: Any, c: Any, **kwargs: Any) -> None:
        pass

    def other_save(m: Any, c: Any, **kwargs: Any) -> None:
        pass

    # TrainerConfig requires many fields.
    # Data block_size must be <= model block_size.
    minimal_data = {
        "model": ModelConfig(vocab_size=10, block_size=1024),
        "data": DataConfig(block_size=1024),
        "optim": OptimConfig(),
        "schedule": LRSchedule(),
        "runtime": RuntimeConfig(out_dir=Path("/tmp")),
        "checkpoint_save_fn": other_save,
    }

    ctx = {"providers": {"checkpoint_save_fn": mock_save}}
    cfg = TrainerConfig.model_validate(minimal_data, context=ctx)
    assert cfg.checkpoint_save_fn == other_save


def test_sampler_config_provider_injection_already_set():
    def mock_load(manager: Any, cfg: Any, logger: Any) -> Any:
        pass

    def other_load(manager: Any, cfg: Any, logger: Any) -> Any:
        pass

    minimal_data = {
        "runtime": RuntimeConfig(out_dir=Path("/tmp")),
        "sample": SampleConfig(start="FILE:prompt.txt"),
        "checkpoint_load_fn": other_load,
    }

    ctx = {"providers": {"checkpoint_load_fn": mock_load}}
    cfg = SamplerConfig.model_validate(minimal_data, context=ctx)
    assert cfg.checkpoint_load_fn == other_load
