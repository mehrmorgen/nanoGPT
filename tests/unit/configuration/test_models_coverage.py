from __future__ import annotations
import pytest
from pathlib import Path
from ml_playground.configuration.models import (
    PreparerConfig,
    TrainerConfig,
    SamplerConfig,
    SharedConfig,
    PoolSizePolicy,
    RuntimeConfig,
    _resolve_path_strict,
)


def test_resolve_path_strict_restricted(tmp_path: Path):
    # Instead of trying to simulate a restricted directory (which fails on some systems/users),
    # we test the explicit logic in _resolve_path_strict: it checks .exists() and raises ValueError if False.
    # Resolution of a non-existent path itself usually doesn't raise unless it's an illegal path.
    non_existent = tmp_path / "does_not_exist_at_all_12345"
    with pytest.raises(ValueError, match="Invalid path"):
        _resolve_path_strict(non_existent)


def test_runtime_config_total_eval_steps_zero():
    # max_iters=0, eval_interval=2000 => 0 steps
    cfg = RuntimeConfig(out_dir=Path("/tmp"), max_iters=0, eval_interval=2000)
    assert cfg.total_eval_steps == 0


def test_runtime_config_total_eval_games():
    # Missing optional fields return 0
    cfg = RuntimeConfig(out_dir=Path("/tmp"), max_games=None, eval_interval_games=None)
    assert cfg.total_eval_games == 0

    # max_games=100, eval_interval_games=10 => 10 games
    cfg = RuntimeConfig(out_dir=Path("/tmp"), max_games=100, eval_interval_games=10)
    assert cfg.total_eval_games == 10


def test_pool_size_policy_pool_size_none():
    # Manual instantiation without provider should raise ValueError on access
    # We pass an empty providers dict in the context to avoid falling back to defaults
    ctx = {"providers": {}}
    policy = PoolSizePolicy.model_validate(
        {"target_labeled_positions": 100, "avg_positions_per_game": 10}, context=ctx
    )
    with pytest.raises(ValueError, match="pool_size_provider must be supplied"):
        _ = policy.pool_size


def test_trainer_config_peft_target_modules_tuple():
    from ml_playground.configuration.models import TrainerConfig

    # Test branch where target_modules is already a tuple
    data = {"enabled": True, "target_modules": ("mod1", "mod2")}
    peft = TrainerConfig.PeftConfig.model_validate(data)
    assert peft.target_modules == ("mod1", "mod2")


def test_inject_providers_non_dict():
    # Test branches where data is not a dict
    assert PreparerConfig._inject_providers("not a dict", None) == "not a dict"  # type: ignore
    assert TrainerConfig._inject_providers(123, None) == 123  # type: ignore
    assert SamplerConfig._inject_providers(None, None) is None  # type: ignore
    assert PoolSizePolicy._inject_provider([], None) == []  # type: ignore
    # _resolve_shared_paths only takes 'data' at runtime, info is injected by pydantic
    assert SharedConfig._resolve_shared_paths("not a dict") == "not a dict"  # type: ignore


def test_providers_from_context_none():
    # Regression test for @[Bug: Context validation bug]
    # Should return default providers when info is None or context is None
    from ml_playground.configuration.models import _providers_from_context

    assert _providers_from_context(None) is not None
