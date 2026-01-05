from __future__ import annotations
import pytest
from pathlib import Path
from typing import Any
from ml_playground.configuration.models import (
    PreparerConfig,
    TrainerConfig,
    SamplerConfig,
    SharedConfig,
    PoolSizePolicy,
    RuntimeConfig,
    _resolve_path_strict,
    _FrozenStrictModel,
)


def test_resolve_path_strict_os_error(mocker: Any):
    # Mock Path.exists to raise OSError
    mocker.patch.object(Path, "exists", side_effect=OSError("Permission denied"))
    with pytest.raises(ValueError, match="Invalid path"):
        _resolve_path_strict(Path("/tmp/restricted"))


def test_resolve_path_strict_not_exists(tmp_path: Path):
    non_existent = tmp_path / "nope"
    with pytest.raises(ValueError, match="Invalid path"):
        _resolve_path_strict(non_existent)


def test_frozen_strict_model_setattr_logger():
    class TestModel(_FrozenStrictModel):
        pass

    m = TestModel()
    new_logger = "not really a logger"
    # Should allow setting logger because it's frozen=False in Field
    m.logger = new_logger
    assert m.logger == new_logger


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
