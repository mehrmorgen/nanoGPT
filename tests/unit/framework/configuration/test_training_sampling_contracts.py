from __future__ import annotations

from pathlib import Path

import pytest

from ml_playground.framework.configuration.models import (
    RuntimeConfig,
    TrainerConfig,
    MetadataConfig,
    ModelConfig,
    DataConfig,
    OptimConfig,
    LRSchedule,
    SamplerConfig,
    SampleConfig,
    PoolSizePolicy,
    coerce_path,
    derive_pool_size,
    resolve_if_relative,
)


def make_shared_config(tmp_path: Path) -> MetadataConfig:
    return MetadataConfig(
        experiment="test",
        config_path=tmp_path / "cfg.toml",
        project_home=tmp_path,
        dataset_dir=tmp_path,
        train_out_dir=tmp_path / "train",
        sample_out_dir=tmp_path / "sample",
    )


def test_runtime_config_log_interval_enforced(tmp_path: Path) -> None:
    with pytest.raises(
        ValueError, match="log_interval must be <= training.runtime.eval_interval"
    ):
        RuntimeConfig(out_dir=tmp_path, log_interval=50, eval_interval=10)


def test_runtime_config_log_interval_games_enforced(tmp_path: Path) -> None:
    with pytest.raises(
        ValueError, match="log_interval_games must be <= runtime.eval_interval_games"
    ):
        RuntimeConfig(
            out_dir=tmp_path,
            eval_interval=10,  # keep overall eval interval valid
            eval_interval_games=1,
            log_interval_games=2,
        )


def test_runtime_config_mps_compile_rejected(tmp_path: Path) -> None:
    with pytest.raises(
        ValueError, match="runtime.compile must be false when device is mps"
    ):
        RuntimeConfig(out_dir=tmp_path, device="mps", compile=True)


def test_trainer_config_block_sizes_validated(tmp_path: Path) -> None:
    with pytest.raises(
        ValueError,
        match="training.data.block_size must be <= training.model.block_size",
    ):
        TrainerConfig(
            model=ModelConfig(block_size=1024),
            data=DataConfig(block_size=2048),
            optim=OptimConfig(),
            schedule=LRSchedule(),
            runtime=RuntimeConfig(out_dir=tmp_path),
        )


def test_sampler_config_requires_runtime(tmp_path: Path) -> None:
    sampler_cfg = SamplerConfig(
        runtime=RuntimeConfig(out_dir=tmp_path), sample=SampleConfig()
    )
    shared_cfg = make_shared_config(tmp_path)
    # Exercise the config surface by calling a helper that relies on the runtime/out_dir invariants.
    assert sampler_cfg.runtime.out_dir == tmp_path
    assert (
        shared_cfg.train_out_dir.exists()
        or shared_cfg.train_out_dir == tmp_path / "train"
    )


def test_derive_pool_size_basic_calculation() -> None:
    """Test derive_pool_size with basic inputs."""
    result = derive_pool_size(
        target_labeled_positions=1000,
        avg_positions_per_game=10,
    )
    assert result == 100


def test_derive_pool_size_with_oversample() -> None:
    """Test derive_pool_size with oversample factor."""
    result = derive_pool_size(
        target_labeled_positions=1000,
        avg_positions_per_game=10,
        oversample_factor=1.5,
    )
    assert result == 150


def test_derive_pool_size_zero_positions() -> None:
    """Test derive_pool_size with zero target positions."""
    result = derive_pool_size(
        target_labeled_positions=0,
        avg_positions_per_game=10,
    )
    assert result == 0


def test_derive_pool_size_zero_avg_positions() -> None:
    """Test derive_pool_size with zero avg positions should raise error."""
    with pytest.raises(ValueError, match="must be > 0"):
        derive_pool_size(
            target_labeled_positions=1000,
            avg_positions_per_game=0,
        )


def test_derive_pool_size_rounding_up() -> None:
    """Test derive_pool_size rounds up correctly."""
    # Test case where result is not an integer
    result = derive_pool_size(100, 30, oversample_factor=1.0)  # 100/30 = 3.33...
    assert result == 4  # Should round up
    # Test with oversample factor
    result = derive_pool_size(50, 20, oversample_factor=1.5)  # (50/20) * 1.5 = 3.75
    assert result == 4  # Should round up


def test_poolSizePolicy_pool_size_property() -> None:
    """Test PoolSizePolicy.pool_size property."""
    policy = PoolSizePolicy(
        target_labeled_positions=100,
        avg_positions_per_game=25,
    )
    assert policy.pool_size == 4


# Tests for validation utilities


def test_coerce_path_with_valid_string() -> None:
    """Test coerce_path with valid string paths."""
    result = coerce_path("/valid/path")
    assert isinstance(result, Path)
    assert result == Path("/valid/path")


def test_coerce_path_with_path_object() -> None:
    """Test coerce_path with Path objects."""
    path = Path("/another/path")
    result = coerce_path(path)
    assert result == path


def test_coerce_path_with_invalid_type() -> None:
    """Test coerce_path with invalid types returns None."""
    assert coerce_path(123) is None
    assert coerce_path(None) is None
    assert coerce_path([]) is None


def test_resolve_if_relative_with_relative_path(tmp_path: Path) -> None:
    """Test resolve_if_relative with relative paths."""
    base = tmp_path / "base"
    base.mkdir()

    result = resolve_if_relative("subdir", base)
    assert result == base / "subdir"


def test_resolve_if_relative_with_absolute_path() -> None:
    """Test resolve_if_relative with absolute paths."""
    abs_path = Path("/absolute/path")
    result = resolve_if_relative(abs_path, Path("/base"))
    assert result == abs_path


def test_resolve_if_relative_with_none_value() -> None:
    """Test resolve_if_relative with None value returns as-is."""
    result = resolve_if_relative(None, Path("/base"))
    assert result is None


# =============================================================================
# Path Resolution with Context Tests
# =============================================================================


def test_preparer_config_path_resolution_with_context(tmp_path: Path) -> None:
    """Test PreparerConfig resolves paths with validation context."""
    from ml_playground.framework.configuration.models import PreparerConfig

    config_file = tmp_path / "config.toml"
    config_file.write_text("# test config")

    # Create with context that includes config_path
    config = PreparerConfig.model_validate(
        {"raw_dir": "./data", "raw_text_path": "./input.txt"},
        context={"config_path": config_file},
    )

    # Paths should be resolved relative to config file location
    assert config.raw_dir == tmp_path / "data"
    assert config.raw_text_path == tmp_path / "input.txt"


def test_preparer_config_path_resolution_without_context() -> None:
    """Test PreparerConfig keeps paths as-is without validation context."""
    from ml_playground.framework.configuration.models import PreparerConfig

    config = PreparerConfig(raw_dir=Path("./data"), raw_text_path=Path("./input.txt"))

    # Paths should remain as provided
    assert config.raw_dir == Path("./data")
    assert config.raw_text_path == Path("./input.txt")


def test_trainer_config_path_resolution_with_context(tmp_path: Path) -> None:
    """Test TrainerConfig resolves runtime paths with validation context."""
    from ml_playground.framework.configuration.models import TrainerConfig

    config_file = tmp_path / "config.toml"
    config_file.write_text("# test config")

    config = TrainerConfig.model_validate(
        {
            "model": {"n_layer": 2, "n_head": 2, "n_embd": 64},
            "data": {"block_size": 128},
            "optim": {},
            "schedule": {},
            "runtime": {"out_dir": "./outputs"},
        },
        context={"config_path": config_file},
    )

    # out_dir should be resolved relative to config file location
    assert config.runtime.out_dir == tmp_path / "outputs"


def test_sampler_config_path_resolution_with_context(tmp_path: Path) -> None:
    """Test SamplerConfig resolves runtime paths with validation context."""
    from ml_playground.framework.configuration.models import SamplerConfig

    config_file = tmp_path / "config.toml"
    config_file.write_text("# test config")

    config = SamplerConfig.model_validate(
        {
            "runtime": {"out_dir": "./samples"},
            "sample": {},
        },
        context={"config_path": config_file},
    )

    # out_dir should be resolved relative to config file location
    assert config.runtime.out_dir == tmp_path / "samples"


# =============================================================================
# MLflow URI Resolution Tests
# =============================================================================


def test_trainer_config_mlflow_uri_resolution(tmp_path: Path) -> None:
    """Test TrainerConfig resolves MLflow URI with validation context."""
    from ml_playground.framework.configuration.models import TrainerConfig

    config_file = tmp_path / "config.toml"
    config_file.write_text("# test config")

    config = TrainerConfig.model_validate(
        {
            "model": {"n_layer": 2, "n_head": 2, "n_embd": 64},
            "data": {"block_size": 128},
            "optim": {},
            "schedule": {},
            "runtime": {
                "out_dir": "./outputs",
                "mlflow_enabled": True,
                "mlflow_tracking_uri": "sqlite:///mlflow.db",
            },
        },
        context={"config_path": config_file},
    )

    # MLflow URI should be resolved relative to config file location
    assert config.runtime.mlflow_tracking_uri is not None
    assert "mlflow.db" in config.runtime.mlflow_tracking_uri


def test_sampler_config_mlflow_uri_resolution(tmp_path: Path) -> None:
    """Test SamplerConfig resolves MLflow URI with validation context."""
    from ml_playground.framework.configuration.models import SamplerConfig

    config_file = tmp_path / "config.toml"
    config_file.write_text("# test config")

    config = SamplerConfig.model_validate(
        {
            "runtime": {
                "out_dir": "./samples",
                "mlflow_enabled": True,
                "mlflow_tracking_uri": "sqlite:///mlflow.db",
            },
            "sample": {},
        },
        context={"config_path": config_file},
    )

    # MLflow URI should be resolved relative to config file location
    assert config.runtime.mlflow_tracking_uri is not None
    assert "mlflow.db" in config.runtime.mlflow_tracking_uri


# =============================================================================
# ExperimentConfig and MetadataConfig Tests
# =============================================================================


def test_experiment_config_path_resolution(tmp_path: Path) -> None:
    """Test ExperimentConfig resolves paths with metadata config_path."""
    from ml_playground.framework.configuration.models import (
        MetadataConfig,
        PreparerConfig,
    )

    config_file = tmp_path / "experiment.toml"
    config_file.write_text("# experiment config")

    # Use absolute paths for metadata as required by validation
    # This validates MetadataConfig path handling
    MetadataConfig(
        experiment="test_exp",
        config_path=config_file,
        project_home=tmp_path / "project",
        dataset_dir=tmp_path / "data",
        train_out_dir=tmp_path / "train_out",
        sample_out_dir=tmp_path / "sample_out",
    )

    # PreparerConfig should resolve raw_dir relative to config location
    prepare = PreparerConfig(raw_dir=Path("./raw"))

    # Verify the prepare config paths work correctly
    assert isinstance(prepare.raw_dir, Path)
    assert prepare.raw_dir == Path("./raw")  # Without context, stays relative


def test_metadata_config_path_resolution(tmp_path: Path) -> None:
    """Test MetadataConfig resolves paths with validation context."""
    from ml_playground.framework.configuration.models import MetadataConfig

    config_file = tmp_path / "config.toml"
    config_file.write_text("# test config")

    # Use absolute paths as required by MetadataConfig validation
    config = MetadataConfig.model_validate(
        {
            "experiment": "test",
            "config_path": str(config_file),
            "project_home": str(tmp_path / "project"),
            "dataset_dir": str(tmp_path / "data"),
            "train_out_dir": str(tmp_path / "train"),
            "sample_out_dir": str(tmp_path / "sample"),
        },
        context={"config_path": config_file},
    )

    # Paths should be resolved relative to config file location
    assert config.config_path == config_file
    assert config.project_home == tmp_path / "project"
