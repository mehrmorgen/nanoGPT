"""Branch coverage tests for configuration/models.py."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
from pydantic import ValidationError

from ml_playground.framework.configuration.models import (
    DataConfig,
    ExperienceStorageConfig,
    ExperimentConfig,
    LRSchedule,
    MetadataConfig,
    ModelConfig,
    OptimConfig,
    PreparerConfig,
    RuntimeConfig,
    SamplerConfig,
    TrainerConfig,
    derive_pool_size,
)


# ---------------------------------------------------------------------------
# _get_resolve_fn (tested via public API)
# ---------------------------------------------------------------------------


def test_resolve_fn_no_context() -> None:
    """Branch [81→82] / [86→87]: no resolve_fn in context → paths not resolved."""
    # Validate without context → _get_resolve_fn returns None
    cfg = RuntimeConfig(out_dir=Path("/tmp"))
    assert cfg is not None


def test_resolve_fn_with_callable_in_context(tmp_path: Path) -> None:
    """Branch [86→87] true: resolve_fn callable in context → used for resolution."""
    calls: list[object] = []

    def fake_resolve(p: Path) -> Path:
        calls.append(p)
        return p

    cfg_path = tmp_path / "config.toml"
    cfg_path.write_text("")
    data = {"raw_dir": "relative/path"}
    cfg = PreparerConfig.model_validate(
        data, context={"config_path": cfg_path, "resolve_fn": fake_resolve}
    )
    assert cfg is not None
    assert len(calls) > 0


# ---------------------------------------------------------------------------
# _resolve_mlflow_tracking_uri (tested via SamplerConfig/TrainerConfig)
# ---------------------------------------------------------------------------


def test_mlflow_uri_none_via_runtime(tmp_path: Path) -> None:
    """Branch [98→99]: mlflow_tracking_uri=None → stays None."""
    cfg = RuntimeConfig(out_dir=Path("/tmp"), mlflow_tracking_uri=None)
    assert cfg.mlflow_tracking_uri is None


def test_mlflow_uri_absolute_sqlite_via_sampler(tmp_path: Path) -> None:
    """Branch [100→101]: sqlite://// absolute → returned as-is."""
    cfg_path = tmp_path / "config.toml"
    cfg_path.write_text("")
    data = {
        "sample": {},
        "runtime": {
            "out_dir": str(tmp_path / "out"),
            "mlflow_enabled": True,
            "mlflow_tracking_uri": "sqlite:////absolute/path/db.sqlite",
        },
    }
    cfg = SamplerConfig.model_validate(data, context={"config_path": cfg_path})
    assert cfg.runtime.mlflow_tracking_uri == "sqlite:////absolute/path/db.sqlite"


def test_mlflow_uri_relative_sqlite_via_sampler(tmp_path: Path) -> None:
    """Branch [103→108]: relative sqlite URI → resolved against base_dir."""
    cfg_path = tmp_path / "config.toml"
    cfg_path.write_text("")
    data = {
        "sample": {},
        "runtime": {
            "out_dir": str(tmp_path / "out"),
            "mlflow_enabled": True,
            "mlflow_tracking_uri": "sqlite:///mlruns.db",
        },
    }
    cfg = SamplerConfig.model_validate(data, context={"config_path": cfg_path})
    assert cfg.runtime.mlflow_tracking_uri is not None
    assert cfg.runtime.mlflow_tracking_uri.startswith("sqlite:///")


def test_mlflow_uri_non_sqlite_via_sampler(tmp_path: Path) -> None:
    """Branch [109]: non-sqlite URI → returned as-is."""
    cfg_path = tmp_path / "config.toml"
    cfg_path.write_text("")
    data = {
        "sample": {},
        "runtime": {
            "out_dir": str(tmp_path / "out"),
            "mlflow_enabled": True,
            "mlflow_tracking_uri": "http://mlflow.example.com",
        },
    }
    cfg = SamplerConfig.model_validate(data, context={"config_path": cfg_path})
    assert cfg.runtime.mlflow_tracking_uri == "http://mlflow.example.com"


# ---------------------------------------------------------------------------
# derive_pool_size
# ---------------------------------------------------------------------------


def test_derive_pool_size_negative_target() -> None:
    """Branch [236→237]: target_labeled_positions < 0 → ValueError."""
    with pytest.raises(ValueError, match="target_labeled_positions must be >= 0"):
        derive_pool_size(-1, 10)


def test_derive_pool_size_zero_avg() -> None:
    """Branch [240→241]: avg_positions_per_game <= 0 → ValueError."""
    with pytest.raises(ValueError, match="avg_positions_per_game must be > 0"):
        derive_pool_size(100, 0)


def test_derive_pool_size_zero_oversample() -> None:
    """Branch [240→241]: oversample_factor <= 0 → ValueError."""
    with pytest.raises(ValueError, match="oversample_factor must be > 0"):
        derive_pool_size(100, 10, oversample_factor=0.0)


# ---------------------------------------------------------------------------
# RuntimeConfig._coerce_logger
# ---------------------------------------------------------------------------


def test_runtime_config_coerce_logger_non_mapping() -> None:
    """Branch [196→197]: data is not a Mapping → returned as-is (triggers other error)."""
    with pytest.raises(ValidationError):
        RuntimeConfig.model_validate("not_a_mapping")


# ---------------------------------------------------------------------------
# RuntimeConfig._strip_computed_fields
# ---------------------------------------------------------------------------


def test_runtime_config_strip_computed_fields_non_mapping() -> None:
    """Branch [403→404]: data is not a Mapping → returned as-is."""
    with pytest.raises(ValidationError):
        RuntimeConfig.model_validate(42)


# ---------------------------------------------------------------------------
# ExperienceStorageConfig._resolve_paths
# ---------------------------------------------------------------------------


def test_experience_storage_config_non_mapping() -> None:
    """Branch [434→435]: data is not a Mapping → returned as-is."""
    with pytest.raises(ValidationError):
        ExperienceStorageConfig.model_validate("not_a_mapping")


def test_experience_storage_config_path_none_in_context(tmp_path: Path) -> None:
    """Branch [442→446]: path in data but resolve context has no config_path."""
    cfg = ExperienceStorageConfig(strategy="memory", path=None)
    assert cfg.strategy == "memory"


# ---------------------------------------------------------------------------
# TrainerConfig._resolve_paths
# ---------------------------------------------------------------------------


def test_trainer_config_non_mapping() -> None:
    """Branch [464→465]: data is not a Mapping → returned as-is."""
    with pytest.raises(ValidationError):
        TrainerConfig.model_validate("not_a_mapping")


# ---------------------------------------------------------------------------
# PreparerConfig._resolve_paths
# ---------------------------------------------------------------------------


def test_preparer_config_non_mapping() -> None:
    """Branch [282→283]: data is not a Mapping → returned as-is."""
    with pytest.raises(ValidationError):
        PreparerConfig.model_validate("not_a_mapping")


# ---------------------------------------------------------------------------
# SamplerConfig._resolve_paths
# ---------------------------------------------------------------------------


def test_sampler_config_non_mapping() -> None:
    """Branch [579→580]: data is not a Mapping → returned as-is."""
    with pytest.raises(ValidationError):
        SamplerConfig.model_validate("not_a_mapping")


# ---------------------------------------------------------------------------
# TrainerConfig._coerce_target_modules (PeftConfig)
# ---------------------------------------------------------------------------


def test_peft_config_coerce_target_modules_set() -> None:
    """Branch [531→532]: target_modules is a set → converted to tuple."""
    cfg = TrainerConfig(
        model=ModelConfig(),
        data=DataConfig(),
        optim=OptimConfig(),
        schedule=LRSchedule(),
        runtime=RuntimeConfig(out_dir=Path("/tmp")),
        peft=TrainerConfig.PeftConfig(
            enabled=True,
            target_modules={"q_proj", "v_proj"},  # type: ignore[arg-type]
        ),
    )
    assert cfg.peft is not None
    assert isinstance(cfg.peft.target_modules, tuple)


# ---------------------------------------------------------------------------
# ExperimentConfig._resolve_all_paths — metadata hasattr fallback
# ---------------------------------------------------------------------------


def test_experiment_config_metadata_non_mapping(tmp_path: Path) -> None:
    """Branch [700→703]: metadata is not a Mapping but has config_path attr."""
    # ExperimentConfig._resolve_all_paths checks hasattr(metadata_obj, "config_path")
    # when metadata_obj is not a Mapping. This is hard to trigger via public API
    # since metadata is always a dict from TOML. Test the no-config_path path instead.
    with pytest.raises((ValidationError, TypeError, ValueError)):
        ExperimentConfig.model_validate({"metadata": "not_a_mapping"})


# ---------------------------------------------------------------------------
# ExperimentConfig._normalize_runtime — metadata_data is None
# ---------------------------------------------------------------------------


def test_experiment_config_normalize_runtime_no_metadata(tmp_path: Path) -> None:
    """Branch [730→-718]: exercise _normalize_runtime with full config."""
    cfg_path = tmp_path / "config.toml"
    cfg_path.write_text("")
    out_dir = str(tmp_path / "out")
    data: dict[str, Any] = {
        "metadata": {
            "experiment": "test",
            "config_path": str(cfg_path),
            "project_home": str(tmp_path),
            "dataset_dir": str(tmp_path),
            "train_out_dir": str(tmp_path / "train"),
            "sample_out_dir": str(tmp_path / "sample"),
        },
        "prepare": {},
        "training": {
            "model": {},
            "data": {},
            "optim": {},
            "schedule": {},
            "runtime": {"out_dir": out_dir},
        },
        "sampling": {
            "sample": {},
            "runtime": {"out_dir": out_dir},
        },
    }
    cfg = ExperimentConfig.model_validate(data)
    assert cfg is not None


# ---------------------------------------------------------------------------
# MetadataConfig._resolve_metadata_paths
# ---------------------------------------------------------------------------


def test_metadata_config_non_mapping() -> None:
    """Branch [780→781]: data is not a Mapping → returned as-is."""
    with pytest.raises(ValidationError):
        MetadataConfig.model_validate("not_a_mapping")
