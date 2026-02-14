from __future__ import annotations

from pathlib import Path

from ml_playground.framework.configuration.models import MetadataConfig
from pydantic import ValidationError
import pytest


def test_metadata_paths_resolve_relative_string_values(tmp_path: Path) -> None:
    """Test metadata paths resolve relative string values."""
    cfg_path = tmp_path / "exp" / "cfg.toml"
    cfg_path.parent.mkdir(parents=True, exist_ok=True)
    cfg_path.write_text("")

    data = {
        "experiment": "unit",
        "config_path": cfg_path,
        "project_home": Path(".."),
        "dataset_dir": Path("../data"),
        "train_out_dir": Path("../out/train"),
        "sample_out_dir": Path("../out/sample"),
    }

    metadata = MetadataConfig.model_validate(data)

    assert metadata.project_home.is_absolute()
    assert metadata.dataset_dir.is_absolute()
    assert metadata.train_out_dir.is_absolute()
    assert metadata.sample_out_dir.is_absolute()

    # Resolved relative to cfg directory (cfg_dir = tmp_path/exp)
    assert metadata.project_home == cfg_path.parent.parent.resolve()
    assert metadata.dataset_dir == (cfg_path.parent.parent / "data").resolve()
    assert (
        metadata.train_out_dir == (cfg_path.parent.parent / "out" / "train").resolve()
    )
    assert (
        metadata.sample_out_dir == (cfg_path.parent.parent / "out" / "sample").resolve()
    )


def test_metadata_paths_preserve_absolute_values(tmp_path: Path) -> None:
    """Test metadata paths preserve absolute values."""
    cfg_path = tmp_path / "exp" / "cfg.toml"
    cfg_path.parent.mkdir(parents=True, exist_ok=True)
    cfg_path.write_text("")

    abs_home = tmp_path / "home"
    abs_ds = tmp_path / "ds"
    abs_train = tmp_path / "runs" / "train"
    abs_sample = tmp_path / "runs" / "sample"

    data = {
        "experiment": "unit",
        "config_path": cfg_path,
        "project_home": abs_home,
        "dataset_dir": abs_ds,
        "train_out_dir": abs_train,
        "sample_out_dir": abs_sample,
    }

    metadata = MetadataConfig.model_validate(data)

    assert metadata.project_home == abs_home
    assert metadata.dataset_dir == abs_ds
    assert metadata.train_out_dir == abs_train
    assert metadata.sample_out_dir == abs_sample


def test_metadata_paths_missing_config_path_raises() -> None:
    """Test metadata paths missing config path raises."""
    # MetadataConfig is strict: config_path is required
    data = {
        "experiment": "unit",
        # no config_path provided
        "project_home": Path(".."),
        "dataset_dir": Path("data"),
        "train_out_dir": Path("out/train"),
        "sample_out_dir": Path("out/sample"),
    }

    with pytest.raises(ValidationError):
        MetadataConfig.model_validate(data)
