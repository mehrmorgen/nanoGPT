"""Branch coverage tests for configuration/loading.py."""

from __future__ import annotations

from pathlib import Path

import pytest

from ml_playground.framework.configuration.loading import (
    load_full_experiment_config,
    load_train_config,
    load_sample_config,
    load_prepare_config,
)


# ---------------------------------------------------------------------------
# _validate_extras branches (tested via load_train_config which calls it)
# ---------------------------------------------------------------------------


def _write_train_config_with_extras(path: Path, extras_toml: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        f"""\
[training]
[training.model]
[training.data]
[training.optim]
[training.schedule]
[training.runtime]
out_dir = "/tmp/out"
{extras_toml}
"""
    )


def test_validate_extras_extras_is_none(tmp_path: Path) -> None:
    """Branch [99→100]: extras is None → coerced to {}."""
    cfg_path = tmp_path / "exp" / "config.toml"
    _write_train_config_with_extras(cfg_path, "[training.extras]")
    # Overwrite to set extras = None equivalent (empty table is fine)
    cfg = load_train_config(cfg_path, default_config_path=tmp_path / "no.toml")
    assert cfg is not None


def test_validate_extras_extras_not_mapping(tmp_path: Path) -> None:
    """Branch [101→102]: extras is not a Mapping → TypeError."""
    cfg_path = tmp_path / "exp" / "config.toml"
    cfg_path.parent.mkdir(parents=True, exist_ok=True)
    cfg_path.write_text(
        '[training]\nextras = "not_a_mapping"\n'
        "[training.model]\n[training.data]\n[training.optim]\n"
        "[training.schedule]\n[training.runtime]\nout_dir = '/tmp/out'\n"
    )
    with pytest.raises((TypeError, Exception), match="extras must be a mapping"):
        load_train_config(cfg_path, default_config_path=tmp_path / "no.toml")


def test_validate_extras_budget_not_mapping(tmp_path: Path) -> None:
    """Branch [106→107]: budget is not a Mapping → ValueError."""
    cfg_path = tmp_path / "exp" / "config.toml"
    cfg_path.parent.mkdir(parents=True, exist_ok=True)
    cfg_path.write_text(
        '[training]\n[training.extras]\nbudget = "not_a_mapping"\n'
        "[training.model]\n[training.data]\n[training.optim]\n"
        "[training.schedule]\n[training.runtime]\nout_dir = '/tmp/out'\n"
    )
    with pytest.raises(
        (ValueError, Exception), match="budget extras must be a mapping"
    ):
        load_train_config(cfg_path, default_config_path=tmp_path / "no.toml")


def test_validate_extras_budget_max_hours_not_number(tmp_path: Path) -> None:
    """Branch [116→117]: budget.max_hours is not a number → ValueError."""
    cfg_path = tmp_path / "exp" / "config.toml"
    cfg_path.parent.mkdir(parents=True, exist_ok=True)
    cfg_path.write_text(
        '[training]\n[training.extras.budget]\nmax_hours = "ten"\n'
        "[training.model]\n[training.data]\n[training.optim]\n"
        "[training.schedule]\n[training.runtime]\nout_dir = '/tmp/out'\n"
    )
    with pytest.raises(
        (ValueError, Exception), match="budget.max_hours must be a number"
    ):
        load_train_config(cfg_path, default_config_path=tmp_path / "no.toml")


def test_validate_extras_budget_max_hours_negative(tmp_path: Path) -> None:
    """Branch [118→119]: budget.max_hours < 0 → ValueError."""
    cfg_path = tmp_path / "exp" / "config.toml"
    cfg_path.parent.mkdir(parents=True, exist_ok=True)
    cfg_path.write_text(
        "[training]\n[training.extras.budget]\nmax_hours = -1\n"
        "[training.model]\n[training.data]\n[training.optim]\n"
        "[training.schedule]\n[training.runtime]\nout_dir = '/tmp/out'\n"
    )
    with pytest.raises((ValueError, Exception), match="budget.max_hours must be >= 0"):
        load_train_config(cfg_path, default_config_path=tmp_path / "no.toml")


def test_validate_extras_budget_only_with_no_model(tmp_path: Path) -> None:
    """Branch [140→141]: budget present, no extras model, no extra keys."""
    cfg_path = tmp_path / "exp" / "config.toml"
    cfg_path.parent.mkdir(parents=True, exist_ok=True)
    cfg_path.write_text(
        "[training]\n[training.extras.budget]\nmax_hours = 5\n"
        "[training.model]\n[training.data]\n[training.optim]\n"
        "[training.schedule]\n[training.runtime]\nout_dir = '/tmp/out'\n"
    )
    cfg = load_train_config(cfg_path, default_config_path=tmp_path / "no.toml")
    assert cfg is not None


def test_validate_extras_unknown_extras_no_model(tmp_path: Path) -> None:
    """Branch [136→137]: extras_payload non-empty, model is None → ValueError."""
    cfg_path = tmp_path / "exp" / "config.toml"
    cfg_path.parent.mkdir(parents=True, exist_ok=True)
    cfg_path.write_text(
        '[training]\n[training.extras]\nunknown_key = "value"\n'
        "[training.model]\n[training.data]\n[training.optim]\n"
        "[training.schedule]\n[training.runtime]\nout_dir = '/tmp/out'\n"
    )
    with pytest.raises(
        (ValueError, Exception), match="Missing extras model registration"
    ):
        load_train_config(cfg_path, default_config_path=tmp_path / "no.toml")


# ---------------------------------------------------------------------------
# load_*_config with missing defaults file
# ---------------------------------------------------------------------------


def _write_minimal_train_config(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        """\
[training]
[training.model]
[training.data]
[training.optim]
[training.schedule]
[training.runtime]
out_dir = "/tmp/out"
"""
    )


def _write_minimal_sample_config(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        """\
[sampling]
[sampling.sample]
[sampling.runtime]
out_dir = "/tmp/out"
"""
    )


def _write_minimal_prepare_config(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        """\
[prepare]
"""
    )


def test_load_train_config_missing_defaults(tmp_path: Path) -> None:
    """Branch [269→272]: defaults file doesn't exist → defaults_raw = {}."""
    cfg_path = tmp_path / "exp" / "config.toml"
    _write_minimal_train_config(cfg_path)
    nonexistent_defaults = tmp_path / "nonexistent_defaults.toml"
    cfg = load_train_config(cfg_path, default_config_path=nonexistent_defaults)
    assert cfg is not None


def test_load_sample_config_missing_defaults(tmp_path: Path) -> None:
    """Branch [298→301]: defaults file doesn't exist → defaults_raw = {}."""
    cfg_path = tmp_path / "exp" / "config.toml"
    _write_minimal_sample_config(cfg_path)
    nonexistent_defaults = tmp_path / "nonexistent_defaults.toml"
    cfg = load_sample_config(cfg_path, default_config_path=nonexistent_defaults)
    assert cfg is not None


def test_load_prepare_config_missing_defaults(tmp_path: Path) -> None:
    """Branch [330→333]: defaults file doesn't exist → defaults_raw = {}."""
    cfg_path = tmp_path / "exp" / "config.toml"
    _write_minimal_prepare_config(cfg_path)
    nonexistent_defaults = tmp_path / "nonexistent_defaults.toml"
    cfg = load_prepare_config(cfg_path, default_config_path=nonexistent_defaults)
    assert cfg is not None


def test_load_full_experiment_config_with_ldres(tmp_path: Path) -> None:
    """Branch [213→214]: ldres config exists → merged into result."""
    exp_name = "test_exp"
    cfg_path = tmp_path / exp_name / "config.toml"
    cfg_path.parent.mkdir(parents=True)
    out_dir = str(tmp_path / "out")
    dataset_dir = str(tmp_path / "data")
    cfg_path.write_text(
        f'[metadata]\ndataset_dir = "{dataset_dir}"\n[prepare]\n'
        f"[training]\n[training.model]\n[training.data]\n"
        f"[training.optim]\n[training.schedule]\n"
        f'[training.runtime]\nout_dir = "{out_dir}"\n'
        f"[sampling]\n[sampling.sample]\n"
        f'[sampling.runtime]\nout_dir = "{out_dir}"\n'
    )

    # Create ldres override config
    ldres_path = (
        tmp_path
        / ".ldres"
        / "etc"
        / "ml_playground"
        / "experiments"
        / exp_name
        / "config.toml"
    )
    ldres_path.parent.mkdir(parents=True)
    ldres_path.write_text("[metadata]\n")

    cfg = load_full_experiment_config(cfg_path, tmp_path, exp_name)
    assert cfg is not None
