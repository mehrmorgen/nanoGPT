from __future__ import annotations

from pathlib import Path
from tempfile import TemporaryDirectory

import pytest
from hypothesis import HealthCheck, given, settings, strategies as st

from ml_playground.framework.configuration import loading as config_loading


def _write_train_config(config_path: Path, budget_toml: str) -> None:
    config_path.write_text(
        "\n".join(
            [
                "[training]",
                "[training.runtime]",
                "out_dir = '.'",
                "[training.extras.budget]",
                budget_toml,
                "",
            ]
        ),
        encoding="utf-8",
    )


@settings(
    max_examples=30,
    deadline=None,
    derandomize=True,
    suppress_health_check=[HealthCheck.function_scoped_fixture],
)
@given(
    max_hours=st.floats(
        min_value=0.0, max_value=200.0, allow_nan=False, allow_infinity=False
    ),
    max_games=st.integers(min_value=0, max_value=10_000),
)
def test_load_train_config_accepts_budget_constraints(
    max_hours: float, max_games: int
) -> None:
    """Budget constraints are accepted for experiments with extras models."""
    with TemporaryDirectory() as tmp_dir:
        config_dir = Path(tmp_dir) / "shakespeare"
        config_dir.mkdir()
        config_path = config_dir / "config.toml"
        _write_train_config(
            config_path,
            "\n".join(
                [
                    f"max_hours = {max_hours}",
                    f"max_games = {max_games}",
                ]
            ),
        )
        cfg = config_loading.load_train_config(config_path)
        budget = cfg.extras["budget"]
        assert budget["max_hours"] == float(max_hours)
        assert budget["max_games"] == max_games


@settings(
    max_examples=20,
    deadline=None,
    derandomize=True,
    suppress_health_check=[HealthCheck.function_scoped_fixture],
)
@given(
    max_hours=st.floats(
        min_value=0.0, max_value=50.0, allow_nan=False, allow_infinity=False
    ),
    max_games=st.integers(min_value=0, max_value=10_000),
)
def test_load_train_config_accepts_budget_without_extras_model(
    max_hours: float, max_games: int
) -> None:
    """Budget constraints are accepted even without extras models."""
    with TemporaryDirectory() as tmp_dir:
        config_dir = Path(tmp_dir) / "budget_only"
        config_dir.mkdir()
        config_path = config_dir / "config.toml"
        _write_train_config(
            config_path,
            "\n".join(
                [
                    f"max_hours = {max_hours}",
                    f"max_games = {max_games}",
                ]
            ),
        )
        cfg = config_loading.load_train_config(config_path)
        budget = cfg.extras["budget"]
        assert budget["max_hours"] == float(max_hours)
        assert budget["max_games"] == max_games


@settings(
    max_examples=20,
    deadline=None,
    derandomize=True,
    suppress_health_check=[HealthCheck.function_scoped_fixture],
)
@given(
    kind=st.sampled_from(
        ["negative_hours", "negative_games", "float_games", "unknown_key"]
    )
)
def test_load_train_config_rejects_invalid_budget_constraints(kind: str) -> None:
    """Invalid budget values are rejected."""
    if kind == "negative_hours":
        budget_toml = "max_hours = -1"
    elif kind == "negative_games":
        budget_toml = "max_games = -2"
    elif kind == "float_games":
        budget_toml = "max_games = 1.5"
    else:
        budget_toml = "max_steps = 10"

    with TemporaryDirectory() as tmp_dir:
        config_dir = Path(tmp_dir) / "shakespeare"
        config_dir.mkdir()
        config_path = config_dir / "config.toml"
        _write_train_config(config_path, budget_toml)
        with pytest.raises(ValueError):
            config_loading.load_train_config(config_path)
