from __future__ import annotations

from pathlib import Path

import pytest

from ml_playground.configuration.loading import (
    _default_config_path_from_root,
    _validate_budget,
)


@pytest.mark.parametrize(
    "budget",
    [
        {},
        {"budget": None},
    ],
)
def test_validate_budget_allows_missing_or_none(budget: dict[str, object]) -> None:
    assert _validate_budget(budget) is None


def test_validate_budget_rejects_unknown_keys() -> None:
    with pytest.raises(ValueError, match="unknown keys"):
        _validate_budget({"budget": {"max_hours": 1, "extra": 2}})


@pytest.mark.parametrize(
    "payload,errmsg",
    [
        ({"budget": "oops"}, "must be a mapping"),
        ({"budget": {"max_hours": "x"}}, "must be a number"),
        ({"budget": {"max_hours": -1}}, "must be >= 0"),
        ({"budget": {"max_games": 1.5}}, "must be an integer"),
        ({"budget": {"max_games": -2}}, "must be >= 0"),
    ],
)
def test_validate_budget_rejects_invalid_shapes(
    payload: dict[str, object], errmsg: str
) -> None:
    with pytest.raises(ValueError, match=errmsg):
        _validate_budget(payload)


def test_default_config_path_handles_project_layouts(tmp_path: Path) -> None:
    # Simulate running from package parent (project_root == package_root.parent)
    package_parent = tmp_path / "parent"
    package_parent.mkdir(parents=True)
    (package_parent / "src" / "ml_playground").mkdir(parents=True)
    path = _default_config_path_from_root(package_parent)
    assert (
        path
        == package_parent
        / "src"
        / "ml_playground"
        / "experiments"
        / "default_config.toml"
    )
