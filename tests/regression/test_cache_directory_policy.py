from __future__ import annotations

import tomllib
from pathlib import Path
from typing import Mapping

REPO_ROOT = Path(__file__).resolve().parents[2]
PYPROJECT = REPO_ROOT / "pyproject.toml"


def _load_pyproject() -> dict[str, object]:
    with PYPROJECT.open("rb") as handle:
        return tomllib.load(handle)


def _as_mapping(value: object) -> Mapping[str, object]:
    if isinstance(value, Mapping):
        return value
    return {}


def _require_cache_prefix(value: str, label: str) -> None:
    assert value.startswith(".cache"), f"{label} must live under .cache, got {value}"


def test_cache_directories_live_under_dot_cache() -> None:
    """Enforce cache locations under .cache and forbid .pytest_cache."""
    assert not (REPO_ROOT / ".pytest_cache").exists(), (
        "Repository root must not contain .pytest_cache; cache directories should live under .cache."
    )

    data = _load_pyproject()
    tool_cfg = _as_mapping(data.get("tool"))

    pytest_cfg = _as_mapping(_as_mapping(tool_cfg.get("pytest")).get("ini_options"))
    _require_cache_prefix(str(pytest_cfg.get("cache_dir", "")), "pytest cache_dir")

    ruff_cfg = _as_mapping(tool_cfg.get("ruff"))
    _require_cache_prefix(str(ruff_cfg.get("cache-dir", "")), "ruff cache-dir")

    mypy_cfg = _as_mapping(tool_cfg.get("mypy"))
    _require_cache_prefix(str(mypy_cfg.get("cache_dir", "")), "mypy cache_dir")

    hypothesis_cfg = _as_mapping(tool_cfg.get("hypothesis"))
    _require_cache_prefix(
        str(hypothesis_cfg.get("database", "")), "hypothesis database"
    )

    mlp_cache = _as_mapping(_as_mapping(tool_cfg.get("ml_playground")).get("cache"))
    _require_cache_prefix(
        str(mlp_cache.get("base_dir", "")), "ml_playground cache.base_dir"
    )

    env_cfg = _as_mapping(mlp_cache.get("env"))
    for key, value in env_cfg.items():
        if isinstance(value, str):
            _require_cache_prefix(value, f"ml_playground cache.env.{key}")
