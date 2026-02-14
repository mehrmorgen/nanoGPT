"""Tests for core/project_config.py using the public API."""

from __future__ import annotations

import pytest

from ml_playground.framework.core.project_config import (
    get_default_host,
    get_pyproject_config,
    get_server_config,
)


def test_get_pyproject_config_returns_dict() -> None:
    """get_pyproject_config returns a dict (empty when pyproject.toml not at expected path)."""
    result = get_pyproject_config()
    assert isinstance(result, dict)


def test_get_server_config_returns_dict() -> None:
    """get_server_config returns a dict (empty when config not found)."""
    result = get_server_config()
    assert isinstance(result, dict)


def test_get_default_host_raises_when_config_missing() -> None:
    """get_default_host raises ValueError when pyproject.toml is not at the expected path."""
    with pytest.raises(ValueError, match="Missing"):
        get_default_host()
