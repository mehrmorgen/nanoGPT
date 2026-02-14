"""Utilities for reading project configuration from pyproject.toml."""

from __future__ import annotations

import tomllib
from pathlib import Path


def _as_mapping(value: object) -> dict[str, object]:
    if isinstance(value, dict):
        return value
    return {}


def get_pyproject_config() -> dict[str, object]:
    """Read the pyproject.toml file and return the ml_playground section."""
    pyproject_path = Path(__file__).parent.parent.parent.parent / "pyproject.toml"

    if not pyproject_path.exists():
        return {}

    with open(pyproject_path, "rb") as file_obj:
        config_obj: object = tomllib.load(file_obj)

    config = _as_mapping(config_obj)
    tool_section = _as_mapping(config.get("tool"))
    return _as_mapping(tool_section.get("ml_playground"))


def get_server_config() -> dict[str, object]:
    """Get server configuration from pyproject.toml."""
    ml_config = get_pyproject_config()
    return _as_mapping(ml_config.get("server"))


def get_default_host() -> str:
    """Get the default server host from pyproject.toml."""
    server_config = get_server_config()
    host = server_config.get("default_host")
    if not host:
        raise ValueError("Missing [tool.ml_playground.server].default_host")
    if not isinstance(host, str):
        raise TypeError("server.default_host must be a string")
    return host
