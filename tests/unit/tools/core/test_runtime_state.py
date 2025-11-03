"""Tests for `ml_playground.tools.core.runtime` state helpers."""

from __future__ import annotations

from pathlib import Path

import pytest
import typer

from ml_playground.tools.core import runtime
from ml_playground.tools.core.config import ToolsConfig


def _write_minimal_pyproject(project_root: Path) -> None:
    content = """
[tool.ml_playground.tools]
""".strip()
    (project_root / "pyproject.toml").write_text(content, encoding="utf-8")


def test_reset_state_clears_fields(tmp_path: Path) -> None:
    """Resetting state should clear previously stored values."""
    runtime.state.learning_mode = True
    runtime.state.verbosity = 2
    runtime.state.dry_run = True
    runtime.state.project_root = tmp_path
    runtime.state.config = ToolsConfig()

    runtime.reset_state()

    assert runtime.state.learning_mode is False
    assert runtime.state.verbosity == 1
    assert runtime.state.dry_run is False
    assert runtime.state.project_root is None
    assert runtime.state.config is None


def test_set_config_injects_configuration(tmp_path: Path) -> None:
    """`set_config` should populate state with provided configuration."""
    runtime.reset_state()
    config = ToolsConfig()

    runtime.set_config(config, project_root=tmp_path)

    assert runtime.state.config is config
    assert runtime.state.project_root == tmp_path


def test_load_config_with_error_handling_success(tmp_path: Path) -> None:
    """Loading configuration should set defaults when pyproject exists."""
    runtime.reset_state()
    _write_minimal_pyproject(tmp_path)

    runtime.load_config_with_error_handling(project_root=tmp_path)

    assert runtime.state.config is not None
    assert runtime.state.project_root == tmp_path
    assert runtime.state.learning_mode == runtime.state.config.learning_mode_default
    assert runtime.state.verbosity == runtime.state.config.default_verbosity


def test_load_config_with_error_handling_failure(tmp_path: Path) -> None:
    """Missing configuration should raise `typer.Exit` and leave state empty."""
    runtime.reset_state()

    with pytest.raises(typer.Exit) as exc_info:
        runtime.load_config_with_error_handling(project_root=tmp_path)

    assert exc_info.value.exit_code == 1
    assert runtime.state.config is None
    assert runtime.state.project_root is None
