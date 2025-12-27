"""Tests for tools.core.runtime helper functions."""

from __future__ import annotations

import textwrap
from pathlib import Path

import pytest
import typer

from ml_playground.tools.core.config import ToolsConfig
from ml_playground.tools.core.runtime import (
    load_config_with_error_handling,
    reset_state,
    set_config,
    state,
)


def test_load_config_with_error_handling_happy_path(tmp_path: Path) -> None:
    """Loads config from a temp pyproject and updates state."""
    pyproject = tmp_path / "pyproject.toml"
    pyproject.write_text(
        textwrap.dedent(
            """
            [tool.ml_playground.tools]
            """
        ).strip()
    )

    reset_state()
    load_config_with_error_handling(project_root=tmp_path)

    assert state.config is not None
    assert state.learning_mode == state.config.learning_mode_default
    assert state.verbosity == state.config.default_verbosity
    assert state.project_root == tmp_path


def test_load_config_with_error_handling_missing_file_exits(tmp_path: Path) -> None:
    """Fails fast with typer.Exit when configuration is missing."""
    reset_state()
    with pytest.raises(typer.Exit):
        load_config_with_error_handling(project_root=tmp_path)


def test_apply_learning_defaults_respects_manual_override(tmp_path: Path) -> None:
    """Learning defaults are skipped if learning_mode was explicitly set."""
    pyproject = tmp_path / "pyproject.toml"
    pyproject.write_text(
        textwrap.dedent(
            """
            [tool.ml_playground.tools]
            learning_mode_default = false
            default_verbosity = 0
            """
        ).strip()
    )

    reset_state()
    state._learning_mode_set = True  # pyright: ignore[reportPrivateUsage]
    state.learning_mode = True
    state.verbosity = 2

    load_config_with_error_handling(project_root=tmp_path)

    assert state.learning_mode is True  # unchanged due to override flag
    assert state.verbosity == 2


def test_apply_learning_defaults_no_config_is_noop() -> None:
    """Calling apply_learning_defaults without config leaves defaults unchanged."""
    reset_state()
    state.apply_learning_defaults()
    assert state.learning_mode is False
    assert state.verbosity == 1


def test_apply_learning_defaults_sets_from_config_when_not_overridden(
    tmp_path: Path,
) -> None:
    """Learning defaults apply when no override is set."""
    pyproject = tmp_path / "pyproject.toml"
    pyproject.write_text(
        textwrap.dedent(
            """
            [tool.ml_playground.tools]
            learning_mode_default = true
            default_verbosity = 2
            """
        ).strip()
    )

    reset_state()
    state._learning_mode_set = False  # pyright: ignore[reportPrivateUsage]
    load_config_with_error_handling(project_root=tmp_path)

    assert state.config is not None
    assert state.learning_mode is True
    assert state.verbosity == 2


def test_apply_learning_defaults_direct_call_with_config() -> None:
    """Direct apply_learning_defaults uses loaded config when override flag is false."""
    reset_state()
    state.config = ToolsConfig()  # defaults
    state._learning_mode_set = False  # pyright: ignore[reportPrivateUsage]

    state.apply_learning_defaults()

    assert state.learning_mode == state.config.learning_mode_default
    assert state.verbosity == state.config.default_verbosity


def test_set_config_applies_defaults_and_project_root(tmp_path: Path) -> None:
    """set_config injects config, project_root, and applies defaults."""
    reset_state()
    cfg = ToolsConfig()
    set_config(cfg, project_root=tmp_path)

    assert state.config is cfg
    assert state.project_root == tmp_path
    assert state.learning_mode == cfg.learning_mode_default
    assert state.verbosity == cfg.default_verbosity
