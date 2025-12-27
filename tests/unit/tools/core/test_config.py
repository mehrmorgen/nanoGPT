"""Tests for `ml_playground.tools.core.config`."""

from __future__ import annotations

import os
import textwrap
from pathlib import Path

import pytest

from ml_playground.tools.core import config as config_module
from ml_playground.tools.core.config import (
    DEFAULT_TOOLS_CONFIG,
    ToolsConfig,
    ToolConfigurationError,
    get_tool_config,
    load_tools_config,
)


def test_tools_config_defaults() -> None:
    cfg = ToolsConfig()
    assert cfg.testing.timeout == 600
    assert cfg.testing.coverage_threshold == 87.0
    assert cfg.quality.enabled is True
    assert cfg.agentic.output_format == "json"
    assert cfg.learning_mode_default is False
    assert cfg.default_verbosity == 1


def test_tools_config_customization() -> None:
    cfg = ToolsConfig(
        testing=config_module.TestToolsConfig(
            timeout=10, parallel_workers=4, coverage_threshold=75.0
        ),
        quality=config_module.QualityToolsConfig(strict_mode=False),
        learning_mode_default=True,
        default_verbosity=2,
    )

    assert cfg.testing.timeout == 10
    assert cfg.testing.parallel_workers == 4
    assert cfg.testing.coverage_threshold == 75.0
    assert cfg.quality.strict_mode is False
    assert cfg.learning_mode_default is True
    assert cfg.default_verbosity == 2


def test_test_tools_config_validation() -> None:
    with pytest.raises(ValueError, match="Timeout"):
        config_module.TestToolsConfig(timeout=-1)

    with pytest.raises(ValueError, match="Coverage threshold"):
        config_module.TestToolsConfig(coverage_threshold=200.0)


def test_tool_config_timeout_upper_bound_validation() -> None:
    with pytest.raises(ValueError, match="Timeout too large"):
        config_module.ToolConfig(timeout=4000)


def test_agentic_tools_config_invalid_output_format_raises() -> None:
    with pytest.raises(ValueError, match="json.*yaml"):
        config_module.AgenticToolsConfig(output_format="xml")


def test_tools_config_default_verbosity_validation() -> None:
    with pytest.raises(ValueError, match="Verbosity level must be 0, 1, or 2"):
        ToolsConfig(default_verbosity=5)


def test_load_tools_config_success(tmp_path: Path) -> None:
    pyproject = tmp_path / "pyproject.toml"
    pyproject.write_text(
        textwrap.dedent(
            """
            [tool.ml_playground.tools.testing]
            timeout = 50
            coverage_threshold = 91.5

            [tool.ml_playground.tools.quality]
            strict_mode = false
            """
        ).strip()
    )

    config = load_tools_config(project_root=tmp_path)

    assert config.testing.timeout == 50
    assert config.testing.coverage_threshold == 91.5
    assert config.quality.strict_mode is False


def test_load_tools_config_missing_file(tmp_path: Path) -> None:
    with pytest.raises(ToolConfigurationError, match="pyproject.toml not found"):
        load_tools_config(project_root=tmp_path)


def test_load_tools_config_invalid_toml(tmp_path: Path) -> None:
    pyproject = tmp_path / "pyproject.toml"
    pyproject.write_text("not = [valid")

    with pytest.raises(ToolConfigurationError, match="Invalid TOML"):
        load_tools_config(project_root=tmp_path)


def test_load_tools_config_invalid_schema_wrapped(tmp_path: Path) -> None:
    """Invalid values inside tools section surface as ToolConfigurationError."""
    pyproject = tmp_path / "pyproject.toml"
    pyproject.write_text(
        textwrap.dedent(
            """
            [tool.ml_playground.tools]
            default_verbosity = "bad"
            """
        )
    )

    with pytest.raises(ToolConfigurationError, match="Invalid tools configuration"):
        load_tools_config(project_root=tmp_path)


def test_load_tools_config_discovers_project_root(tmp_path: Path) -> None:
    """_find_project_root finds pyproject.toml when called from a subdirectory."""
    project_root = tmp_path / "project"
    project_root.mkdir()
    subdir = project_root / "nested"
    subdir.mkdir()

    pyproject = project_root / "pyproject.toml"
    pyproject.write_text(
        textwrap.dedent(
            """
            [tool.ml_playground.tools]
            """
        ).strip()
    )

    original_cwd = Path.cwd()
    try:
        os.chdir(subdir)
        config = load_tools_config(project_root=None)
    finally:
        os.chdir(original_cwd)

    assert config.learning_mode_default is False
    assert config.default_verbosity == 1


def test_get_tool_config_returns_specific_category(tmp_path: Path) -> None:
    pyproject = tmp_path / "pyproject.toml"
    pyproject.write_text(
        textwrap.dedent(
            """
            [tool.ml_playground.tools.environment]
            timeout = 42
            """
        ).strip()
    )

    env_config = get_tool_config("environment", project_root=tmp_path)
    assert env_config.timeout == 42


def test_get_tool_config_invalid_category(tmp_path: Path) -> None:
    pyproject = tmp_path / "pyproject.toml"
    pyproject.write_text("[tool.ml_playground.tools]")

    with pytest.raises(ToolConfigurationError, match="Unknown tool category"):
        get_tool_config("nonexistent", project_root=tmp_path)


def test_default_tools_config_structure_matches_model_defaults() -> None:
    cfg = ToolsConfig.model_validate(DEFAULT_TOOLS_CONFIG)
    assert cfg.testing.timeout == 600
    assert cfg.quality.timeout == 120
    assert cfg.environment.cache_cleanup_age_days == 7
    assert cfg.ci.badge_output_dir == Path("docs/assets")


def test_find_project_root_without_pyproject_fails(tmp_path: Path) -> None:
    """Fail fast when no pyproject is found in any parent."""
    original_cwd = Path.cwd()
    os.chdir(tmp_path)
    try:
        with pytest.raises(ToolConfigurationError, match="pyproject.toml not found"):
            load_tools_config(project_root=None)
    finally:
        os.chdir(original_cwd)


def test_load_tools_config_permission_error_is_wrapped(tmp_path: Path) -> None:
    """Permission errors surface as ToolConfigurationError via load_tools_config."""
    pyproject = tmp_path / "pyproject.toml"
    pyproject.write_text("dummy = true")
    pyproject.chmod(0)
    try:
        with pytest.raises(
            ToolConfigurationError, match="Failed to load pyproject.toml"
        ):
            load_tools_config(project_root=tmp_path)
    finally:
        pyproject.chmod(0o644)
