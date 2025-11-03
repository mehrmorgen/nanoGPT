"""Tests for `ml_playground.tools.core.config`."""

from __future__ import annotations

import textwrap
from pathlib import Path

import pytest

from ml_playground.tools.core import config as config_module
from ml_playground.tools.core.config import (
    DEFAULT_TOOLS_CONFIG,
    ToolsConfig,
    ToolConfigurationError,
    load_tools_config,
    get_tool_config,
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
    with pytest.raises(ValueError):
        config_module.TestToolsConfig(timeout=-1)
    with pytest.raises(ValueError):
        config_module.TestToolsConfig(coverage_threshold=200.0)


def test_tool_config_timeout_upper_bound_validation() -> None:
    with pytest.raises(ValueError) as excinfo:
        config_module.ToolConfig(timeout=4000)

    assert "Timeout too large" in str(excinfo.value)


def test_agentic_tools_config_invalid_output_format_raises() -> None:
    with pytest.raises(ValueError) as excinfo:
        config_module.AgenticToolsConfig(output_format="xml")

    assert "Output format must be 'json' or 'yaml'" in str(excinfo.value)


def test_tools_config_default_verbosity_validation() -> None:
    with pytest.raises(ValueError) as excinfo:
        ToolsConfig(default_verbosity=5)

    assert "Verbosity level must be 0, 1, or 2" in str(excinfo.value)


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
    with pytest.raises(ToolConfigurationError) as exc:
        load_tools_config(project_root=tmp_path)
    assert "pyproject.toml not found" in str(exc.value)


def test_load_tools_config_invalid_toml(tmp_path: Path) -> None:
    pyproject = tmp_path / "pyproject.toml"
    pyproject.write_text("not = [valid")

    with pytest.raises(ToolConfigurationError) as exc:
        load_tools_config(project_root=tmp_path)
    assert "Invalid TOML" in str(exc.value)


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

    with pytest.raises(ToolConfigurationError) as exc:
        get_tool_config("nonexistent", project_root=tmp_path)
    assert "Unknown tool category" in str(exc.value)


def test_default_tools_config_structure_matches_model_defaults() -> None:
    cfg = ToolsConfig.model_validate(DEFAULT_TOOLS_CONFIG)
    assert cfg.testing.timeout == 600
    assert cfg.quality.timeout == 120
    assert cfg.environment.cache_cleanup_age_days == 7
    assert cfg.ci.badge_output_dir == Path("docs/assets")
