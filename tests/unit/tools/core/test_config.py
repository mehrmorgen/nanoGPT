"""Tests for `ml_playground.tools.core.config`."""

from __future__ import annotations

from contextlib import contextmanager
import textwrap
from pathlib import Path
from typing import Generator

import pytest

import ml_playground.tools.core.config as config_module
from ml_playground.tools.core.config import (
    DEFAULT_TOOLS_CONFIG,
    ToolsConfig,
    ToolConfigurationError,
    load_tools_config,
    get_tool_config,
)


@contextmanager
def _override_attr(
    obj: object, name: str, value: object
) -> Generator[None, None, None]:
    original = getattr(obj, name)
    setattr(obj, name, value)
    try:
        yield
    finally:
        setattr(obj, name, original)


def test_tools_config_defaults() -> None:
    cfg = ToolsConfig()
    assert cfg.testing.timeout == 600
    assert cfg.testing.coverage_threshold == 87.0
    assert cfg.quality.enabled is True
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


def test_tools_config_default_verbosity_validation() -> None:
    with pytest.raises(ValueError) as excinfo:
        ToolsConfig(default_verbosity=5)

    assert "Verbosity level must be 0, 1, or 2" in str(excinfo.value)


def test_find_project_root_finds_pyproject(tmp_path: Path) -> None:
    project_root = tmp_path / "repo"
    nested = project_root / "a" / "b"
    nested.mkdir(parents=True)

    (project_root / "pyproject.toml").write_text("[tool.ml_playground.tools]\n")

    with _override_attr(config_module.Path, "cwd", staticmethod(lambda: nested)):
        assert config_module._find_project_root() == project_root


def test_find_project_root_fallbacks_to_cwd_when_missing(tmp_path: Path) -> None:
    nested = tmp_path / "no_pyproject" / "a" / "b"
    nested.mkdir(parents=True)

    with _override_attr(config_module.Path, "cwd", staticmethod(lambda: nested)):
        with pytest.raises(ToolConfigurationError):
            config_module._find_project_root()


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


def test_load_tools_config_wraps_oserror(tmp_path: Path) -> None:
    def boom(_project_root: Path) -> dict[str, object]:
        raise OSError("boom")

    with _override_attr(config_module, "_load_pyproject_toml", boom):
        with pytest.raises(ToolConfigurationError) as exc:
            load_tools_config(project_root=tmp_path)

    assert "Failed to load pyproject.toml" in str(exc.value)


def test_load_tools_config_wraps_validation_error(tmp_path: Path) -> None:
    pyproject = tmp_path / "pyproject.toml"
    pyproject.write_text(
        textwrap.dedent(
            """
            [tool.ml_playground.tools]
            default_verbosity = 5
            """
        ).strip()
    )

    with pytest.raises(ToolConfigurationError) as exc:
        load_tools_config(project_root=tmp_path)
    assert "Invalid tools configuration" in str(exc.value)


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
