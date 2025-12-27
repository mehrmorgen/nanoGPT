"""Configuration management for the ML Playground tools system.

This module provides configuration loading and validation for the tools system,
following the project's TOML-based configuration pattern with explicit timeout
management and Pydantic validation.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, Dict, List, Optional

import tomllib
from pydantic import BaseModel, Field, field_validator

from ml_playground.tools.core.errors import ToolConfigurationError

logger = logging.getLogger(__name__)


class ToolConfig(BaseModel):
    """Base configuration for all tools.

    Follows the project's timeout philosophy: there is no such thing as an
    infinite timeout. All timeouts should be short and based on the specific
    operation and environment.
    """

    enabled: bool = Field(
        default=True, description="Whether this tool category is enabled"
    )
    timeout: int = Field(
        default=300,  # 5 minutes - explicit timeout based on expected operation duration
        description="Timeout in seconds. No infinite timeouts - choose based on expected operation duration and environment",
    )
    environment_vars: Dict[str, str] = Field(
        default_factory=dict,
        description="Environment variables to set when running tools",
    )

    @field_validator("timeout")
    @classmethod
    def validate_timeout(cls, v: int) -> int:
        """Validate timeout is reasonable and follows project philosophy."""
        if v <= 0:
            raise ValueError("Timeout must be positive")
        if v > 3600:  # 1 hour maximum
            raise ValueError(
                "Timeout too large - choose based on expected operation duration"
            )
        return v


class QualityToolsConfig(ToolConfig):
    """Configuration for quality tools (lint, format, typecheck)."""

    timeout: int = Field(
        default=120,  # 2 minutes - linting/formatting should be fast
        description="Timeout for quality tools - should complete quickly in normal environments",
    )
    ruff_config_path: Optional[Path] = Field(
        default=None, description="Path to ruff configuration file"
    )
    mypy_config_section: str = Field(
        default="tool.mypy", description="TOML section for mypy configuration"
    )
    basedpyright_config_section: str = Field(
        default="tool.pyright",
        description="TOML section for basedpyright configuration",
    )
    strict_mode: bool = Field(
        default=True, description="Enable strict mode for type checking (BasedPyright)"
    )


class TestToolsConfig(ToolConfig):
    """Configuration for testing tools."""

    timeout: int = Field(
        default=600,  # 10 minutes - tests can take longer but should have reasonable bounds
        description="Timeout for test execution - based on expected test suite duration",
    )
    pytest_args: List[str] = Field(
        default_factory=list, description="Additional arguments to pass to pytest"
    )
    coverage_threshold: float = Field(
        default=87.0,
        description="Minimum coverage threshold (matches project standard)",
    )
    parallel_workers: int = Field(
        default=-1,  # auto-detect
        description="Number of parallel workers for test execution (-1 for auto)",
    )

    @field_validator("coverage_threshold")
    @classmethod
    def validate_coverage_threshold(cls, v: float) -> float:
        """Validate coverage threshold is reasonable."""
        if not 0.0 <= v <= 100.0:
            raise ValueError("Coverage threshold must be between 0 and 100")
        return v


class EnvironmentToolsConfig(ToolConfig):
    """Configuration for environment management tools."""

    timeout: int = Field(
        default=300,  # 5 minutes - environment operations should be reasonably fast
        description="Timeout for environment operations",
    )
    cache_cleanup_age_days: int = Field(
        default=7,
        description="Age in days after which cache entries are considered stale",
    )
    verify_dependencies: bool = Field(
        default=True,
        description="Whether to verify all dependencies during environment setup",
    )


class CIToolsConfig(ToolConfig):
    """Configuration for CI/CD tools."""

    timeout: int = Field(
        default=900,  # 15 minutes - CI operations can be longer but still bounded
        description="Timeout for CI operations including mutation testing",
    )
    mutation_timeout: int = Field(
        default=60,  # 1 minute per mutation
        description="Timeout per individual mutation test",
    )
    badge_output_dir: Path = Field(
        default=Path("docs/assets"), description="Directory for generated badges"
    )


class AgenticToolsConfig(ToolConfig):
    """Configuration for AI-assisted development tools."""

    timeout: int = Field(
        default=180,  # 3 minutes - agentic operations should be fast
        description="Timeout for agentic tool operations",
    )
    batch_size: int = Field(
        default=10, description="Default batch size for batch operations"
    )
    output_format: str = Field(
        default="json",
        description="Default output format for structured data (json, yaml)",
    )

    @field_validator("output_format")
    @classmethod
    def validate_output_format(cls, v: str) -> str:
        """Validate output format is supported."""
        if v not in {"json", "yaml"}:
            raise ValueError("Output format must be 'json' or 'yaml'")
        return v


class ToolsConfig(BaseModel):
    """Main configuration for the tool integration system."""

    quality: QualityToolsConfig = Field(
        default_factory=QualityToolsConfig,
        description="Configuration for quality tools",
    )
    testing: TestToolsConfig = Field(
        default_factory=TestToolsConfig, description="Configuration for testing tools"
    )
    environment: EnvironmentToolsConfig = Field(
        default_factory=EnvironmentToolsConfig,
        description="Configuration for environment tools",
    )
    ci: CIToolsConfig = Field(
        default_factory=CIToolsConfig, description="Configuration for CI tools"
    )
    agentic: AgenticToolsConfig = Field(
        default_factory=AgenticToolsConfig,
        description="Configuration for agentic tools",
    )

    learning_mode_default: bool = Field(
        default=False, description="Whether learning mode is enabled by default"
    )
    default_verbosity: int = Field(
        default=1, description="Default verbosity level for learning mode (0-2)"
    )

    @field_validator("default_verbosity")
    @classmethod
    def validate_verbosity(cls, v: int) -> int:
        """Validate verbosity level is in valid range."""
        if not 0 <= v <= 2:
            raise ValueError("Verbosity level must be 0, 1, or 2")
        return v


def _find_project_root() -> Path:
    """Find the project root by looking for pyproject.toml."""
    current = Path.cwd()
    while current != current.parent:
        if (current / "pyproject.toml").exists():
            return current
        current = current.parent

    raise ToolConfigurationError(
        "pyproject.toml not found in any parent directory",
        reason="Project root could not be determined",
        rationale="Tool configuration must be rooted at an explicit pyproject.toml",
    )


def _load_pyproject_toml(project_root: Path) -> Dict[str, Any]:
    """Load pyproject.toml configuration."""
    pyproject_path = project_root / "pyproject.toml"

    if not pyproject_path.exists():
        raise ToolConfigurationError(
            f"pyproject.toml not found at {pyproject_path}",
            reason="Configuration file is missing from project root",
            rationale="Tool configuration must be present in pyproject.toml to ensure consistent behavior",
        )

    try:
        with open(pyproject_path, "rb") as f:
            data = tomllib.load(f)
    except tomllib.TOMLDecodeError as e:
        raise ToolConfigurationError(
            f"Invalid TOML in pyproject.toml: {e}",
            reason="Configuration file contains invalid TOML syntax",
            rationale="Configuration must be valid TOML to be parsed reliably",
        ) from e

    return data


def load_tools_config(project_root: Path | None = None) -> ToolsConfig:
    """Load tools configuration from pyproject.toml.

    Args:
        project_root: Path to project root. If None, will search for pyproject.toml

    Returns:
        ToolsConfig: Validated configuration object

    Raises:
        ToolConfigurationError: If configuration is invalid or missing
    """
    if project_root is None:
        project_root = _find_project_root()

    logger.debug(f"Loading tools configuration from {project_root}")

    try:
        pyproject_data = _load_pyproject_toml(project_root)
    except Exception as e:
        if isinstance(e, ToolConfigurationError):
            raise
        raise ToolConfigurationError(
            f"Failed to load pyproject.toml: {e}",
            reason="Unexpected error during configuration loading",
            rationale="Configuration loading must be reliable for consistent tool behavior",
        ) from e

    # Extract tools configuration from [tool.ml_playground.tools] section
    tool_section = pyproject_data.get("tool", {})
    ml_playground_section = tool_section.get("ml_playground", {})
    tools_section = ml_playground_section.get("tools", {})

    try:
        config = ToolsConfig.model_validate(tools_section)
        logger.debug("Successfully loaded tools configuration")
        return config
    except Exception as e:
        raise ToolConfigurationError(
            f"Invalid tools configuration: {e}",
            reason="Configuration validation failed",
            rationale="All configuration must pass validation to ensure predictable tool behavior",
        ) from e


def get_tool_config(category: str, project_root: Path | None = None) -> ToolConfig:
    """Get configuration for a specific tool category.

    Args:
        category: Tool category name (quality, testing, environment, ci, agentic)
        project_root: Path to project root. If None, will search for pyproject.toml

    Returns:
        ToolConfig: Configuration for the specified category

    Raises:
        ToolConfigurationError: If category is invalid or configuration fails to load
    """
    config = load_tools_config(project_root)

    if not hasattr(config, category):
        raise ToolConfigurationError(
            f"Unknown tool category: {category}",
            reason="Requested category is not defined in configuration schema",
            rationale="Tool categories must be predefined to ensure consistent behavior",
        )

    return getattr(config, category)


# Default configuration for use in pyproject.toml
DEFAULT_TOOLS_CONFIG = {
    "learning_mode_default": False,
    "default_verbosity": 1,
    "quality": {
        "enabled": True,
        "timeout": 120,
        "strict_mode": True,
    },
    "testing": {
        "enabled": True,
        "timeout": 600,
        "coverage_threshold": 87.0,
        "parallel_workers": -1,
    },
    "environment": {
        "enabled": True,
        "timeout": 300,
        "cache_cleanup_age_days": 7,
        "verify_dependencies": True,
    },
    "ci": {
        "enabled": True,
        "timeout": 900,
        "mutation_timeout": 60,
        "badge_output_dir": "docs/assets",
    },
    "agentic": {
        "enabled": True,
        "timeout": 180,
        "batch_size": 10,
        "output_format": "json",
    },
}
