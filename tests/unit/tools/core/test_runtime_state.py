"""Tests for `ml_playground.tools.core.runtime` state helpers."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
import typer

import ml_playground.tools.core.runtime as runtime
from ml_playground.tools.core.config import ToolsConfig
from ml_playground.tools.core.errors import ToolConfigurationError


def _write_minimal_pyproject(project_root: Path) -> None:
    content = """
[tool.ml_playground.tools]
""".strip()
    (project_root / "pyproject.toml").write_text(content, encoding="utf-8")


# State reset testing is covered by PBT test_state_reset_clears_fields


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


def test_load_config_with_error_handling_preserves_explicit_learning_mode(
    tmp_path: Path,
) -> None:
    """Explicitly set learning mode should not be overridden by config default."""

    runtime.reset_state()
    _write_minimal_pyproject(tmp_path)

    runtime.state.learning_mode = True
    runtime.state.mark_learning_mode_explicit(True)

    runtime.load_config_with_error_handling(project_root=tmp_path)

    assert runtime.state.config is not None
    assert runtime.state.project_root == tmp_path
    assert runtime.state.learning_mode is True
    # learning_mode_set should reflect explicit configuration, not defaulting
    assert runtime.state.learning_mode_set is True


def test_load_config_with_error_handling_reports_configuration_error(
    tmp_path: Path,
    capsys: Any,
) -> None:
    """Configuration errors should be echoed and cause typer.Exit(1).

    This exercises the ToolConfigurationError-specific branch.
    """

    runtime.reset_state()

    def fake_load_tools_config(_root: Path | None) -> ToolsConfig:
        raise ToolConfigurationError(
            "bad config",
            reason="test",
            rationale="exercise ToolConfigurationError branch",
        )

    original = runtime.load_tools_config
    try:
        runtime.load_tools_config = fake_load_tools_config  # type: ignore[assignment]

        with pytest.raises(typer.Exit) as exc_info:
            runtime.load_config_with_error_handling(project_root=tmp_path)
    finally:
        runtime.load_tools_config = original  # type: ignore[assignment]

    assert isinstance(exc_info.value, typer.Exit)
    exit_exc = exc_info.value
    assert exit_exc.exit_code == 1
    captured = capsys.readouterr()
    assert "Configuration error: bad config" in captured.err


def test_load_config_with_error_handling_reports_unexpected_error(
    tmp_path: Path,
    capsys: Any,
) -> None:
    """Unexpected exceptions should be reported and cause typer.Exit(1).

    This exercises the generic Exception branch.
    """

    runtime.reset_state()

    def fake_load_tools_config(_root: Path | None) -> ToolsConfig:
        raise RuntimeError("boom")

    original = runtime.load_tools_config
    try:
        runtime.load_tools_config = fake_load_tools_config  # type: ignore[assignment]

        with pytest.raises(typer.Exit) as exc_info:
            runtime.load_config_with_error_handling(project_root=tmp_path)
    finally:
        runtime.load_tools_config = original  # type: ignore[assignment]

    assert isinstance(exc_info.value, typer.Exit)
    assert exc_info.value.exit_code == 1
    captured = capsys.readouterr()
    assert "Unexpected error loading configuration: boom" in captured.err
