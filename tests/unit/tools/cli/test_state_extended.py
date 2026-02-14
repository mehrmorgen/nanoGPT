from __future__ import annotations

from pathlib import Path
import pytest
import typer
import os

from ml_playground.tools.cli.state import (
    load_config_with_error_handling,
    apply_cli_options,
    state,
    reset_state,
)
from ml_playground.tools.core.errors import ToolConfigurationError
from ml_playground.tools.core.config import ToolsConfig


def test_load_config_with_error_handling_success(tmp_path: Path) -> None:
    reset_state()
    mock_config = ToolsConfig()
    mock_config.learning_mode_default = True
    mock_config.default_verbosity = 2

    def fake_loader(project_root: Path | None) -> object:
        return mock_config

    load_config_with_error_handling(tmp_path, _loader_override=fake_loader)

    assert state.config == mock_config
    assert state.project_root == tmp_path
    assert state.learning_mode is True
    assert state.verbosity == 2


def test_load_config_with_error_handling_errors(tmp_path: Path) -> None:
    def fake_loader_config_error(project_root: Path | None) -> object:
        raise ToolConfigurationError("cfg boom", reason="r", rationale="rat")

    with pytest.raises(typer.Exit):
        load_config_with_error_handling(
            tmp_path, _loader_override=fake_loader_config_error
        )

    def fake_loader_runtime_error(project_root: Path | None) -> object:
        raise RuntimeError("unexpected")

    with pytest.raises(typer.Exit):
        load_config_with_error_handling(
            tmp_path, _loader_override=fake_loader_runtime_error
        )


def test_apply_cli_options() -> None:
    reset_state()
    apply_cli_options(learning_mode=True, verbosity=3, dry_run=True)
    assert state.learning_mode is True
    assert state.verbosity == 3
    assert state.dry_run is True
    assert os.environ.get("ML_PLAYGROUND_TOOLS_DRY_RUN") == "1"

    apply_cli_options(learning_mode=None, verbosity=None, dry_run=False)
    assert state.learning_mode is True  # Remains True from before
    assert state.verbosity == 3
    assert state.dry_run is False
    assert "ML_PLAYGROUND_TOOLS_DRY_RUN" not in os.environ
