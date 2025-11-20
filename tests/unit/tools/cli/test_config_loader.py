"""Unit tests for ml_playground.tools.cli.config_loader."""

from __future__ import annotations

from pathlib import Path

import pytest
import typer

from ml_playground.tools.cli.config_loader import load_config_with_error_handling
from ml_playground.tools.core.errors import ToolConfigurationError
from ml_playground.tools.core.config import ToolsConfig


def test_load_config_with_error_handling_tool_config_error(tmp_path: Path) -> None:
    """ToolConfigurationError should be echoed and cause typer.Exit(1).

    This exercises the ToolConfigurationError-specific branch.
    """

    class FailingDeps:
        def load_config(self, root: Path | None) -> ToolsConfig:
            raise ToolConfigurationError(
                "test config error",
                reason="invalid config",
                rationale="test failure",
            )

    with pytest.raises(typer.Exit):
        load_config_with_error_handling(project_root=tmp_path, deps=FailingDeps())


def test_load_config_with_error_handling_unexpected_error(tmp_path: Path) -> None:
    """Unexpected exceptions should be echoed and cause typer.Exit(1).

    This exercises the generic Exception branch.
    """

    class FailingDeps:
        def load_config(self, root: Path | None) -> ToolsConfig:
            raise RuntimeError("unexpected boom")

    with pytest.raises(typer.Exit):
        load_config_with_error_handling(project_root=tmp_path, deps=FailingDeps())
