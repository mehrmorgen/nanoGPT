"""Auto-mark all tests in tests/acceptance/ as acceptance tests.

This keeps suite selection easy via `-m acceptance` and allows
separate reporting/CI handling for acceptance tests.
"""

from __future__ import annotations

from pathlib import Path
from typing import Callable, Generator, cast

import pytest
import subprocess
from _pytest.mark.structures import MarkDecorator


acceptance_marker: MarkDecorator = cast(
    MarkDecorator,
    getattr(pytest.mark, "acceptance"),
)
pytestmark: MarkDecorator = acceptance_marker

RunCli = Callable[..., subprocess.CompletedProcess[str]]

_install_package_called = False


@pytest.fixture
def install_package() -> None:
    """Provision project environment for CLI tests using uv sync.

    This ensures CLI entry points are available in a project-local environment
    and remains compatible with externally-managed interpreters (e.g. Nix).
    Using session scope means we only perform sync once per test session.
    """
    global _install_package_called
    if _install_package_called:
        return
    project_root = Path(__file__).resolve().parents[2]
    subprocess.run(
        ["uv", "sync", "--group", "all"],
        cwd=project_root,
        capture_output=True,
        check=True,
    )
    _install_package_called = True


@pytest.fixture
def _install_package(install_package: None) -> None:
    """Legacy alias that keeps the CLI fixtures happy."""
    assert install_package is None


@pytest.fixture
def project_root() -> Path:
    return Path(__file__).resolve().parents[2]


@pytest.fixture
def run_cli(
    project_root: Path,
    _install_package: None,  # Depends on package being installed
) -> Generator[RunCli, None, None]:
    def _run(command: str, *args: str) -> subprocess.CompletedProcess[str]:
        # Run CLI commands normally after package installation
        # This tests the actual user experience with installed CLI tools
        return subprocess.run(
            ["uv", "run", command, *args],
            cwd=project_root,
            capture_output=True,
            text=True,
            check=False,
        )

    yield _run
