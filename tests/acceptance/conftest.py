"""Auto-mark all tests in tests/acceptance/ as acceptance tests.

This keeps suite selection easy via `-m acceptance` and allows
separate reporting/CI handling for acceptance tests.
"""

from __future__ import annotations

from pathlib import Path
from typing import Callable, Generator

import pytest
import subprocess

# Apply the 'acceptance' marker to every test in this package
pytestmark = pytest.mark.acceptance  # type: ignore[attr-defined]


@pytest.fixture(scope="session")  # type: ignore[arg-type]
def install_package() -> None:
    """Install the package in editable mode for CLI tests.

    This simulates the user installation process and ensures CLI entry points
    are available. Using session scope means we only install once per test session.
    """
    project_root = Path(__file__).resolve().parents[2]
    subprocess.run(
        ["uv", "pip", "install", "-e", "."],
        cwd=project_root,
        capture_output=True,
        check=True,
    )


@pytest.fixture
def project_root() -> Path:
    return Path(__file__).resolve().parents[2]


@pytest.fixture  # type: ignore[arg-type]
def run_cli(
    project_root: Path,
    install_package: None,  # Depends on package being installed
) -> Generator[Callable[..., subprocess.CompletedProcess[str]], None, None]:
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
