"""Auto-mark all tests in tests/acceptance/ as acceptance tests.

This keeps suite selection easy via `-m acceptance` and allows
separate reporting/CI handling for acceptance tests.
"""

from __future__ import annotations

import os
import subprocess
from pathlib import Path
from typing import Callable, Generator

import pytest

# Apply the 'acceptance' marker to every test in this package
pytestmark = pytest.mark.acceptance


@pytest.fixture()
def project_root() -> Path:
    return Path(__file__).resolve().parents[2]


@pytest.fixture()
def run_cli(
    project_root: Path,
) -> Generator[Callable[..., subprocess.CompletedProcess[str]], None, None]:
    def _run(command: str, *args: str) -> subprocess.CompletedProcess[str]:
        env = os.environ.copy()
        # Ensure src is in PYTHONPATH so uv run can find the package even if not installed in editable mode
        env["PYTHONPATH"] = f"{project_root / 'src'}:{env.get('PYTHONPATH', '')}"
        return subprocess.run(
            ["uv", "run", command, *args],
            cwd=project_root,
            capture_output=True,
            text=True,
            check=False,
            env=env,
        )

    yield _run
