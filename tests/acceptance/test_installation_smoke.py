from __future__ import annotations

import subprocess
from collections.abc import Callable

RunCli = Callable[..., subprocess.CompletedProcess[str]]


def test_uv_run_can_import_package(run_cli: RunCli) -> None:
    result: subprocess.CompletedProcess[str] = run_cli(
        "python", "-c", "import ml_playground; print('ml_playground import OK')"
    )
    assert result.returncode == 0, result.stderr


def test_tools_entrypoint_help_works(run_cli: RunCli) -> None:
    result: subprocess.CompletedProcess[str] = run_cli("tools", "--help")
    assert result.returncode == 0, result.stderr
    output = (result.stdout or "") + (result.stderr or "")
    assert "Usage:" in output
