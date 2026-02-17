"""Mypy runtime preflight guards."""

from __future__ import annotations

import os
from pathlib import Path

from ml_playground.tools.core.errors import ToolExecutionError


def _workspace_uv_run_count(root_path: Path) -> int:
    """Count other uv run processes in the same workspace."""
    try:
        import psutil
    except Exception:
        return 0

    current_process = psutil.Process(os.getpid())
    ancestor_pids = {proc.pid for proc in current_process.parents()}
    ancestor_pids.add(current_process.pid)
    root_resolved = root_path.resolve()

    count = 0
    for process in psutil.process_iter(["pid", "cmdline"]):
        pid = process.info.get("pid")
        if not isinstance(pid, int) or pid in ancestor_pids:
            continue
        cmdline = process.info.get("cmdline")
        if not isinstance(cmdline, list) or len(cmdline) < 2:
            continue
        executable = str(cmdline[0]).lower()
        if not executable.endswith("uv") and "/uv" not in executable:
            continue
        if str(cmdline[1]).lower() != "run":
            continue
        try:
            process_cwd = Path(process.cwd()).resolve()
        except (OSError, PermissionError):
            continue
        if process_cwd == root_resolved:
            count += 1
    return count


def _iter_probe_files(root_path: Path) -> list[Path]:
    candidates: list[Path] = []
    for pattern in (
        ".venv/lib/python*/site-packages/psutil-stubs/py.typed",
        ".venv/lib/python*/site-packages/mypy/typeshed/stdlib/builtins.pyi",
    ):
        candidates.extend(root_path.glob(pattern))
    return candidates


def ensure_mypy_runtime_ready(root_path: Path) -> None:
    """Fail fast when local filesystem state can deadlock mypy reads."""
    competing_uv = _workspace_uv_run_count(root_path)
    if competing_uv > 0:
        raise ToolExecutionError(
            "Mypy preflight failed: concurrent uv run processes detected",
            reason=(
                f"Found {competing_uv} other 'uv run' processes in this workspace."
            ),
            rationale=(
                "Run mypy with no concurrent uv commands to avoid filesystem races "
                "that can trigger stalled reads and Errno 89."
            ),
        )

    for probe_path in _iter_probe_files(root_path):
        try:
            if probe_path.exists() and not probe_path.is_file():
                raise ToolExecutionError(
                    "Mypy preflight failed: invalid typing stub path",
                    reason=f"Expected a file but found non-file path: '{probe_path}'.",
                    rationale=(
                        "Typing stub metadata in .venv must remain regular files for "
                        "mypy module discovery to work reliably."
                    ),
                )
        except OSError as exc:
            raise ToolExecutionError(
                "Mypy preflight failed: unable to access typing stub metadata",
                reason=(
                    f"Failed to access '{probe_path}' "
                    f"(errno={exc.errno}, error={exc.strerror or str(exc)})."
                ),
                rationale=(
                    "This indicates local filesystem instability for .venv metadata. "
                    "Ensure no concurrent uv processes are running and rerun."
                ),
            ) from exc
