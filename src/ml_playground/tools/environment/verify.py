"""Environment verification functionality for environment tools."""

from __future__ import annotations

from pathlib import Path
from typing import List

from ..core.config import ToolsConfig
from ..core.interfaces import OperationId, ToolResult
from ..utils.subprocess_utils import SubprocessRunner


def run_verify(
    config: ToolsConfig,
    root_path: Path,
    pkg_name: str,
    args: List[str],
    subprocess_runner: SubprocessRunner,
) -> ToolResult:
    """Ensure the project package imports correctly.

    Args:
        config: Tool configuration
        root_path: Project root path
        pkg_name: Package name to test import
        args: Additional arguments (ignored)
        subprocess_runner: Subprocess runner

    Returns:
        ToolResult with execution details
    """
    operation_id = OperationId(namespace="tools", category="env", command="verify")

    # Test package import
    import_cmd = [
        "python",
        "-c",
        f"import {pkg_name}; print('✓ {pkg_name} import OK')",
    ]

    return subprocess_runner.run_uv_command(
        import_cmd,
        cwd=root_path,
        timeout=config.environment.timeout,
        operation_id=operation_id,
    )


def run_info(
    config: ToolsConfig,
    root_path: Path,
    pkg_name: str,
    venv_path: Path,
    cache_dir: Path,
    args: List[str],
    subprocess_runner: SubprocessRunner,
) -> ToolResult:
    """Show environment information.

    Args:
        config: Tool configuration
        root_path: Project root path
        pkg_name: Package name for import check
        venv_path: Virtual environment path
        cache_dir: Cache directory path
        args: Additional arguments (ignored)
        subprocess_runner: Subprocess runner

    Returns:
        ToolResult with execution details
    """
    operation_id = OperationId(namespace="tools", category="env", command="info")

    info_lines: list[str] = []

    # Project information
    info_lines.append(f"Project root: {root_path}")
    info_lines.append(f"Package name: {pkg_name}")

    # Virtual environment
    if venv_path.exists():
        info_lines.append(f"Virtual environment: {venv_path} (exists)")
    else:
        info_lines.append(f"Virtual environment: {venv_path} (missing)")

    # Cache directory
    if cache_dir.exists():
        cache_size = sum(f.stat().st_size for f in cache_dir.rglob("*") if f.is_file())
        cache_size_mb = cache_size / (1024 * 1024)
        info_lines.append(f"Cache directory: {cache_dir} ({cache_size_mb:.1f} MB)")
    else:
        info_lines.append(f"Cache directory: {cache_dir} (missing)")

    # Check if package is importable
    try:
        import_result = subprocess_runner.run_uv_command(
            ["python", "-c", f"import {pkg_name}; print('OK')"],
            cwd=root_path,
            timeout=30,  # Short timeout for info check
            operation_id=operation_id,
        )
        if import_result.success:
            info_lines.append(f"Package import: ✓ {pkg_name} imports successfully")
        else:
            info_lines.append(f"Package import: ✗ {pkg_name} import failed")
    except Exception:
        info_lines.append(f"Package import: ✗ Could not test {pkg_name} import")

    return ToolResult(
        success=True,
        exit_code=0,
        stdout="\n".join(info_lines),
        stderr="",
        operation_id=operation_id,
    )
