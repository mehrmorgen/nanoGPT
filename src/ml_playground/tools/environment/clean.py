"""Environment cleanup functionality for environment tools."""

from __future__ import annotations

import shutil
from pathlib import Path
from typing import List

from ..core.config import ToolsConfig
from ..core.interfaces import OperationId, ToolResult
from ..utils.subprocess_utils import SubprocessRunner


def run_clean(
    config: ToolsConfig,
    root_path: Path,
    cache_dir: Path,
    args: List[str],
    subprocess_runner: SubprocessRunner,
) -> ToolResult:
    """Remove caches and temporary build artifacts.

    Args:
        config: Tool configuration
        root_path: Project root path
        cache_dir: Cache directory path
        args: Additional arguments (ignored)
        subprocess_runner: Subprocess runner

    Returns:
        ToolResult with execution details
    """
    operation_id = OperationId(namespace="tools", category="env", command="clean")

    # Check cache directory before cleanup
    before_entries = []
    if cache_dir.exists():
        before_entries = sorted(cache_dir.iterdir())

    # Define cleanup targets
    cache_targets = [
        cache_dir / "pytest",
        cache_dir / "coverage",
        cache_dir / "hypothesis",
        cache_dir / "pre-commit",
        cache_dir / "ruff",
        cache_dir / "uv",
        cache_dir / "mypy",
    ]

    build_targets = [
        root_path / "htmlcov",
        root_path / "build",
        root_path / "dist",
        root_path / "*.egg-info",
    ]

    # Clean cache directories
    cleaned_paths: list[str] = []
    for target in cache_targets + build_targets:
        if target.name.endswith("*.egg-info"):
            # Handle glob pattern for egg-info directories
            for egg_info in root_path.glob("*.egg-info"):
                if egg_info.exists():
                    shutil.rmtree(egg_info, ignore_errors=True)
                    cleaned_paths.append(str(egg_info.relative_to(root_path)))
        elif target.exists():
            if target.is_dir():
                shutil.rmtree(target, ignore_errors=True)
            else:
                target.unlink(missing_ok=True)
            cleaned_paths.append(str(target.relative_to(root_path)))

    # Clean __pycache__ directories
    pycache_count = 0
    for pycache in root_path.rglob("__pycache__"):
        if pycache.exists():
            shutil.rmtree(pycache, ignore_errors=True)
            pycache_count += 1

    # Prepare output
    output_lines: list[str] = []

    if before_entries:
        output_lines.append("Cache contents before cleanup:")
        for entry in before_entries:
            output_lines.append(f"  - {entry.relative_to(root_path)}")
    else:
        output_lines.append("Cache directory was empty or missing")

    if cleaned_paths:
        output_lines.append(f"\nCleaned {len(cleaned_paths)} cache/build paths:")
        for path in cleaned_paths:
            output_lines.append(f"  - {path}")

    if pycache_count > 0:
        output_lines.append(f"\nRemoved {pycache_count} __pycache__ directories")

    # Check cache directory after cleanup
    after_entries = []
    if cache_dir.exists():
        after_entries = sorted(cache_dir.iterdir())

    if after_entries:
        output_lines.append("\nCache contents after cleanup:")
        for entry in after_entries:
            output_lines.append(f"  - {entry.relative_to(root_path)}")
    else:
        output_lines.append("\nCache directory is now empty or removed")

    return ToolResult(
        success=True,
        exit_code=0,
        stdout="\n".join(output_lines),
        stderr="",
        operation_id=operation_id,
    )
