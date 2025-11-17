"""Runtime CLI package initialization.

This __init__.py provides minimal re-exports required for:
1. Dynamic attribute resolution via _resolve_cli_attr in main.py
2. Test imports from ml_playground.runtime.cli

Exception: Re-exports are required for test compatibility per IMPORT_GUIDELINES.md.
"""

from __future__ import annotations

# Initialize bootstrap system with default dependencies
from ..core.bootstrap import configure_runtime_cli_dependencies
from .main import default_cli_dependencies

# Import from main module to provide test-accessible attributes
from .main import (
    CLIDependencies,
    app,
    global_device_setup,
    log_command_status,
    log_directory,
    override_cli_dependencies,
    run_prepare,
    run_sample,
    run_train,
)

configure_runtime_cli_dependencies(default_cli_dependencies)

__all__ = [
    "CLIDependencies",
    "app",
    "global_device_setup",
    "log_command_status",
    "log_directory",
    "override_cli_dependencies",
    "run_prepare",
    "run_sample",
    "run_train",
]
