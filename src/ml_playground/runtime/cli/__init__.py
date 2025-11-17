"""Runtime CLI package initialization.

This __init__.py provides minimal re-exports required for:
1. Dynamic attribute resolution via _resolve_cli_attr in main.py
2. Test imports from ml_playground.runtime.cli

Exception: Re-exports are required for test compatibility per IMPORT_GUIDELINES.md.
TODO Remove re-exports: Migrate tests to direct imports once feasible.
"""

from __future__ import annotations

# Import from main module to provide test-accessible attributes
from .main import (
    CLIDependencies,
    app,
    global_device_setup,
    log_command_status,
    log_directory,
    override_cli_dependencies,
    run_prepare,
    run_prepare_impl,
    run_sample,
    run_sample_impl,
    run_train,
    run_train_impl,
)

__all__ = [
    "CLIDependencies",
    "app",
    "global_device_setup",
    "log_command_status",
    "log_directory",
    "override_cli_dependencies",
    "run_prepare",
    "run_prepare_impl",
    "run_sample",
    "run_sample_impl",
    "run_train",
    "run_train_impl",
]
