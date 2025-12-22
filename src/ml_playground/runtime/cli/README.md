# Runtime CLI package

This package contains the refactored runtime CLI for ML Playground. The former monolithic `ml_playground.runtime.cli` module has been split into smaller files for clarity and testability:

- `main.py`: Typer app wiring and entry points.
- `commands.py`: command implementations and status logging helpers.
- `device.py`: global device/seed setup.
- `typer_helpers.py`: small Typer/CLI utilities.
- `runners.py`: dependency injection wiring and thin command runners.

Supporting modules:

- `runtime/core/__init__.py`: exports DI helpers for the runtime CLI.
- `runtime/protocols.py`: shared protocol definitions used by CLI tests.

Temporary notes (Phase 1):

- Coverage and static analysis exclude the new runtime CLI while the refactor settles.
- CLI entry points in `pyproject.toml` now target `ml_playground.runtime.cli.main:main_entry`.
