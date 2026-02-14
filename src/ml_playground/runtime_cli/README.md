# Runtime CLI package

This package now *is* the runtime CLI for ML Playground. All CLI logic lives under
`ml_playground.runtime_cli`, and there are no re-export or shim modules.

Core components:

- `main.py`: Typer app wiring and entry points.
- `commands.py`: command implementations and status logging helpers.
- `device.py`: global device/seed setup.
- `typer_helpers.py`: small Typer/CLI utilities.
- `runners.py`: dependency injection wiring and thin command runners.

Supporting runtime helpers:

- `ml_playground.framework.runtime.core.bootstrap`: CLI dependency container.
- `ml_playground.framework.runtime.core.results`: `LearningModeEngine`/`ToolResult` helpers.
- `ml_playground.framework.runtime.helpers`: metadata helpers such as `log_command_status`.
- `ml_playground.framework.runtime.protocols`: metadata protocol types consumed by CLI tests.

Entry points:

- Runtime CLI entry points in `pyproject.toml` target `ml_playground.runtime_cli.main:main_entry`.
