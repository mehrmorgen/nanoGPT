# Runtime Package

<details>
<summary>Related documentation</summary>

- [.dev-guidelines/project-specific/DOCUMENTATION.md](../../../.dev-guidelines/project-specific/DOCUMENTATION.md) – Package README structure, abstraction levels, and folder tree rules.
- [.dev-guidelines/project-specific/DEVELOPMENT.md](../../../.dev-guidelines/project-specific/DEVELOPMENT.md) – CLI architecture standards and runtime practices.
- [docs/framework_utilities.md](../../../docs/framework_utilities.md) – Shared helpers (configuration, tokenizer, error handling) leveraged by runtime flows.

</details>

## Purpose

Runtime orchestration helpers used by the framework CLI. Provides device setup, runner wiring, and metadata utilities that connect configuration models to training and sampling modules.

## Structure

```bash
src/ml_playground/framework/runtime/
├── README.md      # package overview (this file)
├── __init__.py    # runtime exports
├── core/          # dependency bundles and bootstrap helpers
├── device.py      # device setup and dtype guards
├── helpers.py     # log/status helpers for runtime flows
├── protocols.py   # lightweight runtime protocols
└── runners.py     # prepare/train/sample orchestration
```

## Responsibilities

- Assemble dependency bundles (tokenizers, checkpoint managers, loggers) in a single place to keep CLI handlers thin.
- Provide metadata helpers (`runtime.helpers`) for CLI flows:
  - `run_or_exit` and `handle_tool_result` to normalize process exit codes and ToolResult reporting.
  - `extract_exp_config` and `complete_experiments` to work with Typer contexts and experiment naming.
  - `log_directory` and `log_command_status` to emit consistent, testable log lines.
- Maintain runner invariants:
  - `runtime.runners` wraps `run_prepare/train/sample` implementations and always calls `handle_tool_result`.
  - Dependency container (`runtime.core.bootstrap.CLIDependencies`) can be overridden/reset for tests.

## Usage Example

```python
from ml_playground.framework.runtime.runners import run_train_impl

result = run_train_impl("demo", train_cfg, config_path, metadata)
```

## Notes

- Keep runtime helpers focused on orchestration; reusable primitives should live in `core/`, `training/`, or `sampling/` packages.
- When adding new CLI options, update `framework/cli` plus the runtime runners to maintain parity.
- Reference the centralized tokenizer protocol and configuration loaders documented in `docs/framework_utilities.md` when integrating new runtime features.
