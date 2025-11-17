# Runtime Package

<details>
<summary>Related documentation</summary>

- [.dev-guidelines/DOCUMENTATION.md](../../../.dev-guidelines/DOCUMENTATION.md) – Package README structure, abstraction levels, and folder tree rules.
- [.dev-guidelines/DEVELOPMENT.md](../../../.dev-guidelines/DEVELOPMENT.md) – CLI architecture standards and runtime practices.
- [docs/framework_utilities.md](../../../docs/framework_utilities.md) – Shared helpers (configuration, tokenizer, error handling) leveraged by runtime flows.

</details>

## Purpose

End-user runtime orchestration for the `ml_playground` CLI. Provides command wiring, dependency injection, and runtime utilities that connect configuration models to training and sampling modules.

## Structure

```bash
src/ml_playground/runtime/
├── README.md      # package overview (this file)
├── __init__.py    # runtime exports
├── cli.py         # Typer command wiring and dependency injection entrypoints
└── core/          # shared runtime helpers (dependency bundles, logging hookups)
```

## Responsibilities

- Parse runtime options and hydrate typed configuration objects before invoking training or sampling flows.
- Assemble dependency bundles (tokenizers, checkpoint managers, loggers) in a single place to keep CLI handlers thin.
- Expose hooks that the Typer commands in `src/ml_playground/tools/` and `src/ml_playground/cli.py` can call without duplicating wiring logic.

## Usage Example

```python
from ml_playground.runtime.cli.main import build_runtime_context

runtime_ctx = build_runtime_context(exp_config_path)
trainer = runtime_ctx.create_trainer()
trainer.run()
```

## Notes

- Keep runtime helpers focused on orchestration; reusable primitives should live in `core/`, `training/`, or `sampling/` packages.
- When adding new CLI options, update both the Typer command definition and the runtime context factories to maintain parity.
- Reference the centralized tokenizer protocol and configuration loaders documented in `docs/framework_utilities.md` when integrating new runtime features.
