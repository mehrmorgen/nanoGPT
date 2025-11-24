# Tools CLI

<details>
<summary>Related documentation</summary>

- [Documentation Guidelines](../../../../.dev-guidelines/DOCUMENTATION.md) – Folder-level README blueprint and abstraction policy.

</details>

## Purpose

Core CLI infrastructure for the `uv run tools` command, including entry point, dependency injection, and command registration.

## Structure

```bash
src/ml_playground/tools/cli/
├── README.md          # package documentation (this file)
├── main.py            # Typer application entry point
├── config_loader.py   # configuration loading logic
├── dependencies.py    # dependency injection setup
├── helpers.py         # shared CLI helpers
├── state.py           # global CLI state management
└── commands/          # command category implementations
```
