# Tools Package

<details>
<summary>Related documentation</summary>

- [.dev-guidelines/DOCUMENTATION.md](../../../.dev-guidelines/DOCUMENTATION.md) – Folder-level README blueprint and abstraction policy.
- [.dev-guidelines/DEVELOPMENT.md](../../../.dev-guidelines/DEVELOPMENT.md) – Tooling workflow, quality gates, and CLI conventions.

</details>

## Purpose

Developer tooling that powers the `uv run tools ...` command surface. Provides Typer-based CLIs for environment setup, quality gates, CI workflows, review automation, and agentic helpers.

## Structure

```bash
src/ml_playground/tools/
├── README.md          # package overview (this file)
├── cli.py             # Typer entrypoint dispatching to categories
├── __init__.py        # package exports and CLI glue
├── categories/        # user-facing command groups (env, quality, test, ci, dev, agentic)
├── core/              # shared services (state mgmt, dependency injection, logging)
└── utils/             # helpers (subprocess, filesystem, serialization)
```

## Key Concepts

- **Command categories** (`src/ml_playground/tools/categories/`) expose focused Typer apps:
  - `env`: provisioning, verification, cache cleanup.
  - `quality`: lint/format bundles and targeted checks.
  - `test`: pytest orchestrations and coverage tasks.
  - `ci`: full quality gate execution (delegates to pre-commit).
  - `dev`: review automation, repo hygiene, port management.
  - `agentic`: AI-assisted batch operations and review helpers.
- **Core services** provide shared logging, configuration loading, and dependency wiring so categories stay thin.
- **Utilities** wrap subprocess execution, environment management, and path handling with consistent error reporting.

## Usage

```bash
# discover available tool groups
uv run tools --help

# run the quality gate bundle (delegates to pre-commit)
uv run tools ci quality-gate

# verify local environment artifacts
uv run tools env verify

# list review comments needing replies
uv run tools dev review-list <pr_number> --unreplied --unresolved
```

## Implementation Notes

- Commands should depend on the shared services provided in `core/` to keep CLI handlers declarative.
- New categories must be registered inside `cli.py` and documented within their subdirectory README if additional context is required.
- Follow the centralized tokenizer and configuration protocols when tool commands reach into `ml_playground` modules (see `../../../docs/framework_utilities.md`).
