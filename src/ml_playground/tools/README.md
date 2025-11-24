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
├── cli/               # Typer entrypoint and CLI infrastructure
├── ci/                # CI tools (quality gate, coverage badge)
├── dev/               # developer tools (review, status, ai-setup)
├── environment/       # environment management tools
├── quality/           # quality enforcement tools (lint, format)
├── testing/           # testing tools (pytest, coverage)
├── core/              # shared services and interfaces
└── utils/             # low-level utilities
```

## Key Concepts

- **Tool Categories**: Organized as subpackages (`ci`, `dev`, `environment`, `quality`, `testing`).
- **CLI Infrastructure**: The `cli` subpackage handles command registration and dependency injection.
- **Core Services**: Shared logic lives in `core` to keep commands thin.

## Usage

```bash
# discover available tool groups
uv run tools --help

# run the quality gate bundle
uv run tools ci quality-gate

# list review comments
uv run tools dev review-list <pr_number>
```
