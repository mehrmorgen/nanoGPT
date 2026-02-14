# ml_playground Package

<details>
<summary>Related documentation</summary>

- [Documentation Guidelines](../../.dev-guidelines/project-specific/DOCUMENTATION.md) – Unified standards for documentation structure, abstraction levels, and formatting.
- [Architecture](../../.dev-guidelines/project-specific/ARCHITECTURE.md) – System overview and runtime entrypoints for the ml_playground stack.

</details>

## Purpose

Top-level package root for the framework, tooling, and experiments. This directory defines the separation between
runtime framework code, developer tooling, and experiment implementations.

## Dependency Rules

- `framework/` may import only framework modules, stdlib, and third-party packages.
- `tools/` may import `framework/`, but never `experiments/`.
- `experiments/` may import `framework/`, but never `tools/` or framework CLI entrypoints.

## Structure

```bash
src/ml_playground/
├── README.md     # package overview and dependency rules (this file)
├── framework/    # runtime framework code (no tools/experiments deps)
├── tools/        # developer tooling (Typer-based CLI)
└── experiments/  # experiment implementations (framework consumers)
```

## Entry Points

- Framework CLI: `ml_playground.runtime_cli.main:main_entry` (see `framework/README.md`).
- Tools CLI: `ml_playground.tools.cli.main:main_entry` (see `tools/README.md`).
