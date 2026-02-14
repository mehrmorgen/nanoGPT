# Analysis Package

<details>
<summary>Related documentation</summary>

- [.dev-guidelines/project-specific/DOCUMENTATION.md](../../../.dev-guidelines/project-specific/DOCUMENTATION.md) – README structure and formatting rules.
- [docs/framework_utilities.md](../../../docs/framework_utilities.md) – Shared utilities and refactor guidance.

</details>

## Purpose

Provide lightweight analysis utilities that live in the framework layer (sample quality checks, metrics registry, and optional LIT integration).

## Structure

```bash
src/ml_playground/framework/analysis/
├── README.md
├── __init__.py
├── metrics_registry.py
├── sample_quality.py
├── sample_quality_public.py
└── lit/  # optional LIT integration helpers
```

## Responsibilities

- Keep analysis logic framework-only (no tools or experiments imports).
- Expose stable public helpers from `sample_quality_public`.
- Host optional integrations (LIT) behind clear import boundaries.
