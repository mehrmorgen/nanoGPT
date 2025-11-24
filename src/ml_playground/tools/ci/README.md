# CI Tools

<details>
<summary>Related documentation</summary>

- [Documentation Guidelines](../../../../.dev-guidelines/DOCUMENTATION.md) – Folder-level README blueprint and abstraction policy.
- [Development Practices](../../../../.dev-guidelines/DEVELOPMENT.md) – Core development practices and quality standards.

</details>

## Purpose

Tools for Continuous Integration workflows, including quality gates and coverage reporting.

## Usage

```bash
# Run full quality gate (delegates to pre-commit)
uv run tools ci quality-gate

# Generate coverage badge (requires coverage data)
uv run tools ci coverage-badge
```

## Structure

```bash
src/ml_playground/tools/ci/
├── README.md       # package documentation (this file)
└── ci.py           # CI tool implementations (quality-gate, coverage-badge)
```
