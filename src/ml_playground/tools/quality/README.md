# Quality Tools

<details>
<summary>Related documentation</summary>

- [Documentation Guidelines](../../../../.dev-guidelines/DOCUMENTATION.md) – Folder-level README blueprint and abstraction policy.
- [Development Practices](../../../../.dev-guidelines/DEVELOPMENT.md) – Quality standards.

</details>

## Purpose

Tools for code quality enforcement, including linting, formatting, type checking, and dead code detection.

## Usage

```bash
# Run lint checks (ruff)
uv run tools quality lint

# Run formatting (ruff format)
uv run tools quality format

# Run type checking (mypy, basedpyright)
uv run tools quality typecheck

# Check for dead code (vulture)
uv run tools quality deadcode
```

## Structure

```bash
src/ml_playground/tools/quality/
├── README.md        # package documentation (this file)
├── deadcode.py      # vulture integration
├── formatting.py    # ruff format integration
├── linting.py       # ruff lint integration
├── quality.py       # main QualityTools class
└── typing.py        # mypy/basedpyright integration
```
