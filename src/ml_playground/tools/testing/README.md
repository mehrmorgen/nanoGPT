# Testing Tools

<details>
<summary>Related documentation</summary>

- [Documentation Guidelines](../../../../.dev-guidelines/project-specific/DOCUMENTATION.md) – Folder-level README blueprint and abstraction policy.
- [Development Practices](../../../../.dev-guidelines/project-specific/DEVELOPMENT.md) – Testing workflow and gates.

</details>

## Purpose

Tools for running test suites and coverage analysis.

## Usage

```bash
# Run all unit tests
uv run tools test unit

# Run coverage check
uv run tools test coverage

# Run mutation testing
uv run tools test mutation
```

## Structure

```bash
src/ml_playground/tools/testing/
├── README.md            # package documentation (this file)
├── coverage.py          # coverage analysis
├── coverage_helpers.py  # coverage utilities
├── e2e.py               # E2E test runner
├── integration.py       # integration test runner
├── mutation.py          # cosmic-ray mutation testing
├── property.py          # property-based test runner
├── testing.py           # main TestingTools class
└── unit.py              # unit test runner
```
