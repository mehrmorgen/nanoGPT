# CI Tools

CI/CD helpers for ml_playground, focused on quality gates and coverage reporting.

## Overview

The CI package provides a high-level interface to the project's quality standards. It leverages `pre-commit` as the authoritative source of truth for the quality gate, ensuring that development workflows match CI/CD pipelines.

## Key Components

### CITools (`ci.py`)

- **`quality_gate(args)`**: Runs the full pre-commit suite (linting, typing, unit/integration/acceptance/e2e tests, coverage thresholds).
- **`quality_fast(args)`**: Runs a subset of fast feedback hooks (ruff, ruff-format).
- **`quality_ci_local(args)`**: Executes the GitHub Actions quality workflow locally using `act`.
- **`coverage_badge(args)`**: Regenerates the SVG coverage badges based on the latest coverage artifacts.

## Usage

These tools are typically invoked via the `uv run tools ci` command family.

```bash
uv run tools ci quality-gate
uv run tools ci quality-fast
uv run tools ci coverage-badge
```

## Invariants

- **Pre-commit Authority**: The `quality-gate` command must match the `pre-commit` configuration in `.githooks/.pre-commit-config.yaml`.
- **Fail-Fast Verification**: `quality-gate` runs environment verification first, including required quality tools (`pre-commit`, `yamlfix`, `basedpyright`, `mypy`, `vulture`), and fails with remediation guidance when prerequisites are missing.
- **Structured Results**: All commands return `ToolResult` for consistent CLI reporting.
- **Dependency Injection**: Subprocess execution is handled via `SubprocessRunner` to allow testability without side effects.
