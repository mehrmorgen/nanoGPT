---
trigger: manual
description: Troubleshooting guide for common ml_playground development workflow issues
---

# Troubleshooting

Use this guide to diagnose common environment and workflow issues. Keep fixes minimal and document recurring problems here.

## Table of Contents

- [Environment setup failures](#environment-setup-failures)
- [Configuration errors](#configuration-errors)
- [Import and dependency issues](#import-and-dependency-issues)
- [CLI failures](#cli-failures)
- [Testing problems](#testing-problems)

## Environment setup failures

- Run `uv run tools env verify` to validate the UV environment.
- If dependencies drift, re-sync with `uv sync --frozen --group dev` and retry.
- Avoid manual virtualenv activation or `pip install`; UV is the single source of truth.

## Configuration errors

- Ensure experiment TOML paths are correct and readable.
- Use `load_full_experiment_config()` helpers rather than re-implementing merge logic.
- Verify paths are stored as `Path` values before validation.

## Import and dependency issues

- Confirm imports follow [`IMPORT_GUIDELINES.md`](IMPORT_GUIDELINES.md).
- Avoid circular dependencies by keeping framework free of tools/experiments imports.
- If a module cannot be imported, check for stale `.pyc` or missing `__init__.py`.

## CLI failures

- Prefer Typer entry points (`uv run cli ...`) and keep CLI logic thin.
- Validate required artifacts (for example `meta.pkl`) before running train/sample.
- Use `--help` on each command to confirm arguments and defaults.

## Testing problems

- Follow TDD expectations in [`TESTING.md`](TESTING.md).
- Ensure deterministic fixtures and stable seeds for randomized tests.
- Run focused tests first (unit/integration) before the full quality gate.
