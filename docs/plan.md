# Forward-Looking Development Plan

This plan outlines the next phases of development for `ml_playground`, focusing on strict typing, comprehensive testing, and architectural refinement.

## Global Objectives
- **Strict Typing**: Eliminate `Any` and loose types; use Protocols and Pydantic models.
- **Mock-Free Testing**: Use deterministic fakes/stubs; target 100% branch coverage.
- **Defensive Simplification**: Narrow exception handlers and remove "just in case" code.

## Pending Tasks

### Acceptance & E2E Tests
- [x] **Complete Acceptance Tests**
  - **Scope**: Tools and Runtime.
  - **Goal**: Ensure high-level feature requirements are met via acceptance tests.

- [~] **Verify `uv run ml-playground` flows**
  - **Status**: `tests/e2e/test_cli_flow.py` implemented but skipped due to runtime environment issues (`IndexError` in model forward pass on CPU?).
  - **Goal**: Enable and fix E2E tests to validate full prepare -> train -> sample cycle.

### Documentation
- [x] **Add Missing README.md Files**
  - **Scope**: Every tool folder in `src/ml_playground/tools/`.
  - **Requirement**: Create standard READMEs per `.dev-guidelines/DOCUMENTATION.md`.

- [x] **Update Existing README.md Files**
  - **Scope**: All existing READMEs (repo root, experiments, etc.).
  - **Requirement**: Align with `.dev-guidelines/DOCUMENTATION.md` (structure, folder trees, cross-referencing).
