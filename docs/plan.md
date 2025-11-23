# Forward-Looking Development Plan

This plan outlines the next phases of development for `ml_playground`, focusing on strict typing, comprehensive testing, and architectural refinement.

## Global Objectives
- **Strict Typing**: Eliminate `Any` and loose types; use Protocols and Pydantic models.
- **Mock-Free Testing**: Use deterministic fakes/stubs; target 100% branch coverage.
- **Defensive Simplification**: Narrow exception handlers and remove "just in case" code.

## Phase 1: Future Test-Suite Typing Workstreams (Current Focus)

**Objective**: Extend strict typing and mock-free testing to all test kinds (property, unit) for runtime and tools.

1. - [x] **Property Tests Coverage Analysis**
   - Re-run coverage focused on production files exercised by property suites.
   - Record per-file branch coverage to identify gaps.
   - **Gaps Identified**:
     - `tools/utils/subprocess_utils.py`: 80.00%
     - `training/loop/runner.py`: 84.09%
     - `tools/testing/coverage.py`: 83.08%
     - `tools/testing/unit.py`: 83.33%
     - `tools/dev/review.py`: 79.79%

2. - [ ] **Property Test Expansion**
   - `tests/property/runtime`: Expand strategies for complex objects (e.g., model configs).
   - `tests/property/tools`: Cover more CLI states and edge cases (e.g., malformed configs).

## Phase 2: Defensive Code & Type Safety Refinement

**Objective**: Clean up defensive patterns identified in the codebase.

1. - [ ] **Narrow `except Exception` blocks**
   - **Scope**: ~33 instances identified (e.g., `tools/cli/commands/quality.py`, `tools/dev/review.py`).
   - **Action**: Replace broad handlers with specific exceptions (e.g., `FileNotFoundError`, `ValidationError`).
   - **Goal**: Fail fast and loudly for unexpected errors.

2. - [ ] **Refactor Broad `getattr` Usage**
   - **Scope**: ~82 instances identified (e.g., `sampling/runner.py`, `core/file_state.py`).
   - **Action**: Replace dynamic attribute access with defined Protocols or dataclasses.

3. - [ ] **Address TODOs**
   - **Scope**: ~7 relevant TODOs in `src` (excluding generated JSONs).
   - **Action**: Resolve or ticket actionable technical debt.

## Phase 3: Integration & E2E Flows

**Objective**: Validate end-to-end workflows with the hardened runtime/tools.

1. - [ ] **Re-enable and Refactor Integration Tests**
   - Ensure `tests/integration` follows the "no mock" policy.
   - Use ephemeral resources (tmp_path, fake runners).

2. - [ ] **E2E CLI Tests**
   - Verify `uv run ml-playground` flows (prepare -> train -> sample).
   - Use strictly typed configuration overrides for E2E scenarios.

## Known Patterns (For Review)
- **Git Worktree Detection**: Used in `tools/environment` (2 matches). Deemed appropriate for context but monitor for complexity.
