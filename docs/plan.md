# Coverage & Runtime Refactor Plan

Our current objective is to raise **branch coverage for everything under `src/ml_playground/runtime` and the tools package `src/ml_playground/tools` to ≥85%** (overall gate ≥90% line / ≥83% branch), while cleaning up the runtime and tools APIs so tests can use strictly-typed fakes without mocks or test-only branches in production.

Only forward-looking work is tracked here.

## Global Guardrails
- **Testing approach**: Property-based tests (Hypothesis) first for every runtime target; no mocking frameworks.
- **Fakes**: Use deterministic fakes and stubs that satisfy public interfaces or protocols; no production code that exists solely for tests.
- **Verification cadence**: For each tranche run (1) new/updated property suites, (2) targeted unit suites, (3) `uv run ci-tasks coverage-report` and record deltas.

## Status at a glance
- [x] Property suites: `uv run pytest tests/property/runtime -v` and `tests/property/tools -v` both pass.
- [x] Unit suites: `uv run pytest tests/unit/runtime -v` and `tests/unit/tools -v` pass.
- [x] Coverage gate: latest shared run (`uv run tools test coverage -- tests/property/runtime tests/unit/runtime`, 2025-11-21 09:15 UTC+01) yielded **92.48% line / 83.22% branch**. Gate is green (≥83.0% branch).
- [ ] Integration / E2E flows: deferred until coverage gate is green again.

## Current Runtime Focus

### 1. Device
- File: `src/ml_playground/runtime/device.py`.
- Status: Property tests in `tests/property/runtime/test_device_property.py` use a strictly-typed fake torch implementation to cover `global_device_setup` branches (cpu / cuda / mps, dtype and seed handling).
- Next: Ensure runtime CLI device helpers consume the same abstraction once CLI workstream starts.

## Current Tools Focus: Tools Layer & Tools Protocols

These tranches extend the same protocol-driven, strictly-typed testing approach to the `src/ml_playground/tools` package and its CLIs.

### 2. Tools Runtime State & Protocols
- Files: `src/ml_playground/tools/cli/config_loader.py`, `src/ml_playground/tools/cli/state.py`, `src/ml_playground/tools/protocols.py`.
- Status:
  - `tools/core/runtime.py` has been removed as dead code.
  - `tools/cli/config_loader.py` is fully tested including error handling branches.
  - Global state still stores concrete `ToolsConfig`; protocol extraction is the next structural step.
- Plan (step-by-step):
  1. - [ ] **Define the shared tools config protocol**
     - Implement `ToolsConfigLike` in `tools/protocols.py` exposing the fields the CLI actually consumes (`learning_mode_default`, `default_verbosity`, verbosity helpers).
  2. - [ ] **Update tools global state**
     - Switch `GlobalState.config` to `ToolsConfigLike | None` and ensure `state.reset()` clears it deterministically.
  3. - [ ] **Wire the protocol into config loader**
     - Update `tools/cli/config_loader.py` to store `ToolsConfigLike`, reading defaults via local variables before mutating global state.
  4. - [ ] **Keep concrete configs where required**
     - Tool implementations (`quality`, `testing`, `environment`, `ci`, `dev`) and dependency builders continue to accept full `ToolsConfig`.
  5. - [ ] **Verification**
     - `uv run pytest tests/property/tools/cli -v` and `tests/property/tools -v` must stay green after protocol extraction.

### 3. Tools Categories & Deterministic Runners
- Files: `src/ml_playground/tools/quality/quality.py`, `src/ml_playground/tools/testing/testing.py`, `src/ml_playground/tools/environment/environment.py`, `src/ml_playground/tools/ci/ci.py`, `src/ml_playground/tools/dev/dev.py`, plus helpers under `src/ml_playground/tools/**`.
- Status:
  - Property suites already run against deterministic dependency overrides; no mocking.
  - **Update**: `tools/testing/testing.py`, `tools/testing/coverage.py`, and `tools/testing/mutation.py` are now Green (≥83% branch).
  - Remaining laggards:
    1. `tools/environment/setup.py` / `tools/environment/verify.py` (≈73–67% branch)
  - Next steps:
  1. - [ ] **Environment/CI/Dev runners**
     - For `tools/environment/setup.py`, `tools/environment/verify.py`, and `tools/dev/*.py`, create deterministic runner tests that simulate subprocess failures, missing pyproject files, and optional dependency warnings.
  2. - [ ] **Verification and coverage**
     - Run `uv run pytest tests/property/tools -v` / `tests/unit/tools -v` plus `uv run tools test coverage -- tests/property/tools tests/unit/tools`, tracking per-file branch % until each target crosses 85%.

## Tracking & Success Criteria
- Maintain `docs/plan.md` with per-file branch % after each significant change.
- Do not close a workstream until **every targeted runtime or tools file** hits ≥85% branch coverage and tests are type-clean under the strict tooling.
- When the active runtime/tools workstreams meet the threshold and the global gate is green, update this plan with the next scope (e.g. analysis stack, training hooks).
- Within each runtime/tools scope, **always pick the next targets by ascending branch coverage**. This keeps feedback tight and makes it obvious which files to harden next.

### Pragma removal & strict coverage

All previously identified pragmas in `src/ml_playground/**` now have deterministic tests in place. Only branches that are fundamentally untestable (version-dependent code, IO fallbacks) remain guarded. We now focus exclusively on raising branch coverage rather than locating residual pragmas.

### Runtime/tools coverage snapshot (latest run)
- Runtime highlights: `runtime/cli/main.py` (branch 95.59%) and `runtime/core/bootstrap.py` (100%) are now healthy after the CLI/ bootstrap test additions.
- Tools highlights:
  - `tools/core/runtime.py` — Removed (dead code)
  - `tools/cli/main.py` — 95.59%
  - `tools/cli/config_loader.py` — 70.00%
  - `tools/cli/commands/ci.py` — line 93.62% (branch n/a)
  - `tools/cli/commands/dev.py` — line 75.68% (branch n/a)
  - `tools/cli/commands/environment.py` — line 79.17% (branch n/a)
  - `tools/cli/commands/quality.py` — line 84.52% (branch n/a)
  - `tools/testing/coverage.py` — 83.08%
  - `tools/testing/mutation.py` — 89.13%
  - `tools/testing/testing.py` — 91.67%
  - `tools/testing/unit.py` — 83.33%
  - `tools/utils/subprocess_utils.py` — 70.00%
  - `tools/environment/environment.py` — 100.00%
  - `tools/environment/setup.py` — 73.08%
  - `tools/environment/verify.py` — 66.67%
  - `tools/environment/clean.py` — 76.47%
  - `tools/ci/ci.py` — 81.25%
  - `tools/dev/batch_review.py` — 65.79%
  - `tools/dev/ai_guidelines.py` — 72.00%
  - `tools/dev/hygiene.py` — 96.15%
  - `tools/dev/review.py` — 69.15%
  - `tools/dev/workflow_status.py` — 56.82%

**Next concrete targets (lowest branch coverage first, within runtime/tools):**
- `tools/dev/workflow_status.py` — 56.82%
- `tools/dev/batch_review.py` — 65.79%
- `tools/environment/verify.py` — 66.67%
- `tools/dev/review.py` — 69.15%
- `tools/cli/config_loader.py` — 70.00%
- `tools/utils/subprocess_utils.py` — 70.00%
- `tools/environment/setup.py` — 73.08%

### Defensive branch simplification targets (runtime/tools)

We also track **defensive branches** (especially broad `except Exception` handlers and silent fallbacks) we want to either remove or narrow now that coverage is high and tests are explicit. The goal is to keep behavior predictable and observable while avoiding unnecessary safety nets.

- **runtime/runners.py**
  - Branches:
    - Inner `try/except Exception: pass` wrappers around `active_hooks.log_status` in `run_train_impl` and `run_sample_impl` (`pre-*` / `post-*` hooks).
    - Outer `try/except Exception as e` around `run_prepare_impl`, `run_train_impl`, `run_sample_impl`, and `run_analyze` that convert unexpected failures into `ToolResult`.
  - Plan:
    - [ ] **Batch 1 – hooks only**: Remove the inner `try/except` around `log_status` so hook failures become visible (and are still wrapped by the outer handler), then update property tests in `tests/property/runtime/test_runners_simple_property.py` / `test_runners_property.py` to assert failure semantics.
    - [ ] **Later – review outer handlers**: Revisit whether the outer generic `except Exception` blocks should be narrowed to domain-specific errors once E2E/CLI behavior expectations are fully documented.

- **tools/dev/workflow_status.py**
  - Branches:
    - Generic `except Exception` in `_get_git_status`, `_get_quality_status`, `_get_test_status`, `_get_coverage_status`, returning `{ "status": "unknown", ... }`.
    - Per-check `except Exception as e` in `_run_quality_batch` and `_run_test_batch_simple`, which downgrade failures to `"status": "error"` instead of crashing.
  - Plan:
    - [ ] **Keep contract, tighten scope**: Maintain the top-level contract that `run_workflow_status` always returns a `ToolResult(success=True)` but incrementally narrow the caught exception types (e.g. `ToolExecutionError`) where feasible.
    - [ ] Extend `tests/unit/tools/dev/test_workflow_status.py` to assert behavior when underlying tools return structured failures vs. when they raise typed exceptions.

- **tools/dev/batch_review.py**
  - Branches:
    - Generic `except Exception as e` in `_run_quality_batch` and `_run_test_batch` for lint/typecheck/deadcode/unit/integration/coverage, mapping unexpected failures to `"status": "error"`.
  - Plan:
    - [ ] Preserve the batch-review summary contract but consider narrowing the caught exceptions to tools-layer error types.
    - [ ] Ensure `tests/unit/tools/dev/test_batch_review.py` covers both normal failure (`success=False`) and `"status": "error"` downgrade behavior so we can safely adjust the handlers.

- **tools/dev/hygiene.py**
  - Branches:
    - Generic `except Exception` in `run_cleanup_ignored_tracked` and `run_kill_port`, turning subprocess/psutil issues into failing `ToolResult`s.
    - Nested `try/except Exception` fallbacks in `_pids_by_port` to cope with restricted `psutil.net_connections` or `process_iter` behavior across platforms.
  - Plan:
    - [ ] Keep the platform-compatibility fallbacks in `_pids_by_port`, but add tests in `tests/unit/tools/dev/test_hygiene.py` that cover both the primary and fallback paths.
    - [ ] Revisit the outer generic handlers only if we can introduce more precise error types without degrading cross-platform robustness.

- **tools/testing/coverage.py**
  - Branches:
    - Defensive `ToolExecutionError` construction in `_ensure_coverage_data` when automatic coverage generation/combination fails, with a generic "Unknown error during coverage generation/combination" reason when stderr is empty.
    - Broad `except Exception` in `run_coverage_report` when coverage report generation fails for any command.
  - Plan:
    - [ ] Now that coverage generation is stable and fully tested, incrementally tighten the error messages by threading through concrete stderr/stdout where available, avoiding truly opaque "Unknown" reasons.
    - [ ] Consider narrowing the `except Exception` in `run_coverage_report` to the subprocess layer once we have stronger invariants for the runner.

- **tools/testing/mutation.py**
  - Branches:
    - Defensive `except Exception` blocks around mutation-reset/session unlinking and Cosmic Ray invocation, now covered by unit tests.
  - Plan:
    - [ ] Keep the current behavior (convert unexpected runner/filesystem failures into `ToolExecutionError` or failing `ToolResult`) but avoid adding new generic fallbacks.
    - [ ] Use the existing tests in `tests/unit/tools/testing/test_mutation.py` as a safety net while we gradually tighten exception types where possible.

- **tools/testing/testing.py**
  - Branches:
    - Defensive coverage-data generation and fallback paths (`_ensure_coverage_data`, `_run_coverage_test_for_data`, `_generate_coverage_via_pytest`) that were originally mirrored in the facade and are now centralized in `tools/testing/coverage.py`.
  - Plan:
    - [ ] Prefer the consolidated coverage helpers in `tools/testing/coverage.py` for new behavior; avoid reintroducing ad-hoc fallbacks in the facade.
    - [ ] Ensure property tests in `tests/property/tools/testing/test_testing_tools_property.py` keep the public API stable while we avoid expanding generic error handlers.

## Future Test-Suite Typing Workstreams

Once the runtime and tools production surfaces are protocol-aligned and the corresponding property tests are green, the next phase is to extend strict typing and mock-free testing to **all test kinds** that exercise this scope.

1. **Property tests (runtime + tools)**
   - Steps:
     1. - [ ] Re-run coverage focused on the production files exercised by these property suites and record per-file branch % in this plan.
