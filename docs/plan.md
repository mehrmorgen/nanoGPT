# Coverage & Runtime Refactor Plan

Our current objective is to raise **branch coverage for everything under `src/ml_playground/runtime` and the tools package `src/ml_playground/tools` to ≥85%** (overall gate ≥90% line / ≥83% branch), while cleaning up the runtime and tools APIs so tests can use strictly-typed fakes without mocks or test-only branches in production.

Only forward-looking work is tracked here.

## Global Guardrails
- **Testing approach**: Property-based tests (Hypothesis) first for every runtime target; no mocking frameworks.
- **Fakes**: Use deterministic fakes and stubs that satisfy public interfaces or protocols; no production code that exists solely for tests.
- **Verification cadence**: For each tranche run (1) new/updated property suites, (2) targeted unit suites, (3) `uv run ci-tasks coverage-report` and record deltas.

## Status at a glance
- [x] Property test workstream (runtime + tools) — strictly typed fakes, Hypothesis suites, and the `uv run pytest tests/property/runtime -v`, `uv run pytest tests/property/tools -v`, `uv run tools quality typecheck` commands all pass.
- [x] Unit test workstream (runtime + tools) — runtime/tools unit suites are green under `uv run pytest tests/unit/runtime -v` and `uv run pytest tests/unit/tools -v`, and `uv run tools quality typecheck` passes with no issues.
- [ ] Integration/regression/acceptance/E2E workstreams — pending until runtime/tools runtime/unit suites settle; will add checkboxes when each tranche starts.

## Current Runtime Focus

### 1. Device
- File: `src/ml_playground/runtime/device.py`.
- Status: Property tests in `tests/property/runtime/test_device_property.py` use a strictly-typed fake torch implementation to cover `global_device_setup` branches (cpu / cuda / mps, dtype and seed handling).
- Next: Ensure runtime CLI device helpers consume the same abstraction once CLI workstream starts.

### 2. Runners (Production-Side Refactor)
- File: `src/ml_playground/runtime/runners.py`.
- Status:
  - Property tests in `tests/property/runtime/test_runners_simple_property.py` exercise success and failure paths for `run_prepare_impl`, `run_train_impl`, `run_sample_impl`, and `run_analyze`, including seed-resolver hooks and learning-mode branches.
  - Coverage is improving but tests currently rely on stub config objects that are not aligned with the concrete `PreparerConfig`/`TrainerConfig`/`SamplerConfig` types.
- Plan:
  1. - [x] Introduce protocol-style config interfaces under `src/ml_playground/runtime/protocols.py`, modelling only what `runners.py` actually reads (runtime device/dtype/seed, logger, and minimal model/data/sampler/trainer attributes).
  2. - [x] Update `run_prepare_impl`, `run_train_impl`, and `run_sample_impl` in `runtime/runners.py` to depend on these protocols instead of the concrete configuration dataclasses, without changing control flow.
  3. - [x] Ensure existing production call sites that pass real configs continue to type-check and behave the same via structural compatibility.
  4. - [x] Update `tests/property/runtime/test_runners_simple_property.py` to rely on the new protocols for its stub config dataclasses so the tests become type-clean without broad `type: ignore` usage.
- Goal: Clean separation between runtime behavior and configuration representation, enabling strictly-typed fakes in tests and higher coverage for failure paths and learning-mode behavior.

### 3. Runtime CLI & Bootstrap (Next Tranche)
- Files: `src/ml_playground/runtime/core/bootstrap.py`, `src/ml_playground/runtime/helpers.py`, `src/ml_playground/runtime/cli/app.py`, `src/ml_playground/runtime/cli/main.py`, `src/ml_playground/runtime/cli/runners.py`, plus their existing tests under `tests/property/runtime/**` and `tests/unit/runtime/**`.
- Plan (step-by-step):
  1. - [x] **Inventory existing CLI tests**
     - Open `tests/property/runtime/cli/test_cli_property.py` and `tests/property/runtime/test_runtime_cli_property.py`.
     - List which commands / flags / error paths are already covered.
  2. - [x] **Add or extend property tests for missing paths**
     - For each uncovered CLI command or global option branch, add a new Hypothesis example in the CLI property tests.
     - Use Typer’s `CliRunner` helpers and the existing patterns in `tests/property/runtime/test_runners_simple_property.py` and `tests/unit/runtime/test_cli_runtime.py` for guidance.
  3. - [ ] **Align CLI runners with runtime protocols**
     - Update `runtime/cli/runners.py` so that helper functions take `PrepareConfigLike`, `TrainConfigLike`, and `SampleConfigLike` from `runtime/protocols.py` instead of concrete config types.
     - Ensure no behavior changes: keep logging, error handling, and learning-mode wiring identical.
  4. - [ ] **Exercise bootstrap wiring**
     - Add or extend property tests in `tests/property/runtime/core/test_bootstrap_property.py` (or create it if missing) to:
       - Construct a minimal fake dependency container.
       - Verify that `bootstrap` wires runtime runners, logging, and configuration consistently.
  5. - [ ] **Verification**
     - Run runtime tests: `uv run pytest tests/property/runtime -v` and `uv run pytest tests/unit/runtime -v`.
     - Run coverage: `uv run tools test coverage -- tests/property/runtime tests/unit/runtime` and record branch % for the runtime files listed above.

## Current Tools Focus: Tools Layer & Tools Protocols

These tranches extend the same protocol-driven, strictly-typed testing approach to the `src/ml_playground/tools` package and its CLIs.

### 4. Tools Runtime State & Protocols
- Files: `src/ml_playground/tools/core/runtime.py`, `src/ml_playground/tools/cli/state.py`, `src/ml_playground/tools/protocols.py`.
- Status:
  - `tools/core/runtime.py` exposes `reset_state`, `set_config`, and `load_config_with_error_handling` for the tools CLI.
  - `tools/cli/state.py` manages global tools CLI flags and shared configuration.
- Plan (step-by-step):
  1. - [ ] **Define the shared tools config protocol**
     - Implement `ToolsConfigLike` in `tools/protocols.py` with the attributes:
       - `learning_mode_default: bool`
       - `default_verbosity: int`
  2. - [ ] **Update tools global state**
     - In `tools/cli/state.py`, change `GlobalState.config` to use `ToolsConfigLike | None`.
     - Verify `state.reset()` clears all fields, including `config` and `project_root`.
  3. - [ ] **Wire the protocol into the tools runtime helpers**
     - In `tools/core/runtime.py`, update:
       - `set_config(config: ToolsConfigLike, project_root: Path | None = None)` to store the protocol instance.
       - `load_config_with_error_handling` to call `load_tools_config`, assign to `state.config`, and then read `learning_mode_default` and `default_verbosity` via a local `config` variable.
  4. - [ ] **Keep concrete tools configuration where needed**
     - Leave `tools/cli/dependencies.py` and the concrete tool implementations (`quality`, `testing`, `environment`, `ci`, `dev`) using `ToolsConfig` directly, so they can access the full configuration shape.
  5. - [ ] **Verification**
     - Run the tools CLI property tests: `uv run pytest tests/property/tools/cli -v`.
     - Run all tools property tests: `uv run pytest tests/property/tools -v`.

### 5. Tools Categories & Deterministic Runners
- Files: `src/ml_playground/tools/quality/quality.py`, `src/ml_playground/tools/testing/testing.py`, `src/ml_playground/tools/environment/environment.py`, `src/ml_playground/tools/ci/ci.py`, `src/ml_playground/tools/dev/dev.py`, plus their helpers in `src/ml_playground/tools/**`.
- Status:
  - Property tests under `tests/property/tools/**` already exercise the main categories using a deterministic `SubprocessRunner` stub (`DeterministicRunner`) wired via `ToolsDependencies` overrides.
  - No mocking frameworks are used; behavior is controlled by dependency injection and deterministic subprocess stubs.
- Plan (step-by-step):
  1. - [ ] **Quality tools**
     - Files: `tools/quality/quality.py`, `tools/quality/linting.py`, `tools/quality/formatting.py`, `tools/quality/deadcode.py`, `tools/quality/typing.py`.
     - Tests: `tests/property/tools/quality/test_quality_tools_property.py`.
     - Ensure each public method on `QualityTools` delegates to a single helper function and uses `SubprocessRunner` exclusively for external commands.
     - Extend the property tests if any public method is not yet covered (e.g. `all_checks`).
  2. - [ ] **Testing tools**
     - Files: `tools/testing/testing.py`, `tools/testing/coverage.py`, `tools/testing/mutation.py`, and related helpers.
     - Tests: `tests/property/tools/testing/test_testing_tools_property.py`.
     - Use `DeterministicRunner` and the existing override helpers in `tests/property/tools/_helpers.py` as the only way to stub subprocess behavior.
     - Add property tests for any uncovered `TestingTools` methods that are part of developer workflows (for example, `coverage`, `coverage_threshold`, `clean`).
  3. - [ ] **Environment, CI, and dev tools**
     - Files: `tools/environment/environment.py`, `tools/environment/setup.py`, `tools/environment/verify.py`, `tools/environment/services.py`, `tools/ci/ci.py`, `tools/dev/dev.py`.
     - Add or extend unit/property tests under `tests/property/tools/**` and `tests/unit/tools/**` to cover:
       - Happy-path invocations (e.g. `env setup`, `ci quality-gate`, `dev batch-review`).
       - Representative failure paths (e.g. missing pyproject, failed subprocess).
     - Use dependency injection and deterministic runners; avoid introducing mocks.
  4. - [ ] **Verification and coverage**
     - Run tools tests: `uv run pytest tests/property/tools -v` and `uv run pytest tests/unit/tools -v`.
     - Run coverage focused on tools: `uv run tools test coverage -- tests/property/tools tests/unit/tools`.
     - Record branch coverage for each of the files listed above and iterate until they reach ≥85%.

## Tracking & Success Criteria
- Maintain `docs/plan.md` with per-file branch % after each significant change.
- Do not close a workstream until **every targeted runtime or tools file** hits ≥85% branch coverage and tests are type-clean under the strict tooling.
- When the active runtime/tools workstreams meet the threshold and the global gate is green, update this plan with the next scope (e.g. analysis stack, training hooks).
- Within each runtime/tools scope, **always pick the next targets by ascending branch coverage**. This keeps feedback tight and makes it obvious which files to harden next.

### Pragma removal & strict coverage

We want to eliminate `# pragma: no cover` from production code wherever practical by replacing it with explicit, deterministic tests. Current pragmas in the `src/ml_playground` tree and the coverage plan are:

- [x] **analysis.lit.integration**
  - File: `src/ml_playground/analysis/lit/integration.py`
  - Pragmas:
    - Import/optional-dependency guards for `lit_nlp` and server components.
    - Defensive barriers around app construction and server start fallbacks.
  - Plan:
    - [x] Add focused unit tests under `tests/unit/analysis/test_lit_integration.py` that:
      - [x] Use fakes / `override_attr` to simulate missing `lit_nlp` (import failure) and assert the raised `RuntimeError` messages.
      - [x] Stub `lit_server.Server` / `serve` call sites to raise representative exceptions and assert we rewrap them into `RuntimeError` with the expected text.
      - [x] Exercise the legacy/compatibility serve code paths by injecting minimal fake modules/attributes instead of relying on real LIT.

- [x] **tools.core.runtime**
  - File: `src/ml_playground/tools/core/runtime.py`
  - Pragmas:
    - `ToolConfigurationError` branch and generic `Exception` catch when loading tools config for the CLI.
  - Plan:
    - [x] Add unit tests in `tests/unit/tools/core/test_runtime_state.py` (or a sibling test module) that:
      - [x] Use `override_attr` to force `load_tools_config` to raise `ToolConfigurationError` and assert we echo the configuration error and exit via `typer.Exit(1)`.
      - [x] Do the same for an arbitrary `Exception`, asserting the "Unexpected error" message and exit code.
    - [x] Keep behavior aligned with the tools CLI property tests so the CLI exercises the same paths.

- [x] **tools.testing.testing**
  - File: `src/ml_playground/tools/testing/testing.py`
  - Pragmas:
    - Defensive `except Exception` when collecting combined coverage metrics.
  - Plan:
    - [x] Extend `tests/unit/tools/testing/test_testing_facade_misc.py` by wiring a `FakeSubprocessRunner` / fake JSON handler that deliberately raises during metrics collection, and assert that we return a `ToolResult` with `success=False`, exit code `1`, and combined stderr.
    - [x] Ensure property tests in `tests/property/tools/testing/test_testing_tools_property.py` still use deterministic runners and do not rely on this defensive branch.

- [ ] **tools.testing.mutation**
  - File: `src/ml_playground/tools/testing/mutation.py`
  - Pragmas:
    - Defensive `except Exception` blocks around session-file deletion and `cosmic_ray` invocation/fallbacks.
  - Plan:
    - [ ] Add focused unit tests in `tests/unit/tools/testing/test_mutation.py` that:
      - [ ] Use a fake filesystem object or `Path` stub whose `unlink` raises and assert we surface `ToolExecutionError` with an informative reason.
      - [ ] Inject a `SubprocessRunner` that raises a generic `Exception` when `cosmic_ray` commands are invoked and assert the resulting `ToolResult` has `success=False`, exit code `1`, and the expected stderr.

- [x] **tools.dev.ai_guidelines**
  - File: `src/ml_playground/tools/dev/ai_guidelines.py`
  - Pragmas:
    - Top-level defensive `except Exception` around `setup_ai_guidelines` orchestration.
  - Plan:
    - [x] Extend `tests/unit/tools/dev/test_batch_review.py` or add `tests/unit/tools/dev/test_ai_guidelines.py` to:
      - [x] Inject fakes for filesystem and logging helpers that raise during setup and assert we return `SetupResult(success=False, ...)` with the captured error message.
      - [x] Keep the happy-path property/acceptance tests driving the real flow without triggering this branch.

- [x] **training.checkpointing.service**
  - File: `src/ml_playground/training/checkpointing/service.py`
  - Pragmas:
    - Defensive wrappers around user-supplied `checkpoint_load_fn` / `checkpoint_save_fn` and meta-path resolution.
  - Plan:
    - [x] Add unit tests in `tests/unit/training/checkpointing/test_service.py` that:
      - [x] Pass fakes for `checkpoint_load_fn` and `checkpoint_save_fn` which raise `CheckpointError` / `RuntimeError`, asserting we log a warning and either fall back to defaults (for save) or return `None` (for load).
      - [x] Provide a deliberately invalid meta-path configuration to trigger the meta-source error branch and assert we warn and return `None`.

- [x] **core.tokenizer**
  - File: `src/ml_playground/core/tokenizer.py`
  - Pragmas:
    - Type/shape guards in the character tokenizer path (non-mapping vocab, unsupported `tokenizer_kwargs`).
  - Plan:
    - [x] Extend `tests/unit/core/test_tokenizer.py` (or create it) to:
      - [x] Call the relevant factory with an invalid `vocab` (non-mapping) and assert we raise `TypeError` with the documented message.
      - [x] Pass unsupported `tokenizer_kwargs` and assert we raise `ValueError` with the expected text.

✅ **All defensive branches have been refined with comprehensive tests.** We will only keep pragmas that guard truly untestable branches (e.g. CPython/version-specific behavior). Anything covered by the plans above should have the pragma removed once its tests are in place. As of the latest focused runtime+tools run, line coverage is **92.97%** and branch coverage is **83.06%**, so the global gate (≥90% line / ≥83% branch) is now green.

### Runtime/tools coverage snapshot (latest run)
- **Runtime (branch coverage)**
  - `runtime/device.py` — 100.00%
  - `runtime/helpers.py` — 97.37%
  - `runtime/runners.py` — 92.86%
  - `runtime/cli/typer_helpers.py` — 100.00%
  - `runtime/cli/app.py` — 81.82%
  - `runtime/cli/main.py` — 50.00%
  - `runtime/cli/runners.py` — 66.67%
  - `runtime/core/bootstrap.py` — 33.33%
- **Tools (branch coverage)**
  - `tools/core/config.py` — 85.00%
  - `tools/core/interfaces.py` — 91.67%
  - `tools/core/learning_mode.py` — 84.00%
  - `tools/core/runtime.py` — 50.00%
  - `tools/cli/main.py` — 95.59%
  - `tools/cli/config_loader.py` — 70.00%
  - `tools/cli/commands/ci.py` — line 93.62% (branch n/a)
  - `tools/cli/commands/dev.py` — line 75.68% (branch n/a)
  - `tools/cli/commands/environment.py` — line 79.17% (branch n/a)
  - `tools/cli/commands/quality.py` — line 84.52% (branch n/a)
  - `tools/testing/coverage.py` — 70.77%
  - `tools/testing/mutation.py` — 71.74%
  - `tools/testing/testing.py` — 69.81%
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
- `runtime/core/bootstrap.py` — 33.33%
- `runtime/cli/main.py` — 50.00%
- `tools/core/runtime.py` — 50.00%
- `tools/dev/batch_review.py` — 65.79%
- `tools/cli/config_loader.py` — 70.00%
- `tools/testing/coverage.py` — 70.77%

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

Once the runtime and tools production surfaces are protocol-aligned and the corresponding property tests are green, the next phase is to extend strict typing and mock-free testing to **all test kinds** that exercise this scope. We will tackle **one kind of test at a time**, using the existing directory layout under `tests/` as the source of truth.

1. **Property tests (runtime + tools)**
   - Runtime-focused property tests (all under `tests/property/runtime`):
     - `tests/property/runtime/test_device_property.py`
     - `tests/property/runtime/test_runners_simple_property.py`
     - `tests/property/runtime/test_runners_property.py`
     - `tests/property/runtime/test_helpers_property.py`
     - `tests/property/runtime/test_core_results_property.py`
     - `tests/property/runtime/core/test_bootstrap_property.py`
     - CLI-focused: `tests/property/runtime/cli/test_app_property.py`, `test_cli_property.py`, `test_commands_property.py`, `test_main_property.py`, `test_runners_property.py`, `tests/property/runtime/test_runtime_cli_property.py`
   - Tools-focused property tests (all under `tests/property/tools`):
     - Core/helpers: `tests/property/tools/core/test_runtime_state_property.py`, `tests/property/tools/_helpers.py`
     - CLI: `tests/property/tools/cli/test_tools_cli_property.py`
     - Categories: `tests/property/tools/quality/test_quality_tools_property.py`, `tests/property/tools/testing/test_testing_tools_property.py`, `tests/property/tools/environment/test_environment_tools_property.py`, `tests/property/tools/ci/test_ci_tools_property.py`, `tests/property/tools/dev/test_batch_review_property.py`, `tests/property/tools/dev/test_dev_tools_property.py`
     - Utilities: `tests/property/tools/testing/test_coverage_helpers_property.py`, `tests/property/tools/utils/test_subprocess_utils_property.py`
   - Steps:
     1. - [x] Ensure every fake or stub used in property tests is **strictly typed** and satisfies the relevant runtime/tools protocols (e.g. `PrepareConfigLike`, `TrainConfigLike`, `SampleConfigLike`, `SharedConfigLike`, `ToolsConfigLike`). Prefer small dataclasses or simple helper classes over anonymous `SimpleNamespace` objects.
     2. - [x] Confirm that property tests do **not** use `unittest.mock` or patching frameworks. Where substitution is needed, rely on:
        - Constructor parameters / dependency injection, or
        - Module-level patch points explicitly designed for tests (e.g. deterministic runners via context managers).
     3. - [x] Run property tests for runtime and tools:
        - `uv run pytest tests/property/runtime -v`
        - `uv run pytest tests/property/tools -v`
     4. - [x] Run type checking, ensuring tests are included in the checking scope (via `tools quality typecheck` or equivalent), and fix any reported issues by tightening stubs rather than weakening types.
     5. - [ ] Re-run coverage focused on the production files exercised by these property suites and record per-file branch % in this plan.

2. **Unit tests (runtime + tools)**
   - Runtime-focused unit tests (all under `tests/unit/runtime`):
     - CLI helpers: `tests/unit/runtime/cli/test_cli.py`, `test_device.py`, `test_main_module.py`, `test_typer_helpers.py`
     - Core runtime: `tests/unit/runtime/test_bootstrap.py`, `tests/unit/runtime/test_cli_runtime.py`, `tests/unit/runtime/test_results.py`
   - Tools-focused unit tests (all under `tests/unit/tools`):
     - Core: `tests/unit/tools/core/test_config.py`, `test_errors.py`, `test_interfaces.py`, `test_learning_mode.py`, `test_runtime_state.py`
     - CLI: `tests/unit/tools/cli/test_main.py`, `tests/unit/tools/cli/commands/test_ci_commands.py`, `test_dev_commands.py`, `test_environment_commands.py`, `test_quality_commands.py`, `test_testing_commands.py`
     - Categories: `tests/unit/tools/categories/test_ci.py`, `test_dev.py`, `test_environment.py`, `test_quality.py`, `test_testing.py`, `test_testing_additional.py`
     - Testing helpers: `tests/unit/tools/testing/test_coverage.py`, `test_coverage_helpers.py`, `test_mutation.py`, `test_testing_facade_misc.py`
     - Dev tools: `tests/unit/tools/dev/test_batch_review.py`, `test_hygiene.py`, `test_review_rendering.py`
     - Utils: `tests/unit/tools/utils/test_subprocess_utils.py`, and shared fakes in `tests/unit/tools/fakes.py`
   - Steps:
     1. - [x] Replace any remaining mocks or untyped stubs with small, typed helper classes or `SimpleNamespace` instances that implement the required protocol attributes/methods. Where possible, **reuse** the same stubs used in the property tests.
     2. - [x] Ensure these tests do not introduce `type: ignore` except where strictly necessary (and document the reason next to any ignore).
     3. - [x] Run unit tests and type checks:
        - `uv run pytest tests/unit/runtime -v`
        - `uv run pytest tests/unit/tools -v`
        - `uv run tools quality typecheck`
     4. - [x] Fix any typing issues by tightening the tests (e.g. refining stubs) rather than loosening production types.

3. **Integration tests (runtime + tools)**
   - Files:
     - `tests/integration/test_datasets_shakespeare.py`
     - `tests/integration/test_speakger_pilot.py`
     - `tests/integration/test_trainer_loop.py`
   - Steps:
     1. - [x] Identify where these tests hit runtime or tools surfaces (e.g. training loops that implicitly rely on runtime runners or tools commands).
     2. - [x] Ensure any shared fixtures or helper utilities use protocol-compatible types and deterministic fakes where external systems are involved (e.g. filesystem layout, subprocesses).
     3. - [x] Avoid `unittest.mock`; if behavior substitution is needed, create explicit helper functions or fixtures that wrap the real APIs.
     4. - [x] Run integration tests: `uv run pytest tests/integration -v`.
     5. - [x] Include integration tests in type checking where feasible and fix any typing errors by improving test fixtures and helpers.

4. **Acceptance tests (runtime + tools)**
   - Files:
     - `tests/acceptance/steps/test_checkpointing_steps.py`
     - `tests/acceptance/steps/test_tools_cli_steps.py`
     - `tests/acceptance/tools/cli/test_learn_commands.py`
   - Steps:
     1. - [x] Keep acceptance tests as **black-box behavior checks** while ensuring that any shared helpers (e.g. step definitions, CLI runners) are strictly typed and rely on the same deterministic patterns as the property/unit tests.
     2. - [x] Where acceptance tests drive tools or runtime CLIs, use the real entry points (`ml_playground.tools.cli.main`, `ml_playground.runtime.cli.main`) and inject deterministic configuration/paths via test-only TOML files or fixtures.
     3. - [x] Run acceptance tests: `uv run pytest tests/acceptance -v`.
     4. - [x] Ensure any helpers used by acceptance tests are covered by unit/property tests and are type-clean.

5. **End-to-end (E2E) tests (runtime + tools)**
   - Files:
     - `tests/e2e/ml_playground/test_sample_smoke.py`
     - `tests/e2e/ml_playground/experiments/bundestag_char/test_cli_bundestag_char.py`
     - `tests/e2e/ml_playground/experiments/speakger/test_sampler_analysis.py`
   - Steps:
     1. - [x] Treat E2E tests as **full pipeline** checks using real CLIs and minimal but realistic configuration (see the experiment configs under `tests/e2e/ml_playground/experiments/**`).
     2. - [x] Ensure any E2E-specific fixtures (e.g. temporary directories, sample configs) are typed and reuse the same protocol-compatible config objects where applicable.
     3. - [x] Avoid mocks; if external systems must be isolated (e.g. network), do so via configuration (e.g. localhost endpoints, temporary dirs) rather than patching.
     4. - [x] Run E2E tests: `uv run pytest tests/e2e -v`.
     5. - [x] Optionally include E2E suites in coverage runs to observe high-level coverage, but rely primarily on property/unit/integration tests to hit detailed branches.
