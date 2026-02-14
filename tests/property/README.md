# Property-Based Tests

<details>
<summary>Related documentation</summary>

- [Documentation Guidelines](../../.dev-guidelines/project-specific/DOCUMENTATION.md) – Unified standards for all repository docs, covering top-level, module, test, and tool content.
- [Testing Standards](../../.dev-guidelines/project-specific/TESTING.md) – Strict TDD workflow: write a failing test, implement the minimal fix, then refactor safely with green builds.
- [Unit Tests README](../unit/README.md) – Unit tests validate individual functions, classes, and small modules in isolation.

</details>

Property-based tests validate invariants across large input spaces using Hypothesis.
They run alongside unit tests but live in this dedicated suite so Hypothesis-specific
configuration stays isolated.

## Principles

- Keep properties deterministic: set explicit `@settings(derandomize=True)` or pin the
  Hypothesis seed via environment variables.
- Use dependency injection seams (e.g., `CLIDependencies`, configuration factories)
  instead of monkeypatching or mocks.
- Prefer `TemporaryDirectory()` or other context managers over function-scoped fixtures.
- Exercise public entry points only, per `.dev-guidelines/project-specific/TESTING.md#public-vs-private-apis`.
- Prefer `run_training`/`run_sampling` with `TrainingPlan`/`SamplingPlan` when exercising runtime entrypoints.

## Folder Structure

```bash
tests/property/
├── README.md                       # this file
├── cli_invariants.py               # shared CLI invariant helpers
├── experiments/                    # experiment-specific properties
├── framework/                      # mirrors src/ml_playground/framework/
│   ├── analysis/                   # analysis properties
│   ├── configuration/              # TOML loading and config invariants
│   ├── core/                       # core utility properties
│   ├── data_pipeline/              # data preparation/tokenization properties
│   ├── runtime/                    # framework runtime properties
│   ├── sampling/                   # sampling properties
│   └── training/                   # training properties
├── runtime_cli/                    # runtime CLI properties
└── tools/                          # tools CLI properties
```

## Running

- Full property suite (with unit tests): `uv run tools test coverage`
- Specific property module: `uv run pytest tests/property/<path>/test_*.py`

## Capturing Shrunk Examples as Regression Tests

- **Trigger**: Let Hypothesis shrink a failing input (store is under `.cache/hypothesis/`).
- **Inspect**: Run `uv run python -m hypothesis show <test-module>::<test-name>` to print the shrunken case if available, or open the cached JSON in `.cache/hypothesis/`.
- **Codify**: Translate the minimal input into a deterministic check using `@example(...)` or an explicit unit/property test. Prefer fixtures/helpers over hard-coded globals.
- **Verify**: Rerun the relevant module (`uv run pytest tests/property/<path>/test_*.py`) to ensure the new guardrails fail without the fix and pass with it.
- **Document**: Leave a brief comment referencing the original failure or issue to aid future triage.
