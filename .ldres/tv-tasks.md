# tv-tasks

This file tracks targeted refactoring tasks Thomas requested. Keep items small, reviewable,
and enforce our UV + pre-commit workflow. Each task includes scope, acceptance criteria, and commit guidance.

---

## Task 1: Reduce Makefile indirections in test targets

- Scope:
  - In `Makefile`, replace indirections like `$(MAKE) pytest-core PYARGS="-m integration --no-cov"`
with direct invocations using `$(RUN) pytest` and `$(PYTEST_BASE)`.
  - Targets to simplify: `integration`, `e2e`, and any others using `pytest-core` where a single direct call is clearer.
- Acceptance criteria:
  - `make integration` and `make e2e` call `$(RUN) pytest $(PYTEST_BASE) ...` directly.
  - `make test`, `make pytest-all`, and pre-commit continue to pass locally and in CI.
- Commit guidance:
  - Branch: `chore/makefile-simplify-test-targets`
  - Commit: `chore(makefile): reduce test target indirections; invoke pytest directly`

---

## Task 2: Integrate standalone units into canonical packages

- Target units:
  - `ml_playground/checkpoint.py`
  - `ml_playground/ema.py`
  - `ml_playground/estimator.py`
  - `ml_playground/lr_scheduler.py`
  - `ml_playground/tokenizer_protocol.py`
- Scope:
  - Move or refactor these modules under their canonical package locations
(`ml_playground/training/checkpointing/`, `ml_playground/training/`, `ml_playground/models/utils/`
or `core/`, `ml_playground/training/optim/`, `ml_playground/core/`), aligned with prior restructurings.
  - Update all imports across the codebase and tests to the new canonical paths.
  - Ensure there are no re-export facades; comply with `IMPORT_GUIDELINES.md`.
- Acceptance criteria:
  - All imports reference canonical modules; no legacy paths remain.
  - `make quality` and the full test suite pass.
  - No circular imports introduced; `pyright` clean.
- Commit guidance:
  - Use small commits, one module at a time:
    - `refactor(training): move checkpoint helpers to training/checkpointing`
    - `refactor(training): move ema to training/ema`
    - `refactor(models): move estimator to models/utils` (or agreed location)
    - `refactor(training): move lr_scheduler to training/optim`
    - `refactor(core): move tokenizer_protocol to core/`
  - Follow up with `docs(...)` if READMEs or guides reference old paths.

---

## Task 3: Rename refactorings list to tv-tasks.md

- Scope:
  - Adopt `.ldres/tv-tasks.md` as the source of truth for Thomas's task list.
  - Optionally add a pointer note at the top of `.ldres/Refactorings.md` indicating this file supersedes it.
- Acceptance criteria:
  - `.ldres/tv-tasks.md` exists and is referenced in ongoing work.
  - Team uses this file for new action items.
- Commit guidance:
  - Branch: `docs/tv-tasks`
  - Commit: `docs(tasks): add tv-tasks.md and point from Refactorings.md`

---

## Notes

- `make quality` is the canonical gate and already runs pre-commit hooks
(ruff, format, checkmake, pyright, mypy, vulture,pytest).
No separate test run in the Make target is necessary.
- When the "remove mocks" effort is complete, enforce no mock usage via Ruff banned-modules and a CI grep sweep.

---

## Task 8: Automate coverage badge generation ✅ (2025-10-02)

- Scope:
  - Add the `coverage-badge` CLI as a dependency and create a `Makefile` target `coverage-badge` that
  runs `uv run make coverage-report`, emits `coverage.xml`,
  and writes `docs/assets/coverage.svg`.
  - Add a local pre-commit hook that invokes the make target and stages the regenerated badge automatically.
  - Extend the GitHub Actions quality workflow to run the same target and fail if the committed badge diverges.
  - Update `README.md` to render the badge and document the workflow in `.dev-guidelines/TESTING.md`.
- Acceptance criteria:
  - Every commit (local and CI) regenerates and stages an up-to-date `docs/assets/coverage.svg`.
  - `make coverage-badge` is idempotent and shared across local and CI workflows.
  - README badge always reflects latest coverage; quality gate ensures badge freshness.
- Commit guidance:
  - Branch: `chore/coverage-badge`
  - Commit 1: `chore(makefile): add coverage badge target`
  - Commit 2: `chore(ci): enforce coverage badge freshness`
  - Commit 3: `docs(readme): add coverage badge`

---

This document enumerates prioritized refactoring prompts that can be handed to code-generation agents.
Each numbered task contains nested sub-goals to encourage small, reviewable commits.

---

## Agent-Ready, Granular Refactoring Prompts (Copy/Paste)

These prompts are self-contained and aligned with:

- `.dev-guidelines/DEVELOPMENT.md`
- `.dev-guidelines/IMPORT_GUIDELINES.md`
- `.dev-guidelines/GIT_VERSIONING.md`
- `.dev-guidelines/TESTING.md`
- `docs/framework_utilities.md`

Pre-commit hooks automatically run `make quality` during commits,
so manual invocations are optional if you want faster feedback. Use short-lived feature branches,
follow Conventional Commits, and always pair behavioral changes with tests. Never bypass verification (`--no-verify` is prohibited).

### Mandatory workflow for every prompt (no exceptions)

- **Create feature branch**: Update `main`, then create a kebab-case feature branch (`git switch -c <type>/<scope>-<desc>`).
- **Keep changesets tiny**: Implement the minimal behavior in ≤200 touched lines per commit, pairing tests and code.
- **Run quality gates**: Execute `make quality` (or stricter slices) locally before each commit and before opening the PR.
- **Open focused PR**: Push the branch, open a PR summarizing the change, list validation commands, and request review.
- **Merge cleanly into `main`**: After CI passes and review approval,
use fast-forward or rebase-merge so the branch lands on `main` (a.k.a. master); delete the feature branch afterward.

---
