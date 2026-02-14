---
trigger: always_on
description: Core development practices, quality standards, and workflow for contributors
---

# ml_playground Development Practices

Core development practices, quality standards, and workflow for ml_playground contributors.

<details>
<summary>Related documentation</summary>

- [Developer Guidelines Index](./README.md) – Entry point for the ml_playground guideline system with quick-start commands and core principles.
- [Documentation Guidelines](./DOCUMENTATION.md) – Standards for structure, abstraction, and formatting across documentation.
- [Testing Standards](./TESTING.md) – Detailed requirements for TDD, coverage, and test organization.

</details>

## Table of Contents

- [Guiding Principles](#guiding-principles)
- [Quality Gates (Mandatory)](#quality-gates-mandatory)
- [Commit Standards](#commit-standards)
- [Testing Standards](#testing-standards)
- [Code Style Standards](#code-style-standards)
- [Tool Configuration Policy](#tool-configuration-policy)
- [Dev Tooling Quick Reference](#dev-tooling-quick-reference)
- [Architecture Notes](#architecture-notes)

## Guiding Principles

- **Quality gates and TDD discipline.** Always run `uv run tools ci quality-gate` before committing and practice strict
  TDD as the default workflow (see [Developer Guidelines](README.md#core-principles-non-negotiable) and the canonical
  process in [Testing Standards](TESTING.md#test-driven-development-required)).
- **UV-first Typer CLIs.** Prefer the published Typer entry points (`tools env`, `tools test`, `tools ci`) over ad-hoc
  scripts so local workflows mirror CI (see the [repository README](../README.md#policy)).
- **Single-source, fail-fast configuration.** Treat TOML as the sole source of truth; the configuration loaders merge the
  global defaults with experiment overrides, resolve relative paths, and raise immediately on malformed input while the
  strict Pydantic models forbid extras and enforce cross-field invariants (see
  [Configuration documentation](../docs/framework_utilities.md#configuration-system) and
  [`ml_playground/framework/configuration`](../src/ml_playground/framework/configuration/)).
- **Strict Typing, Immutability, and Determinism.** Use explicit type hints, treat configuration and data structures as
  immutable, and favor pure, side-effect-free functions to keep code predictable and prevent state bugs.
- **Centralized Utilities and Explicit Device Management.** Extend shared infrastructure (tokenizers, data prep, error
  handling) rather than reinventing it. Device selection (CPU/GPU) must be explicit and passed through function
  signatures; global device state is forbidden.
- **Deterministic, multi-layered tests.** Keep unit tests fast, isolated, and deterministic; complement them with
  property, integration, e2e, and acceptance suites so changes are guarded at multiple levels while maintaining coverage
  expectations (see [tests/README.md](../tests/README.md#structure) and
  [tests/unit/README.md](../tests/unit/README.md#principles)).
- **Documentation with intentional abstraction.** Follow the abstraction gradient and DRY rules for README files,
  keeping shared narratives centralized and using annotated folder trees rather than duplicating prose (see the
  [Documentation Guidelines](DOCUMENTATION.md#abstraction-policy)).
- **Git hygiene and reviewability.** Develop on short-lived feature branches, keep commits granular and conventional,
  and maintain a linear, runnable history to streamline reviews and CI (see
  [Developer Guidelines](README.md#core-principles-non-negotiable)).
- **Self-contained tooling.** Run helper scripts via UV, keep them documented and explicit in their CLI contracts, and
  avoid hidden behavior or manual environment tweaks (see [tools/README.md](../tools/README.md#conventions)).
- **Strict API Boundaries and Dependency Scoping.** External imports must be restricted to a defined public API surface.
  Production code must only import packages from the default dependency group; development dependencies are forbidden.
- **Explicit Error Contracts (Fail Fast).** All public functions must explicitly document the exceptions they raise.
  Catching broad exceptions (`except Exception:`, `except:`) is strictly forbidden.
- **No test-only bypass of public APIs.** Tests must exercise the same public surface that production code uses. Public
  injection hooks (e.g., `runtime_cli` global option overrides for echo/log/context) exist for controlled dependency
  replacement and are the only supported seam; do not reach into private helpers or rebind module globals to avoid the
  public API. When new hooks are required, add them to the public API explicitly and document them rather than patching
  internals.

## Quality Gates (Mandatory)

Pre-commit and CI both execute `uv run tools ci quality-gate`, which wraps ruff lint/format, mdformat, pyright, mypy, and the targeted pytest slices. Override the default parallelism via `uv run tools ci quality-gate PRE_COMMIT_JOBS=4` when needed. See [Framework Utilities Documentation](../docs/framework_utilities.md) for supporting infrastructure.

For focused iterations, rely on task-specific commands (e.g., `uv run pytest path/to/test.py`, `uv run ruff check path/to/file.py`). Convenience wrappers remain available under `tools ci` and `tools env` for coverage reports, property suites, and lint-only passes.

## Commit Standards

### Granular Commits Policy

- **One logical change per commit** (e.g., fix a test, adjust a config, refactor a function)
- **Keep commits under ~200 lines** unless unavoidable
- **Ensure quality gates pass before the commit is recorded** (the pre-commit hook enforces this automatically)
- **Pairing rule (REQUIRED)**: Each functional or behavioral change MUST include its tests in the same commit (unit
  and/or integration). Creating new files (untracked) is expected when adding tests—stage them together with the
  production change.
- **Granularity**: Prefer a short sequence of TDD commits to a single large commit. Each step should keep the suite
  green.

### Practical Tips

- Stage hunks selectively: `git add -p`
- Separate mechanical formatting from semantic changes
- Split large features into reviewable increments
- Keep tests passing at every step
- Pair production and test changes: when adding/refactoring code, include the minimal tests that specify the behavior in
  the same commit.
- Acceptable exceptions: documentation only commits, pure test refactors (no behavior change), or mechanical formatting.
  For everything else, pair code+tests.

### Runnable State Requirement (MANDATORY)

- Every commit MUST be in a runnable state when checked out.
- Runnable means:
  - Pre-commit (and therefore `uv run tools ci quality-gate`) passes when the commit is created. Do not bypass hooks or suppress failures.
  - No partially applied migrations or broken CLI entry points.
  - Documentation build (if modified) is not broken.
- Do not commit code that knowingly breaks the build with intent to "fix later". Split work into smaller, independently
  runnable commits.

### Branching Model (Feature Branches REQUIRED)

- All work MUST happen on short-lived feature branches. Do not commit directly to `main`.
- For naming conventions, linear history, and rebase policy, see `GIT_VERSIONING.md`.

### Conventional Commit Format

See `GIT_VERSIONING.md` for the required Conventional Commits format, examples, and usage notes.

## Testing Standards

For testing standards, practices, and examples, see [TESTING.md](TESTING.md).

## Code Style Standards

### Automatic Formatting

Ruff automatically applies modern Python best practices:

- **Type annotations**: `typing.List` → `list`, `typing.Dict` → `dict`
- **Union syntax**: `Optional[str]` → `str | None`, `Union[A, B]` → `A | B`
- **Import organization**: Sorted and cleaned automatically
- **Code formatting**: Black-style formatting
- **Whitespace cleanup**: Trailing whitespace removal

### Development Guidelines

- **Strictly typed code**: Use explicit types and `pathlib.Path` for filesystem paths
- **No Dynamic Attributes**: `hasattr`/`setattr` are banned in tests (and production code) to enforce type safety and usage of the public API.
- **Pure functions and dependency injection**:
  - **Prefer pure functions**: Functions should be deterministic and side-effect free
  - **Explicit dependencies**: Pass dependencies as function parameters instead of using global state
  - **Avoid global mutable state**: Use factory functions and explicit parameter passing instead of module-level singletons
  - **Example**:
    ```python
    # ❌ Bad: Global state
    _current_deps = None
    def get_deps(): return _current_deps
    
    # ✅ Good: Explicit dependency injection
    def run_command(deps: Dependencies, ...): ...
    ```
- **Pure functions**: Favor pure functions for data preparation
- **Explicit device selection**: Make device selection explicit in code
- **Configuration and overrides**:
  - TOML is the primary source of truth; avoid ad-hoc CLI flags that mutate config.
  - Allowed exceptions (documented and tested):
    - Global CLI option `--exp-config PATH` to choose a specific experiment TOML; `experiments/default_config.toml` is
      merged first.
    - Environment JSON overrides: `ML_PLAYGROUND_TRAIN_OVERRIDES` and `ML_PLAYGROUND_SAMPLE_OVERRIDES`.
      These are deep-merged and then strictly re-validated; invalid env overrides are ignored.

## Tool Configuration Policy

**Single-source rule**: All tool configuration must live in `pyproject.toml` only.

**Prohibited**: Standalone config files (.ruff.toml, mypy.ini, pyrightconfig.json, pytest.ini, setup.cfg, etc.)

**Centralized sections**:

- `[tool.ruff]` for lint/format settings
- `[tool.mypy]` for type checker settings
- `[tool.pyright]` for static analysis include/exclude
- `[tool.pytest.ini_options]` for pytest testpaths and options

## Dev Tooling Quick Reference

Use these commands from the repository root (or specify `--repo`/`--cwd` when required). They are
optimized for non-interactive or copy-paste workflows.

### uv

- **One-off commands**:

  ```bash
  uv run python -m http.server 8000
  uv run ruff check ml_playground/
  ```

- **Locked sync (no drift)**:

  ```bash
  uv sync --locked
  ```

- **Regenerate lockfile with explicit upgrade**:

  ```bash
  uv lock --upgrade
  ```

### ripgrep (`rg`)

- **Search code (smart case)**:

  ```bash
  rg "CheckpointManager" ml_playground/
  ```

- **Search tests only**:

  ```bash
  rg --glob 'tests/**' "ml_playground.runtime_cli"
  ```

### GitHub CLI (`gh`)

- **Create draft PR (copy existing description file)**:

  ```bash
  gh pr create --draft --base master --title "docs: add dev tooling quick reference" --body-file .ldres/pr-body.md
  ```

- **Watch PR status (non-interactive)**:

  ```bash
  gh pr checks --watch
  ```

- **Open PR in browser**:

  ```bash
  gh pr view --web
  ```

### GitHub Actions locally (`act`)

- **Install**:

  ```bash
  brew install act
  ```

  Use the [official installation guide](https://nektosact.com/installation/index.html) on non-macOS platforms and make sure
  Docker Desktop (or another OCI runtime) is running before executing any jobs.

- **Match the hosted runner image**:

  ```bash
  act --container-architecture linux/amd64 \
      -P ubuntu-latest=catthehacker/ubuntu:act-latest --list
  ```

  Running with `--list` prints the jobs `act` would execute and validates that the image mapping provides Python 3.13 and
  the tooling bundle we expect on `ubuntu-latest`.

- **Replay the quality workflow**:

  ```bash
  act --container-architecture linux/amd64 \
      -P ubuntu-latest=catthehacker/ubuntu:act-latest \
      -W .github/workflows/quality.yml --job quality
  ```

  Passing `--job` narrows the invocation to the `quality` job, matching what CI runs on pushes and PRs.

- **Persist caches between runs**:

  ```bash
  act --container-architecture linux/amd64 \
      -P ubuntu-latest=catthehacker/ubuntu:act-latest \
      -W .github/workflows/quality.yml --job quality \
      --bind .cache:/root/.cache --bind .venv:/root/project/.venv
  ```

  Mirror CI cache locations (`.cache/uv`, `.cache/ruff`, `.venv`) so dependency downloads survive container teardown and
  subsequent runs start warm.

- **Inject secrets or environment overrides when necessary**:

  ```bash
  act --secret GITHUB_TOKEN=$(gh auth token) \
      -e .github/workflows/events/quality.push.json \
      -P ubuntu-latest=catthehacker/ubuntu:act-latest
  ```

  Event payloads let you debug branch- or PR-specific logic locally; store them under `.github/workflows/events/` if you
  need repeatable scenarios and remember to keep sensitive data out of version control.

- **Understand limitations**: `act` skips GitHub-hosted services (e.g., dependency caches, OpenID providers) and only
  approximates matrix and reusable workflows. Use it for fast feedback, then confirm with a real GitHub Actions run before
  merging.

#### CI inspection shortcuts

- **List recent runs for a branch**:

  ```bash
  gh run list --branch chore/python-version-alignment --limit 5 --json databaseId,headSha,status,conclusion,url
  ```

- **Surface failing runs only**:

  ```bash
  gh run list --branch chore/python-version-alignment --status failure --json databaseId,url
  ```

- **Inspect job-level status for a run**:

  ```bash
  gh api repos/mehrmorgen/nanoGPT/actions/runs/18234316534/jobs --jq '.jobs[] | {name,status,conclusion,log_url}'
  ```

#### PR management helpers

- **One-shot PR status**:

  ```bash
  gh pr checks 41
  ```

- **Merge with rebase and clean branches**:

  ```bash
  gh pr merge 41 --rebase --delete-branch
  ```

- **Find PRs for the current branch**:

  ```bash
  gh pr list --head docs/dev-tooling
  ```

### `fzf` helpers

- **Interactive file picker feeding `rg`**:

  ```bash
  rg --files | fzf | xargs open
  ```

- **Pick Make targets**:

  ```bash
  ./tools/dev_tasks.py --help
  ```

### Non-interactive status & logs

- **Tail quality logs (CI artifact)**:

  ```bash
  gh run download --name quality --dir /tmp/mlp-quality && tail -n 200 /tmp/mlp-quality/*/logs.txt
  ```

- **Follow local development server**:

  ```bash
  uv run python scripts/run_dev_server.py 2>&1 | tee /tmp/mlp-dev.log
  ```

- **Check git status without pager**:

  ```bash
  git -c pager.status=cat status --short --branch
  ```

- **Audit branch tracking info**:

  ```bash
  git branch -vv
  ```

- **List merged branches (safe to delete)**:

  ```bash
  git branch --merged
  ```

- **Prune local branches with no remote**:

  ```bash
  git remote prune origin
  ```

- **See commits not yet on master**:

  ```bash
  git rev-list refactor/p11-di-samplerconfig ^master
  ```

## Architecture Notes

See [`ARCHITECTURE.md`](ARCHITECTURE.md) for detailed diagrams and flows covering configuration loading, dataset lifecycle, runtime entry points, and checkpoint retention policies.

For import standards, see [IMPORT_GUIDELINES.md](IMPORT_GUIDELINES.md).
