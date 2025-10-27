# tools/ (Legacy)

**DEPRECATED**: This directory contains legacy developer utilities. The functionality has been integrated into the main `ml_playground` module and is now accessible via `uv run tools <command>`.

## Migration Status

**New Integrated Commands** (use these instead):
- `uv run tools ci quality-gate` — Quality gates, coverage workflows, and mutation testing
- `uv run tools env <command>` — Environment setup, verification, cache cleanup
- `uv run tools quality <command>` — Lint/format operations for fast feedback  
- `uv run tools test <command>` — Pytest suite orchestration
- `uv run tools agentic <command>` — AI-assisted development workflows

**Legacy Files** (still available but deprecated):
- `ci_tasks.py` — Use `uv run tools ci` instead
- `env_tasks.py` — Use `uv run tools env` instead  
- `lint_tasks.py` — Use `uv run tools quality` instead
- `test_tasks.py` — Use `uv run tools test` instead
- `lit_tasks.py` — LIT integration helpers (no integrated equivalent yet)

**Utility Scripts** (still used):
- `task_utils.py` — shared helpers used by legacy CLIs
- `review.py` — inspect review threads, bulk-reply, and delete comments using JSON mappings
- `cleanup_ignored_tracked.py` — remove accidentally tracked files that should be ignored
- `mutation_summary.py` — prints the active Cosmic Ray configuration before mutation runs
- `mutation_report.py` — summarizes mutant outcomes after a Cosmic Ray run
- `port_kill.py` — kill a process bound to a TCP port (Mac/Linux)
- `setup_ai_guidelines.py` — configure symlinks for AI pair-programming workflow per guideline docs
- `llama_cpp/` — vendor instructions and helpers for GGUF conversion


## GitHub CLI setup

The GitHub CLI (`gh`) is the preferred way to open pull requests, review diffs, and inspect CI from the terminal.

### Install

- **macOS**: `brew install gh`
- **Windows**: `winget install --id GitHub.cli`
- **Ubuntu/Debian**: Follow the [official instructions](https://cli.github.com/packages) to add the repository, then run `sudo apt install gh`.
- **Other distros**: follow the binary instructions at https://cli.github.com/manual/installation

Verify the CLI is wired up before running project scripts:

```bash
gh --version
```

### Authenticate

Run the guided login once per machine:

```bash
gh auth login --web --scopes 'repo,workflow'
gh auth status
```

- Choose GitHub.com ➔ HTTPS ➔ `Y` to use the web browser flow.
- The additional `workflow` scope lets `gh pr merge` interact with required checks.
- Confirm credentials are cached in the macOS keychain (or your OS-equivalent credential store).

### Daily usage

- **Create PRs** from a feature branch: `gh pr create --fill --head <branch>` (opens editor for body tweaks).
- **Sync** with default branch before pushing: `git fetch origin && git rebase origin/master` (requires a clean worktree).
- **Inspect CI** without leaving the shell: `gh run list --limit 5`.
- **Check out reviews**: `gh pr checkout <number-or-url>`.

Keep commands scoped to the repository root; the CLI inherits the project Git remotes.

## Usage

Always run through the project venv using UV. From repo root:

**New Integrated Commands** (recommended):
```bash
# Quality gates
uv run tools ci quality-gate

# Coverage report with threshold enforcement  
uv run tools test coverage --fail-under 87

# Run unit tests
uv run tools test unit

# Fast lint bundle
uv run tools quality lint

# Environment setup
uv run tools env setup
```

**Legacy Commands** (deprecated but still functional):
```bash
# Quality gates (legacy)
uv run ci-tasks quality

# Run GitHub quality workflow locally via act (legacy)
uv run ci-tasks quality-ci-local

# Coverage report with threshold enforcement (legacy)
uv run ci-tasks coverage-report --fail-under 87

# Run unit tests (legacy)
uv run test-tasks unit

# Fast lint bundle (legacy)
uv run lint-tasks ruff

# Environment setup (legacy)
uv run env-tasks setup
```

- **`quality-ci-local`** (legacy): Binds `.cache/uv`, `.cache/pre-commit`, `.cache/ruff`, and `.venv` into the container. Toggle mounts with `--no-bind-caches` or pass additional flags directly to `act`. Use `uv run tools ci local` for the integrated equivalent.

## Examples

- TensorBoard port is stuck on 6006:

```bash
uv run python tools/port_kill.py 6006
# Use integrated command: uv run tools env info  # (shows TensorBoard status)
# Or legacy command: uv run env-tasks tensorboard --logdir out/<run>/logs/tb
```

- Clean up noisy artifacts that slipped into Git:

```bash
uv run python tools/cleanup_ignored_tracked.py --dry-run
uv run python tools/cleanup_ignored_tracked.py --apply
```

- Reply to multiple review comments at once:

```bash
cat > replies.json <<'JSON'
{
  "https://github.com/ORG/REPO/pull/123#discussion_r1": "Thanks, updated config.",
  "PRRC_kwD0...": "Handled in commit abc123."
}
JSON
uv run python tools/review.py list --pr 123 --unreplied --unresolved
uv run python tools/review.py bulk-reply --pr 123 --replies replies.json --dry-run
uv run python tools/review.py bulk-reply --pr 123 --replies replies.json
```

- Delete review comments in bulk:

```bash
cat > delete.json <<'JSON'
[
  "https://github.com/ORG/REPO/pull/123#discussion_r1",
  "PRRC_kwD0..."
]
JSON
uv run python tools/review.py delete --pr 123 --comments delete.json --dry-run
uv run python tools/review.py delete --pr 123 --comments delete.json
```

## Conventions

- UV-only: invoke tools with `uv run python ...` to use the project environment.
- Keep scripts self-contained, documented, and under 200 LOC where practical.
- Prefer clear CLI flags and `--help` text; avoid hidden behavior.
- Align documentation with `.dev-guidelines/DOCUMENTATION.md` when editing this file or adding tool docs; keep mutation workflow notes in `.dev-guidelines/TESTING.md`.
