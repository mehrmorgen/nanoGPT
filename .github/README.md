---
trigger: manual
description: Implementation notes for GitHub automation, workflows, and operational tooling
---

# GitHub Automation Overview

Implementation-specific reference for the `.github/` directory, detailing how our CI workflows are structured and operated on GitHub Actions.

<details>
<summary>Related documentation</summary>

- [Continuous Integration Guidelines](../.dev-guidelines/project-specific/CI.md) – Platform-agnostic CI policies and maintenance checklist.
- [Development Practices](../.dev-guidelines/project-specific/DEVELOPMENT.md) – Core quality gates, tooling, and workflow expectations.
- [Testing Standards](../.dev-guidelines/project-specific/TESTING.md) – Required TDD workflow and test organization.

</details>

## Table of Contents

- [Directory Overview](#directory-overview)
- [Workflow Summary](#workflow-summary)
- [Caching Implementation](#caching-implementation)
- [Operational Commands](#operational-commands)
- [Review Automation](#review-automation)
- [Maintenance Notes](#maintenance-notes)

## Directory Overview

```bash
.github/
├── README.md               # GitHub automation reference (this file)
├── workflows/              # GitHub Actions workflow definitions
│   ├── quality.yml         # Fast gate covering linting, typing, and test suite
│   └── mutation-suite.yml  # Scheduled/manual mutation testing pipeline
├── copilot-instructions.md # Supplemental settings for GitHub Copilot
├── ARCHITECTURE.md         # Mirrors core architecture guidelines for external links
├── DEVELOPMENT.md          # Mirrors development practices for external links
├── DOCUMENTATION.md        # Mirrors documentation guidelines for external links
├── GIT_VERSIONING.md       # Mirrors versioning rules for external links
├── IMPORT_GUIDELINES.md    # Mirrors import policy for external links
├── REQUIREMENTS.md         # Mirrors dependency policy for external links
├── SETUP.md                # Mirrors environment setup guide for external links
└── TESTING.md              # Mirrors testing standards for external links
```

## Workflow Summary

- **`quality.yml`**
  - **Purpose**: Mandatory gate executed on every push/PR to enforce linting, formatting, typing, and tiered pytest suites via `uv run ci-tasks quality`.
  - **Triggers**: `push`/`pull_request` to active branches.
  - **Timeout**: 30 minutes (default runner limit; job typically completes in ~2 minutes when caches hit).
  - **Caching**: Relies on `astral-sh/setup-uv@v6` for wheel caching plus standalone `pre-commit` and `ruff` caches; the job does not restore `.venv`, instead running `uv sync --frozen --group dev` each time before pruning the wheel cache.
  - **Manual use**: `gh workflow run quality.yml --ref <branch>`.
- **`mutation-suite.yml`**
  - **Purpose**: Executes the mutation testing suite (`uv run ci-tasks mutation run`) and captures reports.
  - **Triggers**: Weekly cron (`0 1 * * 1`) and manual `workflow_dispatch` for investigative runs.
  - **Timeout**: Explicit 180-minute limit to cap long-running mutation jobs.
  - **Prerequisites**: Installs `python3-dev`, `build-essential`, `libffi-dev`, and `gfortran` before dependency sync.
  - **Caching**: Reuses the shared `uv` wheel cache and the targeted `pre-commit`/`ruff` caches without restoring `.venv`; dependency sync always runs before invoking the mutation suite, followed by a wheel-cache prune.
  - **Artifacts**: Uploads `mutation-report.txt` and `.cache/cosmic-ray/session.sqlite` on completion.

See [`workflows/README.md`](workflows/README.md) for per-file implementation details, command snippets, and change history highlights.

## Caching Implementation

- **Wheel cache (`.cache/uv`)**: Delegated to `astral-sh/setup-uv@v6` with `enable-cache: true`, `cache-local-path: .cache/uv`, and a post-step `uv cache prune --ci` to prevent growth.
- **Targeted tool caches**: `pre-commit` (`.cache/pre-commit`) and `ruff` (`.cache/ruff`) use dedicated `actions/cache@v4` entries so config changes do not invalidate unrelated data.
- **Virtual environment**: Not cached. Gate and mutation workflows always run `uv sync --frozen --group dev` before executing tasks so the environment mirrors the lockfile on every run.

## Operational Commands

- **Trigger a workflow**: `gh workflow run <workflow>.yml --ref <branch>`.
- **Monitor a run**: `gh run watch <run-id>` for full streaming logs.
- **Sample progress**: `timeout 180 gh run watch <run-id>` to observe the first three minutes and exit (workflow continues server-side).
- **Cancel a run**: `gh run cancel <run-id>` to free runner capacity.
- **List recent runs**: `gh run list --workflow <workflow>.yml --limit 5`.

## Review Automation

Our review workflow combines shared ownership with explicit prompts so every pull request includes context, validation, and clear follow-up paths.

- **Ownership model**: every core contributor is a code owner. Authors @-mention the most relevant teammates instead of relying on CODEOWNERS.
- **Pull request template**: `.github/PULL_REQUEST_TEMPLATE.md` captures validation commands, *risk call-outs* (highlights for deploy, data, or performance hazards), and *reviewer notes* (files or commits that deserve focused attention). Authors must complete every section before requesting review.
- **Review checklist**: `.dev-guidelines/project-specific/CODE_REVIEW_CHECKLIST.md` is referenced by the PR template and complements author/reviewer guidelines.
- **Bots and tooling**: `tools/review.py` provides helper commands for triaging discussion threads. Consider adding automation hooks (e.g., GitHub Apps, Probot) to ensure checklists are completed before merging.

## Maintenance Notes

- Reflect any workflow changes (new jobs, triggers, cache keys) in this README and in `../.dev-guidelines/project-specific/CI.md` during the same pull request.
- When bumping action versions or adding system dependencies, document the change under the relevant workflow section.
- Review scheduled workflows quarterly to validate cron cadence, credential freshness, and runtime budgets.
- Keep this README focused on GitHub-specific implementation; platform-agnostic principles belong in the central CI guidelines.
