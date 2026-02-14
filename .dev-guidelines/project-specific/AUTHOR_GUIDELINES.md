---
trigger: manual
description: Author responsibilities and workflows for code review submissions
---

# Code Review Author Guidelines

> Adapted from Michael Lynch’s [How to Make Your Code Reviewer Fall in Love with You](https://mtlynch.io/code-review-love/) (CC BY 4.0).

## Purpose

- Clarify expectations for anyone opening a pull request in `ml_playground`.
- Pair with project policies in `DEVELOPMENT.md`, `TESTING.md`, and `GIT_VERSIONING.md` to keep reviews fast, empathetic, and deterministic.

## Before You Open a Pull Request

- Run local automation using `uv run tools ci quality-gate` and any focused commands (`uv run tools test unit`, `uv run tools quality lint`) required for confidence.
- Self-review every diff via your editor or `git diff --staged` to catch TODOs, debug prints, or flaky changes.
- Confirm scope is limited to one logical change (see `DEVELOPMENT.md#granular-commits-policy`) and that each functional change ships with tests.
- Update documentation, configs, and telemetry together when behavior changes; link the authoritative file per `DOCUMENTATION.md`.

## Preparing the Pull Request

- Write the why first in the PR template. Describe user impact, alternatives considered, and follow-up items.
- Summarize validation: list the exact `uv run ...` commands executed, attach coverage deltas if relevant, and link to dashboards.
- Call out risk areas (performance, migrations, data shape changes) so reviewers can focus on the right contexts.
- Attach screenshots or logs when CLI output or artifacts change. Store long-lived assets under `docs/assets/`.

## Optimizing for Reviewers

- Label the PR appropriately (feature, bugfix, docs) and @-mention the teammates accountable for the touched areas—every core contributor shares code ownership and is expected to engage.
- Batch related commits: use Conventional Commits (`docs(..): ...`, `feat(..): ...`) and keep history rebased onto `master`.
- Provide inline guidance: leave temporary PR comments when refactors unblock future work or when reviewers should focus on specific files.
- Surface dependencies: mention linked issues, experiments, or follow-up tickets so context is easy to trace.

## During Review

- Respond promptly: aim to reply or push updates within one business day to keep context warm.
- Prefer discussion over debate: summarize what you heard and propose next steps when disagreements arise.
- Use project tooling: run `uv run tools dev review-list <pr_number> --unreplied --unresolved` daily until the review closes.
- Document decisions: capture significant design outcomes directly in the PR conversation for future discoverability.

## After Approval

- Re-run quality gates if rebasing or force-pushing. Do not rely on stale CI runs.
- Squash responsibly: preserve meaningful commit boundaries unless the reviewer requests a squash merge.
- Verify monitors post-merge (coverage badges, mutation reports) and follow up if automation flags regressions.
- Retro: note friction points so we can evolve the guidelines in retrospectives.

## References

- Parent policies: [`DEVELOPMENT.md`](./DEVELOPMENT.md), [`TESTING.md`](./TESTING.md), [`GIT_VERSIONING.md`](./GIT_VERSIONING.md), [`DOCUMENTATION.md`](./DOCUMENTATION.md)
- Review checklist: [`CODE_REVIEW_CHECKLIST.md`](./CODE_REVIEW_CHECKLIST.md)
- Reviewer counterpart: [`REVIEWER_GUIDELINES.md`](./REVIEWER_GUIDELINES.md)
