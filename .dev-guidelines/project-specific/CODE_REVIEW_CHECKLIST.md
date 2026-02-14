---
trigger: manual
description: Shared code review checklist for authors and reviewers
---

# Code Review Checklist

## For Authors

- [ ] `uv run tools ci quality-gate` passes and relevant focused suites (e.g., `uv run tools test unit`) are green.
- [ ] PR title states the single change being shipped; summary covers the why, user impact, and validation commands (copy/paste outputs when useful).
- [ ] Scope is one logical change; commits follow Conventional Commit style and pair code with tests.
- [ ] Documentation/config updates accompany behavior changes; links point to canonical sources.
- [ ] Screenshots/logs are attached when CLI output, artifacts, or UX change.

## For Reviewers

- [ ] Understand the problem statement, constraints, and proposed solution.
- [ ] Code is readable, maintainable, and aligned with `DOCUMENTATION.md`/`DEVELOPMENT.md` abstractions.
- [ ] Tests cover new behavior, negative paths, and edge cases; automation gaps are identified.
- [ ] Naming, structure, and dependencies follow import and configuration guidelines.
- [ ] Feedback is actionable, kind, and marked `[blocking]` only when necessary.
