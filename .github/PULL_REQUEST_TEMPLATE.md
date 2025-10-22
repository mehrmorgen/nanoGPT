---
name: Pull Request
about: Submit a change aligned with ml_playground review standards
---

# Pull Request

## Summary

Describe the user-facing impact. Explain the **why** before the **what**. Link design docs or experiment notes if applicable.

## Context & Links

- Related issues / tickets:
- Follow-up work (if any):

## Testing & Validation

- Commands executed:

  ```bash
  uv run ci-tasks quality
  # add focused commands, e.g. uv run test-tasks unit, uv run lint-tasks ruff
  ```

- Coverage or benchmark deltas (if relevant):

## Risk & Rollout

- Risk level (low / medium / high):
- Mitigations / fallbacks:
- Monitoring or alerts to watch post-merge:

## Reviewer Guidance

Call out files or commits that deserve extra attention. Reference experiment configs, telemetry, or prior art to speed up review.

## Checklist

- [ ] I followed [`AUTHOR_GUIDELINES.md`](../.dev-guidelines/AUTHOR_GUIDELINES.md) including a self-review of the diff.
- [ ] All automation listed in **Testing & Validation** is linked or pasted, and failures (if any) are explained.
- [ ] Scope is a single logical change with paired tests and docs per [`DEVELOPMENT.md`](../.dev-guidelines/DEVELOPMENT.md).
- [ ] I updated configs/docs/telemetry where behavior changed and linked canonical sources (`DOCUMENTATION.md`).
- [ ] Reviewers can verify using [`CODE_REVIEW_CHECKLIST.md`](../.dev-guidelines/CODE_REVIEW_CHECKLIST.md).
