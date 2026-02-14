---
trigger: manual
description: Reviewer responsibilities and behaviors for empathetic, high-signal code reviews
---

# Code Review Reviewer Guidelines

> Adapted from Michael Lynch’s [How to Do Code Reviews Like a Human (Part One)](https://mtlynch.io/human-code-reviews-1/), [Part Two](https://mtlynch.io/human-code-reviews-2/), and Simon Tatham’s [Code Review Antipatterns](https://www.chiark.greenend.org.uk/~sgtatham/quasiblog/code-review-antipatterns/) (CC BY 4.0).

## Purpose

- Maintain quality while mentoring authors and reinforcing our collaborative culture.
- Provide predictable, empathetic review experiences that match `ml_playground` policies and automation.

## Approach Reviews with Empathy

- Lead with encouragement: acknowledge what’s working before diving into issues.
- Review the code, not the coder. Keep language objective and actionable.
- Ask questions when unsure. Prefer “What if we…?” over directives.
- Explain why: link standards, tests, or architecture references for every requested change.

## Focus on High-Value Feedback

- Prioritize correctness, security, performance, and maintainability.
- Mark blocking feedback with **`[blocking]`** so authors can sequence fixes; leave non-blocking notes as suggestions.
- Avoid nitpicks. If automation can catch it, ensure it’s in `uv run tools ci quality-gate` or `ruff` instead.
- Offer alternatives when pointing out problems; pair with code snippets or configuration examples where possible.

## Collaborate with Automation

- Trust CI results but verify discrepancies. Re-run targeted commands locally (`uv run tools test unit`, `uv run tools ci quality-gate`) when failures look environmental.
- Use `uv run python tools/review.py list --pr <number> --unreplied --unresolved` to stay on top of open threads.
- Encourage authors to add guardrails (tests, monitoring) when risky behavior is introduced.

## Keep Reviews Timely

- Aim to respond within one business day. If you need more time, leave a status update.
- Batch feedback to avoid drip comments. Use draft reviews for work-in-progress notes and finalize once ready.
- Hand off to another reviewer if you’re blocked or unavailable; note the context in the PR.

## Close the Loop

- Verify updates: pull the branch, inspect critical paths, and confirm CI passes after fixes.
- Celebrate improvements. Call out notable quality boosts or learnings in your final comment.
- Capture insights in retrospectives or docs when patterns emerge (e.g., recurring confusion around a module).

## References

- Author counterpart: [`AUTHOR_GUIDELINES.md`](./AUTHOR_GUIDELINES.md)
- Review checklist: [`CODE_REVIEW_CHECKLIST.md`](./CODE_REVIEW_CHECKLIST.md)
- Core policies: [`DEVELOPMENT.md`](./DEVELOPMENT.md), [`TESTING.md`](./TESTING.md), [`DOCUMENTATION.md`](./DOCUMENTATION.md)
