# Regression Tests

<details>
<summary>Related documentation</summary>

- [.dev-guidelines/TESTING.md](../../.dev-guidelines/TESTING.md) – Regression policy, gating rules, and fixture guidance.
- [.dev-guidelines/DOCUMENTATION.md](../../.dev-guidelines/DOCUMENTATION.md) – README format and folder tree standards.

</details>

## Purpose

Capture historical bugs with deterministic, focused tests to prevent reintroductions. Each regression test should:

- Reproduce a previously observed failure using minimal inputs.
- Assert the user-visible behavior or invariant that was broken.
- Document the original issue (link to ticket/PR when possible).

## When to Add a Regression Test

- A bug escapes existing unit/property/integration coverage.
- A reviewer requests explicit guardrails for a tricky edge case.
- A scenario is difficult to express as a pure unit test but still narrower than a full acceptance suite.

## How to Run

```bash
uv run pytest tests/regression -q -rA
```

Run alongside fast coverage gates:

```bash
uv run tools ci quality-gate  # delegates to pre-commit and includes regression suite
```

## Folder Structure

```bash
tests/regression/
├── README.md                 # this file
└── test_public_api_policy.py # sample regression enforcing public API contract
```

## Authoring Guidelines

- Keep tests isolated and fast; prefer in-memory fixtures or temporary directories.
- Reference the original bug in a comment at the top of the test.
- When possible, include both the failing setup and the fixed expectation in the same test to make intent obvious.
- Avoid duplicating coverage already provided by unit/property suites—regressions should be targeted.
