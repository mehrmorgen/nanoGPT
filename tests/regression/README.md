# Regression Tests

<details>
<summary>Related documentation</summary>

- [.dev-guidelines/project-specific/TESTING.md](../../.dev-guidelines/project-specific/TESTING.md) – Regression policy, gating rules, and fixture guidance.
- [.dev-guidelines/project-specific/DOCUMENTATION.md](../../.dev-guidelines/project-specific/DOCUMENTATION.md) – README format and folder tree standards.

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
├── README.md                              # this file
├── test_cache_directory_policy.py         # ensures cache dirs follow conventions
├── test_cli_exit_code_consistency.py      # guards against raw sys.exit() usage
├── test_cli_isolation.py                  # prohibited cross-imports between CLI layers
├── test_import_boundaries.py              # framework/tools/experiments layering
├── test_namespace_compliance.py           # PEP 420 namespace enforcement
├── test_no_mocks.py                       # forbids mocking/patching libraries
├── test_no_rogue_configs.py               # forbids standalone config files outside pyproject.toml
├── test_public_api_policy.py              # enforces public API contract and layering
├── test_repo_hygiene_policy.py            # repository-wide code hygiene checks
├── test_sampling_regressions.py           # sampling-specific regression guards
├── test_side_effect_boundaries_policy.py  # side-effect containment enforcement
└── tools/
    └── cli/
        ├── test_cli_exit_code_consistency_tools.py  # tools CLI exit code guards
        └── test_no_mocks_tools.py                   # tools-specific mock prohibition
```

## Authoring Guidelines

- Keep tests isolated and fast; prefer in-memory fixtures or temporary directories.
- Reference the original bug in a comment at the top of the test.
- When possible, include both the failing setup and the fixed expectation in the same test to make intent obvious.
- Avoid duplicating coverage already provided by unit/property suites—regressions should be targeted.
