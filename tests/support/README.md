# Test Support Assets

<details>
<summary>Related documentation</summary>

- [.dev-guidelines/TESTING.md](../../.dev-guidelines/TESTING.md) – Fixture policies, shared asset expectations, and gating rules.
- [.dev-guidelines/DOCUMENTATION.md](../../.dev-guidelines/DOCUMENTATION.md) – README structure requirements and folder tree standards.

</details>

## Purpose

Central location for immutable fixtures and helpers shared across test suites. Use this directory for:

- Reusable configuration builders and fixture factories.
- Small static assets (JSON, TOML, text) referenced by multiple tests.
- Utilities that must be imported by both unit and higher-level suites without duplication.

## Guidelines

- Treat files as read-only during tests. If a test needs writable temp data, create it under `tmp_path` or another temporary directory.
- Keep assets minimal to avoid slowing down the repository checkout or test execution.
- Document new helpers with inline comments explaining primary consumers.
- Prefer utility functions over large sample datasets; generate data programmatically when feasible.

## Folder Structure

```bash
tests/support/
├── README.md          # this file
└── config_builders.py # shared factories for experiment/test configurations
```

## Notes

- Add new modules only when multiple suites require the helper; otherwise keep logic inside the nearest test package.
- When adding binary assets, explain their provenance and size rationale in the pull request description.
- Remember that pre-commit hooks exercise the full quality gate; keep support helpers deterministic and dependency-light.
