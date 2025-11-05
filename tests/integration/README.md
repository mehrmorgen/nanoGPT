# Integration Tests

<details>
<summary>Related documentation</summary>

- [.dev-guidelines/TESTING.md](../../.dev-guidelines/TESTING.md) – Integration suite scope, gating rules, and fixture policies.
- [.dev-guidelines/DOCUMENTATION.md](../../.dev-guidelines/DOCUMENTATION.md) – README structure requirements and folder tree standards.

</details>

Integration tests verify that multiple components work together correctly via Python APIs (not necessarily the CLI).
They use real code paths and small in-memory or tiny on-disk data.

## Principles

- Fast and deterministic.
- Minimal I/O; no network.
- Avoid side effects outside temp dirs.
- No test-only code paths in production.

## Run Locally

- Run all integration tests: `uv run test-tasks integration`
- Single file: `uv run pytest tests/integration/test_*.py`

## Folder structure

```bash
tests/integration/
├── README.md                    # scope and patterns for integration tests
├── conftest.py                  # integration pytest setup/markers
├── test_datasets_shakespeare.py # integration of dataset helpers
└── test_speakger_pilot.py       # integration around SpeakGer pipeline
```
