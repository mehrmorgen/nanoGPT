# Acceptance Tests

<details>
<summary>Related documentation</summary>

- [.dev-guidelines/project-specific/TESTING.md](../../.dev-guidelines/project-specific/TESTING.md) – Suite scopes, gating rules, and fixture policies.
- [.dev-guidelines/project-specific/DOCUMENTATION.md](../../.dev-guidelines/project-specific/DOCUMENTATION.md) – README structure requirements and folder tree standards.

</details>

## Purpose

Validate repository-level policies, workflow enforcement, and end-to-end behaviors through the public CLI. Acceptance tests exercise realistic scenarios that combine multiple subsystems while remaining deterministic and fast.

## Principles

- Cover policy enforcement (e.g., CLI workflow guards, commit hygiene) rather than low-level logic.
- Use only public entry points and Typer commands; no private helpers.
- Keep tests deterministic: seed randomness, avoid network access, and confine filesystem writes to temporary directories.
- Prefer scenario-style assertions that describe observable behavior from the user’s perspective.

## How to Run

```bash
uv run tools test acceptance
```

Run a single feature module:

```bash
uv run pytest tests/acceptance/steps/test_<feature>.py
```

## Folder Structure

```bash
tests/acceptance/
├── README.md                    # this file
├── conftest.py                  # suite-specific fixtures (CLI runner, temp workspaces)
├── features/                    # Gherkin-style feature definitions (pytest-bdd)
│   ├── checkpointing.feature
│   ├── runtime_cli.feature
│   └── tools_cli.feature
├── steps/                       # step implementations binding features to CLI calls
│   ├── test_checkpointing_steps.py
│   └── test_tools_cli_steps.py
└── tools/                       # direct CLI acceptance tests
    ├── test_installation_smoke.py
    └── cli/
        ├── _helpers.py
        ├── conftest.py
        └── test_learn_commands.py
```

## Notes

- Keep feature files concise and scenario-driven. Reuse shared step definitions where possible.
- Ensure every new scenario documents the policy or workflow it protects so future contributors understand the intent.
- Run the full acceptance suite before merging changes that modify CLI workflows or developer tooling.
