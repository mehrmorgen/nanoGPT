# Tools Package

<details>
<summary>Related documentation</summary>

- [.dev-guidelines/project-specific/DOCUMENTATION.md](../../../.dev-guidelines/project-specific/DOCUMENTATION.md) – Folder-level README blueprint and abstraction policy.
- [.dev-guidelines/project-specific/DEVELOPMENT.md](../../../.dev-guidelines/project-specific/DEVELOPMENT.md) – Tooling workflow, quality gates, and CLI conventions.

</details>

## Purpose

Developer tooling that powers the `uv run tools ...` command surface. Provides Typer-based CLIs for environment setup, quality gates, CI workflows, review automation, and agentic helpers. These tools follow the project-wide development practices documented in `.dev-guidelines/project-specific/DEVELOPMENT.md` (strict typing, dependency injection over mocking, tests-first, coverage enforcement).

## Structure

```bash
src/ml_playground/tools/
├── README.md          # package overview (this file)
├── cli/               # Typer entrypoint + category command modules (main.py, commands/)
├── core/              # shared services (state mgmt, dependency injection, logging)
└── utils/             # helpers (subprocess, filesystem, serialization)
```

## Key Concepts

- **Command categories** (`src/ml_playground/tools/categories/`) expose focused Typer apps:
  - `env`: provisioning, verification, cache cleanup.
  - `quality`: lint/format bundles and targeted checks.
  - `test`: pytest orchestrations and coverage tasks.
  - `ci`: full quality gate execution (delegates to pre-commit).
  - `dev`: review automation, repo hygiene, port management.
  - `agentic`: AI-assisted batch operations and review helpers.
- **Core services** provide shared logging, configuration loading, and dependency wiring so categories stay thin.
- **Utilities** wrap subprocess execution, environment management, and path handling with consistent error reporting.
- **AI workflows**: use `uv run tools agentic ...` for batch review and learning-mode helpers; review AI-generated changes, keep type hints, and ensure tests/coverage pass.

## Usage

```bash
# discover available tool groups
uv run tools --help

# run the quality gate bundle (delegates to pre-commit)
uv run tools ci quality-gate

# verify local environment artifacts + required quality toolchain
uv run tools env verify

# list review comments needing replies
uv run tools dev review-list <pr_number> --unreplied --unresolved

# AI batch review (JSON output for downstream consumers)
uv run tools agentic batch-review --format json
```

The packaged CLI entrypoint is `ml_playground.tools.cli.main:main_entry`, wired via the
`tools` script in `pyproject.toml`.

## Implementation Notes

- Commands should depend on the shared services provided in `core/` to keep CLI handlers declarative.
- New categories must be registered inside `cli.py` and documented within their subdirectory README if additional context is required.
- Follow the centralized tokenizer and configuration protocols when tool commands reach into `ml_playground` modules (see `../../../docs/framework_utilities.md`).
