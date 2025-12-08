# ml-playground: strict, typed, UV-only training/sampling module

![Line Coverage](docs/assets/coverage-lines.svg)

## Related documentation

- [Developer Guidelines](.dev-guidelines/README.md) – Entry point for setup,
  development workflow, and policies.
- [Documentation Guidelines](.dev-guidelines/DOCUMENTATION.md) – Standards for
  docs structure and formatting.

This module provides a single, one-way interface to prepare data, train,
and sample. It is CPU/MPS-friendly, strictly typed, and uses TOML configs.

## Documentation abstraction policy

- Top-level docs are high-level and describe the why and the overall layout.
- Each subfolder contains its own `README.md` with a focused scope and a folder tree.

## Repository structure (high-level)

```bash
.
├── src/
│   └── ml_playground/         # core module (configs, experiments, runtime code)
│       ├── analysis/          # analysis tools (e.g., LIT integration)
│       ├── experiments/       # self-contained experiments (mid-level docs)
│       └── tools/             # developer tooling (quality, ci, dev, env)
├── tests/                     # test suite (see per-folder README for scope)
├── scripts/                   # specialized utility scripts
├── docs/                      # supplementary docs (framework utilities, LIT, etc.)
├── pyproject.toml             # strict typing/linting/testing configuration
└── README.md                  # this file (top-level, high abstraction)
```

## Quick Start

```bash
# Setup environment
uv run tools env setup

# Verify environment
uv run tools env verify

# Run quality gate
uv run tools ci quality-gate
```

## Developer Workflow

See [.dev-guidelines/README.md](.dev-guidelines/README.md) for comprehensive
details on:

- Quality tooling (mandatory before commit)
- TDD and commit policies
- Review management (`uv run tools dev review-*`)

## Workflows (high-level)

- Prepare/train/sample workflows are driven by the built-in Typer CLI:
  `uv run cli <command>`.
- Refer to each experiment's `README.md` for specific instructions.

## Notes

- **Configuration**: Defined via TOML dataclasses under
  `src/ml_playground/configuration/`.
- **Framework Utilities**:
  See [docs/framework_utilities.md](docs/framework_utilities.md).
- **Mutation Testing**:
  See `.dev-guidelines/README.md`.
