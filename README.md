# ml-playground: strict, typed, UV-only training/sampling module

![Line Coverage](docs/assets/coverage-lines.svg) ![Branch Coverage](docs/assets/coverage-branches.svg)

This module provides a single, one-way interface to prepare data, train, and
sample. It is CPU/MPS-friendly, strictly typed, and uses TOML configs.

- Developer Guidelines: see `.dev-guidelines/README.md` for setup,
  development workflow, and policies.
- Development tools: use `uv run tools --help` for integrated development
  tooling.
- Coverage badges: `uv run tools ci coverage-badge` (integrated directly)

AI Development Guidelines

- Use `uv run tools env ai-guidelines <tool_name>` to set up AI guidelines
  symlinks.
- The previous `scripts/setup_ai_guidelines.py` has been integrated and is no
  longer needed.

## Documentation abstraction policy

- Top-level docs are high-level and describe the why and the overall layout.
- Each subfolder contains its own `README.md` with a focused scope and a folder tree.

## Repository structure (high-level)

```bash
.
├── src/
│   └── ml_playground/         # core module (configs, experiments, runtime code)
│       ├── analysis/          # analysis tools (e.g., LIT integration)
│       ├── datasets/          # optional package; experiments can run without it
│       ├── experiments/       # self-contained experiments (mid-level docs)
│       └── configs/           # example configs referenced by docs/CLIs
├── tests/                     # test suite (see per-folder README for scope)
│   ├── unit/                  # low-level API tests
│   ├── integration/           # multi-module tests via Python APIs
│   ├── e2e/                   # CLI-level smoke tests
│   └── acceptance/            # higher-level behaviors and policies
├── scripts/                   # specialized utility scripts
│   └── llama_cpp/             # GGUF model conversion utilities
├── docs/                      # supplementary docs (framework utilities, LIT, etc.)
├── pyproject.toml             # strict typing/linting/testing configuration
└── README.md                  # this file (top-level, high abstraction)
```

- Quality tooling is mandatory before commit (ruff, mypy, BasedPyright), and
  tests must pass.
- Linear history for own work: rebase your branches and avoid merge commits;
  fast-forward only. See `.dev-guidelines/README.md` for developer policies.
- Test-Driven Development (TDD) is required for functional changes: write a
  failing test, implement minimal code to pass, then refactor.
- Code reviews follow `.dev-guidelines/project-specific/AUTHOR_GUIDELINES.md`,
  `.dev-guidelines/project-specific/REVIEWER_GUIDELINES.md`, and the shared
  `.dev-guidelines/project-specific/CODE_REVIEW_CHECKLIST.md`, which define author preparation
  steps, reviewer expectations, and a shared quality checklist.
- Granular commits are required. Each functional/behavioral change MUST pair
  its production code with the corresponding tests in the same commit
  (unit/integration). Exceptions: documentation-only, test-only refactors, and
  mechanical formatting.
- **Review management**: `uv run tools dev review-*` (was `scripts/review.py`).
  For triage, use
  `uv run tools dev review-list <pr_number> --unreplied --unresolved`; for
  bulk replies,
  `uv run tools dev review-bulk-reply <pr_number> --replies replies.json`; to
  delete comments,
  `uv run tools dev review-delete <pr_number> --comments delete.json`.

Setup and Developer Workflow

- See `.dev-guidelines/README.md` for environment setup, development
  practices, and testing policies (entry point to all developer guidelines).

Nix + direnv (optional, recommended for reproducible CLI environments)

- Enter the dev shell with: `nix develop`
- Enable auto-loading with direnv once per clone:
  - `direnv allow`
- The configured shell exports deterministic UV/cache environment variables and
  bootstraps `.venv` on first entry (`uv sync --group all` + `uv pip install -e .`).

Datasets

- Shakespeare (GPT-2 BPE; prepared via internal
  `ml_playground.experiments.shakespeare`).
- Bundestag (char-level; prepared via internal
  `ml_playground.experiments.bundestag_char`; requires a user-provided text at
  `src/ml_playground/experiments/bundestag_char/datasets/input.txt`).
- Bundestag (tiktoken BPE; prepared via internal
  `ml_playground.experiments.bundestag_tiktoken`).

Workflows (high-level)

- Prepare/train/sample workflows are driven by the built-in Typer CLI:
  `uv run cli <command>`. For exact commands, refer to each experiment's
  `README.md` and `.dev-guidelines/README.md`.
- Universal meta policy: the data directory must contain a `meta.pkl` file used
  by training and sampling. The `prepare` step is responsible for writing
  `meta.pkl`.

Notes

- Configuration is defined via TOML dataclasses under `src/ml_playground/configuration/`.
- CPU/MPS are first-class. CUDA may be selected in TOML if available.
- Checkpoint behavior and policies are described in `.dev-guidelines/README.md`.
- For framework utilities, see [Framework Utilities Documentation](docs/framework_utilities.md).
- CLI validations: train and sample commands now fail fast if `meta.pkl` is missing.

Mutation testing

- See `.dev-guidelines/README.md` for how to run optional mutation testing
  (Cosmic Ray).

TensorBoard (auto-enabled)

- Training logs to TensorBoard. See `.dev-guidelines/README.md` for commands.

GGUF export (vendor approach)

- See `scripts/llama_cpp/README.md` for the exact steps.

Testing

- See `.dev-guidelines/README.md` for testing standards and gates.
- See `tests/*/README.md` for folder-specific scope and patterns.
