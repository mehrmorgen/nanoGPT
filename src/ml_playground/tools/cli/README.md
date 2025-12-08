# Tools CLI

<details>
<summary>Related documentation</summary>

- [Documentation Guidelines](../../../../.dev-guidelines/DOCUMENTATION.md) – Folder-level README blueprint and abstraction policy.

</details>

## Purpose

Core CLI infrastructure for the `uv run tools` command, including entry point, dependency injection, and command registration.

## Structure

```bash
src/ml_playground/tools/cli/
├── README.md          # package documentation (this file)
├── main.py            # Typer application entry point
├── config_loader.py   # configuration loading logic
├── dependencies.py    # dependency injection setup
├── helpers.py         # shared CLI helpers
├── state.py           # global CLI state management
└── commands/          # command category implementations
    ├── quality.py     # code quality tools (format, lint, typecheck)
    ├── testing.py     # test runners (unit, integration, e2e, coverage)
    ├── environment.py # environment management (setup, clean, info)
    ├── ci.py          # CI/CD utilities (quality gates, badges)
    ├── dev.py         # development helpers (bootstrap, serve)
    ├── analysis.py    # analysis tools (profiling, metrics)
    └── learn.py       # learning mode helpers (explain, best-practices)
```

## Usage

### Basic command structure
```bash
uv run tools [GLOBAL_OPTIONS] <CATEGORY> <COMMAND> [COMMAND_OPTIONS]
```

### Global options & top-level behavior
- `--exp-config PATH`: Path to experiment configuration file
- `--learning-mode`: Enable learning mode with contextual help
- `--verbosity {0,1,2}`: Output verbosity level (0=minimal, 1=standard, 2=comprehensive)
- `--dry-run`: Show what would be executed without running
- `--project-root PATH`: Project root directory (auto-detected if not specified)

Top-level behavior:
- Calling `uv run tools` with no subcommand prints a short welcome message and
  full help for the tools CLI and exits with code `2` (usage error).
- Calling `uv run tools` with global options but no subcommand (for example,
  `uv run tools --dry-run`) shows the same friendly welcome + full help and
  exits with code `2`.
- Commands that expect positional arguments (for example,
  `uv run tools learn explain <category.command>`) will print an error about the
  missing argument followed by full help for that command and exit with code
  `2` when the argument is omitted.

### Common usage examples
```bash
# Run all quality checks
uv run tools quality all

# Run unit tests with coverage
uv run tools test coverage

# Set up development environment
uv run tools env setup

# Get help with learning mode
uv run tools --learning-mode quality format

# Check what would be cleaned (dry run)
uv run tools --dry-run env clean

# Run specific test suite with verbose output
uv run tools --verbosity 2 test e2e
```

## Learning mode helpers

The tools CLI includes learning mode to help understand each command:
- `uv run tools --learning-mode`: Enable learning mode globally
- `uv run tools learn commands`: List all available commands with descriptions
- `uv run tools learn explain <command>`: Get detailed explanation of a specific command
- `uv run tools learn best-practices`: Show general and category-specific best practices

Learning mode provides contextual information about:
- What each command does and when to use it
- Expected inputs and outputs
- Common pitfalls and troubleshooting
- Related commands and workflows

## Exit behavior

- **Success (0)**: Command completed successfully
- **Error (1)**: Command failed due to validation, configuration, or execution errors
- **Usage (2)**: Invalid command-line arguments, missing required options or
  positional arguments, or invoking the CLI without a subcommand.

All errors are printed to stderr with clear, actionable messages. In learning mode, additional context is provided for error conditions.

## Related tests

- Unit tests: `tests/unit/ml_playground/tools/cli/`
- Integration tests: `tests/integration/ml_playground/tools/cli/`
- E2E tests: `tests/e2e/test_cli_flow.py` (covers tools CLI workflows)
- Acceptance tests: `tests/acceptance/features/tools_cli.feature`

Run tests with:
```bash
# Unit tests
uv run tools test unit

# Integration tests  
uv run tools test integration

# Full test suite
uv run tools test all
```
