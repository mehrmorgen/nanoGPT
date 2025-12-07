# Runtime CLI

<details>
<summary>Related documentation</summary>

- [.dev-guidelines/DEVELOPMENT.md](../../../../.dev-guidelines/DEVELOPMENT.md) – Core development practices, CLI standards, and workflow.
- [.dev-guidelines/TESTING.md](../../../../.dev-guidelines/TESTING.md) – Testing standards, suite scopes, and E2E policies.
- [.dev-guidelines/DOCUMENTATION.md](../../../../.dev-guidelines/DOCUMENTATION.md) – README structure, abstraction levels, and folder tree standards.

</details>

## Purpose

High-level runtime CLI for running ML Playground experiments via TOML configs.

Provides a single entry point (exposed as `uv run ml-playground`) to:

- prepare experiment datasets
- train models
- sample from trained checkpoints
- run analysis flows

Experiments must not read TOML directly; configuration is loaded, validated, and injected by the runtime.

## Structure

```bash
src/ml_playground/runtime/cli/
├── README.md        # runtime CLI documentation (this file)
├── app.py           # Typer app definition and global options
├── main.py          # programmatic + console entry points
├── commands.py      # prepare / train / sample / analyze command callbacks
├── device.py        # device setup helpers for CLI runs
├── result.py        # ToolResult handling and run-or-exit helpers
├── runners.py       # orchestration helpers for CLI commands
└── typer_helpers.py # shared Typer argument/option helpers
```

## Usage

The runtime CLI is usually invoked via `uv`:

```bash
uv run ml-playground --help
uv run ml-playground prepare bundestag_char --exp-config path/to/config.toml
uv run ml-playground train bundestag_char --exp-config path/to/config.toml
uv run ml-playground sample bundestag_char --exp-config path/to/config.toml
uv run ml-playground analyze bundestag_char --exp-config path/to/config.toml
```

### Global options

- `--exp-config PATH` – experiment-specific TOML override; replaces the experiment's `config.toml` while still merging `default_config.toml` first.
- `--learning-mode / --no-learning-mode` – enable or disable educational explanations for ML workflow operations.
- `--verbosity, -v` – learning-mode verbosity: `0=minimal`, `1=standard`, `2=comprehensive`.

When run with no subcommand, the CLI prints a short welcome message and full help (due to `no_args_is_help=True`) and exits with code 0.

### Exit behaviour & message standards

The runtime CLI follows consistent patterns for user-facing messages and exit codes:

#### Exit codes
- `0` – Success (command completed successfully)
- `1` – General error (unexpected exceptions, keyboard interrupts, command failures)
- `2` – Configuration error (missing/invalid config files)

#### Message formatting
- **Success messages**: Written to stdout via `typer.echo(result.stdout)`
- **Error messages**: Written to stderr via `typer.echo(result.stderr, err=True)`
- **Learning mode**: Uses standardized emoji prefixes:
  - 📚 Learning Mode explanations
  - 💡 Best practices
  - 🔗 Related concepts

#### Error handling patterns
- Missing config file via `--exp-config` → logged error and exit code `2`
- Keyboard interrupt → "Operation cancelled by user" message and exit code `1`
- Command failures → error details to stderr and exit code from `ToolResult.exit_code`
- All commands use the same `handle_tool_result()` function for consistent behavior

#### Consistency with tools CLI
The runtime CLI follows the same patterns as the tools CLI:
- Shared `ToolResult` handling with stdout/stderr separation
- Learning mode formatting with consistent emoji prefixes
- Exit code conventions (0=success, 1=error, 2=config)
- Standardized error message propagation

## Related tests

Runtime CLI behaviour is guarded by:

- Acceptance tests: `tests/acceptance/features/runtime_cli.feature`
- E2E tests: `tests/e2e/test_cli_flow.py`

These suites verify help output, argument handling, and end-to-end `prepare -> train -> sample` flows on CPU.
