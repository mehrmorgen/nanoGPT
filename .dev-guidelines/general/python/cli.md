# CLI Frameworks

Comparison of CLI frameworks with rationale for the choice reflected in this repo’s configuration.

## Comparison (summary)

| Framework         | Typing support                    | Ergonomics                 | Subcommands/nesting | Completion/docs             | Async support          | Ecosystem/maturity                | Notes                                   |
| ----------------- | --------------------------------- | -------------------------- | ------------------- | --------------------------- | ---------------------- | --------------------------------- | --------------------------------------- |
| **Typer**         | Strong (type hints drive parsing) | Very concise, FastAPI-like | Yes                 | Shell completion, rich help | Yes                    | Mature, backed by FastAPI authors | Current choice in this repo.            |
| Click             | Good via decorators/type params   | Verbose compared to Typer  | Yes                 | Completion, good help       | Limited async patterns | Very mature, large ecosystem      | Stable baseline; more boilerplate.      |
| argparse (stdlib) | Minimal                           | Verbose                    | Limited nesting     | Basic help only             | No                     | Stable                            | Good for zero deps, but low ergonomics. |

## Our choice

- **Typer** for primary CLIs: concise, type-driven parsing, good UX/help, completion built-in.

## When to consider alternatives

- Click: when you need maximum stability and broad plugin patterns without Typer abstractions.
- argparse: for minimal one-off scripts or environments forbidding third-party deps.

## Practices

- Keep CLI layers thin; push logic into library modules.
- Leverage type hints for validation; avoid custom parsing when built-ins suffice.
- Provide completion scripts and clear `--help` examples.
