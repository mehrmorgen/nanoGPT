# Linting, Formatting, and Typing

Comparison of lint/format/type tools and rationale for the choices reflected in this repo’s configuration.

## Lint/Format comparison

| Tools                  | Coverage                     | Speed     | Config burden               | Ecosystem/maturity | Notes                            |
| ---------------------- | ---------------------------- | --------- | --------------------------- | ------------------ | -------------------------------- |
| **ruff + ruff-format** | Lint + format + import rules | Very fast | Low (one tool)              | Mature, active     | Current choice in this repo.     |
| black + isort + flake8 | Format + imports + lint      | Moderate  | Higher (multi-tool configs) | Very mature        | Stable trio; more moving parts.  |
| pylint                 | Lint + style                 | Slower    | Higher                      | Mature             | Detailed checks, heavier output. |

## Typing comparison

| Tools       | Coverage | Speed    | Ecosystem/maturity       | Notes                        |
| ----------- | -------- | -------- | ------------------------ | ---------------------------- |
| **pyright** | Strong   | Fast     | Mature, Microsoft-backed | Current choice in this repo. |
| **mypy**    | Strong   | Moderate | Mature, large ecosystem  | Also used in this repo.      |
| pytype      | Strong   | Moderate | Smaller ecosystem        | Alternative static checker.  |

## Our choice

- **ruff + ruff-format** for lint/format due to speed and single-tool configuration.
- **pyright + mypy** for static typing to cross-validate type coverage.

## When to consider alternatives

- black/isort/flake8: teams already standardized there; accept more configs/tooling.
- pylint: when you need deeper style checks and are comfortable with slower runs.
- pytype: specific environments that prefer its inference model; smaller ecosystem.

## Practices

- Keep import hygiene strict (no star imports, absolute imports, side-effect-free modules).
- Run type checkers regularly; gate on zero `Any` creep where possible.
- Keep tooling config centralized in `pyproject.toml` where supported.
- Avoid local/distributed config drift; prefer repo-wide settings.
