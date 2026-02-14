# Configuration and Validation

Comparison of configuration/validation approaches and rationale for the choice reflected in this repo’s configuration.

## Comparison (summary)

| Approach                          | Validation strictness                             | Type fidelity          | Error clarity | Performance | Ecosystem/maturity | Notes                                                 |
| --------------------------------- | ------------------------------------------------- | ---------------------- | ------------- | ----------- | ------------------ | ----------------------------------------------------- |
| **Pydantic + TOML**               | Strong (`extra="forbid"`, type coercion controls) | High                   | Descriptive   | Good        | Mature             | Current choice in this repo.                          |
| dataclasses + manual validation   | None by default                                   | Depends on custom code | Depends       | N/A         | Stdlib             | Lightweight but easy to drift.                        |
| attrs + manual/validators         | Good with validators                              | Good                   | Good          | Good        | Mature             | Requires consistent validator discipline.             |
| Marshmallow                       | Strong schema-based                               | Good                   | Good          | Moderate    | Mature             | Another schema framework; more boilerplate.           |
| JSON/YAML loaders + ad-hoc checks | Weak                                              | Weak                   | Poor          | N/A         | Common             | Minimally viable; not recommended for strict configs. |

## Our choice

- **Pydantic + TOML**: strict schemas, helpful errors, path handling, and alignment with `pyproject.toml` ecosystem.

## When to consider alternatives

- dataclasses/attrs: ultra-lightweight configs or where Pydantic is not allowed; accept more manual checks.
- Marshmallow: if a team standardizes on Marshmallow schemas and tooling.
- Plain JSON/YAML loaders: only for trivial scripts where strict validation is unnecessary.

## Practices

- Keep configuration as single source (TOML + typed models).
- Forbid extras and prefer explicit defaults; fail fast on unknown keys.
- Resolve paths deterministically; avoid cwd-dependent resolution.
- Gate environment overrides explicitly and re-validate after merging.
