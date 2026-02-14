# General Python Guidance

Neutral best practices and selection criteria for Python projects. Use this as a starting point for new work and to understand which alternatives were considered and why.

## How to use this pack

- Start here to understand the criteria we prioritize (typing, reproducibility, ergonomics, ecosystem, performance).
- Each sibling doc provides comparison tables, our current choice, rationale, and when to consider an alternative.
- Keep this pack tool- and project-neutral: no project-specific commands.

## Criteria we prioritize

- Reproducibility: lock fidelity, deterministic installs, isolation.
- Typing: static coverage, ergonomics, ecosystem support.
- Developer ergonomics: CLI UX, readability, boilerplate, docs.
- Performance: startup/runtime cost, cache behavior.
- Ecosystem/maturity: maintenance, community, integrations.
- Determinism in tests and tooling: isolated, side-effect free, seeded randomness.

## Documents in this pack

- `deps-and-packaging.md`
- `cli.md`
- `config.md`
- `testing.md`
- `lint-and-type.md`
- `docs-formatting.md`
- `mutation-testing.md`

## Scope constraint

Documentation-only: this pack does not prescribe project code changes; it records choices and alternatives.
