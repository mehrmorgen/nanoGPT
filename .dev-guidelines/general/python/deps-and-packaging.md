# Dependencies and Packaging

Neutral guidance on dependency management tools and packaging workflows, with comparisons and rationale for the choices reflected in this repo’s configuration.

## Comparison (summary)

| Tool                    | Lock fidelity            | Reproducibility           | Performance          | UX/ergonomics             | Ecosystem/maturity        | Notes                                                |
| ----------------------- | ------------------------ | ------------------------- | -------------------- | ------------------------- | ------------------------- | ---------------------------------------------------- |
| **uv**                  | Strong (lockfile)        | High                      | Fast installs/builds | Modern CLI, PEP 582-like  | Growing, backed by Astral | Current choice in this repo (aligns with pyproject). |
| pip + venv              | None by default          | Medium (requirements.txt) | Baseline             | Familiar, minimal         | Mature, ubiquitous        | Requires extra tooling for locking.                  |
| pip-tools (pip-compile) | Strong (generated locks) | High                      | Moderate             | CLI is simple; extra step | Mature for apps           | Good for deterministic pinning on pip stack.         |
| poetry                  | Lockfile                 | High                      | Moderate             | Rich CLI, TOML management | Mature, popular           | Manages virtualenvs; opinionated.                    |
| pdm                     | Lockfile                 | High                      | Moderate             | PEP 582 support, modern   | Growing                   | Similar goals to poetry with different UX.           |

## Our choice

- **uv** for installs, locking, and execution because it is fast, produces deterministic locks, and integrates cleanly with `pyproject.toml`.

## When to consider alternatives

- pip+pip-tools: if constrained to a pip-only environment or need minimal tooling changes.
- poetry/pdm: if you need built-in project scaffolding or PEP 582 workflows and the team already standardizes on them.
- plain pip+venv: smallest footprint for single-use scripts; accept weaker reproducibility.

## Practices

- Prefer lockfiles for all reproducible environments.
- Avoid optional dependencies at import time; gate extras via explicit groups/extras.
- Keep transitive pinning in lockfiles, not scattered in code.
- Ensure build backends remain centralized in `pyproject.toml`.
