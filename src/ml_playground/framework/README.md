# Framework

## Purpose

Shared runtime, configuration, and core logic that is safe to import from both tools and experiments. This layer must not depend on tools or experiments.

## Dependency Rules

- May import framework-only modules, stdlib, and third-party packages.
- Must not import `ml_playground.tools` or `ml_playground.experiments`.

## Public API

- See subpackage READMEs for entrypoints and contracts.
