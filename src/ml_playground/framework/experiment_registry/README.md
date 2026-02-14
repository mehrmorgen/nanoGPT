# Experiment Registry

## Purpose

Framework-owned registry and protocol contracts for experiments. Provides:

- Protocols for preparers/trainers/samplers.
- Registry loading for experiment preparers.
- Extras model registration/loading for experiment-specific config sections.

## Dependency Rules

- May import framework modules and `ml_playground.experiments` as data locations for discovery only.
- Must not import `ml_playground.tools`.

## Public API

- `registry.load_preparers()`
- `extras_registry.register_extras_model()`
- `extras_registry.get_extras_model()`
- `extras_registry.load_extras_models()`
- Protocols: `Preparer`, `Trainer`, `Sampler`, `ExperimentIntegration`
