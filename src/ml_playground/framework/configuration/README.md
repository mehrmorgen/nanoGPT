# Configuration Package

<details>
<summary>Related documentation</summary>

- [Framework Utilities](../../docs/framework_utilities.md) – Configuration helpers shared across experiments.
- [.dev-guidelines/project-specific/DEVELOPMENT.md](../../.dev-guidelines/project-specific/DEVELOPMENT.md) – Configuration policies and quality standards.

</details>

## Purpose

Configuration management utilities for `ml_playground`. Provides Pydantic models and loading helpers for experiment
configs.

## Structure

- `models.py` - Pydantic configuration models
- `loading.py` - TOML loading and deep merge helpers
- `cli.py` - CLI configuration adapters and helpers
- `merge_utils.py` - Dictionary merging logic

## Key APIs

- `ExperimentConfig` - Complete configuration tree
- `TrainerConfig` - Training-specific configuration
- `load_full_experiment_config()` - Load and validate experiment configuration (low-level)
- `cli.load_experiment()` - Load configuration for CLI (high-level)

## Extras Configuration

The `extras` field (available in most config sections) is a flexible dictionary for experiment-specific settings.
It is treated as a pass-through by the framework:
- It must be a dictionary (mapping).
- Framework validation does not enforce schemas on `extras` content.
- Experiment-specific code (e.g., in `extras_registry`) can perform its own validation if needed.

## Usage Example

```python
from ml_playground.framework.configuration.loading import load_full_experiment_config

config = load_full_experiment_config(config_path, project_home, experiment_name)
```
