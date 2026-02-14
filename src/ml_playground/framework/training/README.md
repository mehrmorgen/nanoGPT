# Training Package

<details>
<summary>Related documentation</summary>

- [Framework Utilities](../../docs/framework_utilities.md) – Training configuration helpers referenced by this package.
- [.dev-guidelines/project-specific/DEVELOPMENT.md](../../.dev-guidelines/project-specific/DEVELOPMENT.md) – Training workflow and quality standards.

</details>

## Purpose

Training orchestration package providing complete training loop management, checkpointing, evaluation hooks, and LR
scheduling for machine learning models in `ml_playground`.

## Structure

- `loop/` - Core training loop orchestration and runner
- `checkpointing/` - Checkpoint save/load and management services
- `hooks/` - Training lifecycle hooks (evaluation, logging, model setup, data loading)

## Key APIs

- `Trainer` - Main training orchestrator class
- `run_training()` - Convenience helper for executing a `TrainingPlan`
- `TrainingPlan` / `TrainingSession` - Structured training entrypoints used by the CLI
- `create_manager()` - Checkpoint manager factory
- `save_checkpoint()` / `load_checkpoint()` - Checkpoint operations
- `ml_playground.framework.training.api` - high-level `TrainingPlan`/`TrainingSession` helpers that wrap the
  existing trainer for forward-only CLI usage.

## Usage Example

```python
from pathlib import Path

from ml_playground.framework.configuration.models import MetadataConfig, TrainerConfig
from ml_playground.framework.training.api import TrainingPlan, run_training

trainer_cfg = TrainerConfig(...)
metadata = MetadataConfig(
    experiment="my-exp",
    config_path=Path("configs/exp.toml"),
    project_home=Path("."),
    dataset_dir=Path("data"),
    train_out_dir=Path("runs/train"),
    sample_out_dir=Path("runs/sample"),
)
plan = TrainingPlan(config=trainer_cfg, metadata=metadata)
summary = run_training(plan)
```
