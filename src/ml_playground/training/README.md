# Training Package

<details>
<summary>Related documentation</summary>

- [Framework Utilities](../../docs/framework_utilities.md) – Training configuration helpers referenced by this package.
- [.dev-guidelines/DEVELOPMENT.md](../../.dev-guidelines/DEVELOPMENT.md) – Training workflow and quality standards.

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
- `train()` - Functional training interface
- `create_manager()` - Checkpoint manager factory
- `save_checkpoint()` / `load_checkpoint()` - Checkpoint operations

## Usage Example

```python
from ml_playground.training.loop.runner import Trainer
from ml_playground.configuration.models import TrainerConfig

trainer = Trainer(config, shared_config)
final_iter, best_loss = trainer.run()
```
