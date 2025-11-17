# Vier Gewinnt (Connect Four) Experiment

This experiment trains a nano-scale GPT model on a synthetically generated dataset of Connect Four games.

<details>
<summary>Related documentation</summary>

- [Experiment Overview](../../Readme.md) – General conventions for all experiments.
- [Framework Utilities](../../../../docs/framework_utilities.md) – Shared infrastructure for configuration, datasets, and runtime helpers.

</details>

## Folder Structure

```bash
vier_gewinnt/
├── Readme.md        # Experiment documentation (this file)
├── config.toml      # Configuration for the experiment
├── preparer.py      # Data preparation script
├── sampler.py       # Sampling script
├── trainer.py       # Training script
└── datasets/        # Prepared dataset artifacts
```

## Usage

1. **Prepare the dataset:**

   ```bash
   uv run python -m ml_playground.cli prepare vier_gewinnt
   ```

1. **Train the model:**

   ```bash
   uv run python -m ml_playground.cli train vier_gewinnt
   ```

1. **Sample from the model:**

   ```bash
   uv run python -m ml_playground.cli sample vier_gewinnt
   ```
