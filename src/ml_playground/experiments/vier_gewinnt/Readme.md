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
   uv run cli prepare vier_gewinnt --exp-config src/ml_playground/experiments/vier_gewinnt/config.easy.toml
   ```

1. **Train:**

   ```bash
   uv run cli train vier_gewinnt --exp-config src/ml_playground/experiments/vier_gewinnt/config.easy.toml
   ```

1. **Sample:**

   ```bash
   uv run cli sample vier_gewinnt --exp-config src/ml_playground/experiments/vier_gewinnt/config.easy.toml
   ```

## Playing the game locally

- As a script:

  ```bash
  uv run python src/ml_playground/experiments/vier_gewinnt/play.py --player1 easy --player2 hard
  ```

- As a module (preferred for PYTHONPATH):

  ```bash
  uv run python -m ml_playground.experiments.vier_gewinnt.play --player1 easy --player2 hard
  ```

Player types: `human`, `easy` (random), `medium` (heuristic), `hard` (minimax), plus `easy_ai` / `medium_ai` / `hard_ai` to use sampler checkpoints.

## Configuration files

- `config.toml`: Default configuration shared across training and sampling.
- `config.easy.toml`, `config.medium.toml`, `config.hard.toml`: Difficulty-specific overrides.
- `test_config.toml`: Lightweight config used in tests.

## Player Algorithms

The experiment includes several AI implementations for data generation and interactive play:

### Random Player

- **Algorithm**: Uniformly selects a random column from the set of currently valid moves.
- **Usage**: Serves as a baseline or for generating high-variance noise in datasets.

### Heuristic Player

- **Algorithm**: Rule-based decision making with the following priority:
  1. **Win Immediately**: If a move results in a win, take it.
  1. **Block Opponent**: If the opponent can win on their next turn, block that column.
  1. **Center Preference**: If no critical moves exist, prioritize columns in the order `[3, 2, 4, 1, 5, 0, 6]` (center-out).
  1. **Fallback**: If preferred columns are full, choose randomly among valid moves.

### Minimax Player

- **Algorithm**: Depth-limited Minimax search (default depth=4) with Alpha-Beta pruning.
- **Evaluation Function**:
  - **Terminal States**: +/- 100,000 points for a win/loss.
  - **Positional Scoring**:
    - **Center Control**: +3 points for each token in the center column.
    - **Window Scoring** (evaluating 4-token windows):
      - 4-in-a-row (Self): +100 points
      - 3-in-a-row (Self): +5 points
      - 2-in-a-row (Self): +2 points
      - 3-in-a-row (Opponent): -4 points (penalty for leaving threats open)
