# Connect Four Sequence Modeling Experiment

Minimal experiment that teaches a GPT-style transformer to predict moves in Connect Four games generated via random self-play.

## Overview
- **Dataset**: Synthetic Connect Four matches generated on the fly via uniform random self-play (with optional horizontal mirroring).
- **Encoding**: Character-level tokenizer over board renderings, move annotations (`[MOVE:x]`), and outcomes (`[WIN:1]`, `[DRAW]`).
- **Method**: NanoGPT training loop configured through `config.toml`; dataset preparation performed through the experiment preparer.
- **Pipeline**: `prepare → train → sample` using the standard `ml_playground` CLI targets.

## Data
- **Raw inputs**: Generated into `datasets/games.txt` during `make prepare connect_four`.
- **Prepared outputs**:
  - `datasets/train.bin`
  - `datasets/val.bin`
  - `datasets/meta.pkl` (contains tokenizer metadata and dataset stats)

Each serialized game is stored as:
```
[START]
.|.|.|.|.|.|.
.|.|.|.|.|.|.
... board rows ...
[MOVE:3]
... updated board ...
[WIN:1]
```
Mirror augmentation doubles the dataset by reflecting boards and move indices.

## Method / Model
- Character-level tokenizer built from the prepared corpus.
- Transformer depth 4, width 128, context window 512 (see `config.toml`).
- Random self-play produces a diverse but noisy policy; future improvements can swap in stronger agents.
- Checkpoints rotate via the runtime config (`ckpt_last.pt`, `ckpt_best.pt`).

## Environment Setup

```bash
make setup
make verify
```

## How to Run

```bash
# Prepare dataset (writes datasets/*.bin + meta)
make prepare connect_four

# Train using the default experiment config
make train connect_four CONFIG=ml_playground/experiments/connect_four/config.toml

# Sample from the latest checkpoint
make sample connect_four CONFIG=ml_playground/experiments/connect_four/config.toml

# End-to-end loop (prepare + train + sample)
make loop connect_four CONFIG=ml_playground/experiments/connect_four/config.toml
```

For lightweight smoke tests and CI runs, use the reduced `test_config.toml` instead of the full config.

## Configuration Highlights
- `config.toml` – full training run (~20k iterations) with dropout and LR schedule.
- `test_config.toml` – tiny model for quick correctness checks.
- `preparer.py` exposes extras for custom datasets: `num_games`, `seed`, `augment`, and overrides for injecting precomputed game text.

## Outputs
- Datasets: `ml_playground/experiments/connect_four/datasets/`
- Training runs: respect the `out_dir` defined inside your chosen TOML config (default `./out/connect_four`).

## Troubleshooting
- Regenerate games: `make prepare connect_four FORCE_REGEN=1` (or pass `force_regen` through the CLI extras).
- Dataset too small? Increase `num_games` via `make prepare connect_four NUM_GAMES=50000` (exposed through preparer extras).
- Use the `seed` extra for deterministic dataset regeneration during tests.

## Notes / Future Work
- Replace random self-play with heuristic or MCTS-guided agents for higher quality trajectories.
- Add evaluation harnesses (e.g., pit the trained model against baselines) in a future sampler module.
- Explore multi-task heads (win prediction) to improve strategic awareness.
