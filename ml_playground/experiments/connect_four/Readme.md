# Connect Four Experiment

Train a small GPT model to predict Connect Four moves from randomly generated game states.

## Overview
- **Dataset**: Synthetic Connect Four games generated with random valid moves.
- **Encoding/Tokenizer**: Custom integer vocabulary (board cells + separator + move tokens).
- **Method**: NanoGPT-style training using the shared ml_playground training loop.
- **Pipeline**: `prepare` → `train` → `sample` via the `ml_playground` CLI.

## Data
- **Inputs (raw)**: None. Games are generated procedurally during `prepare`.
- **Outputs (prepared)**:
  - `train.bin`: Flattened token stream of training sequences.
  - `val.bin`: Validation token stream.
  - `meta.pkl`: Metadata describing the vocabulary and dataset statistics.

Each training example encodes a 6×7 board (42 tokens), a separator token, and the next move token.

## Method/Model
- **Tokenization**: Integers `0`, `1`, and `2` encode empty cells, the current player, and the opponent respectively. Columns `0-6` map to move tokens `3-9`, and `10` is a separator token between the board state and the move target.
- **Model**: A compact GPT configured via [`config.toml`](./config.toml) with `block_size = 44` to cover one encoded position.

## Environment Setup (preferred)

```bash
make setup
make verify
```

## How to Run

- **Config path**: `ml_playground/experiments/connect_four/config.toml`

### Prepare dataset
```bash
make prepare connect_four
```

### Train
```bash
make train connect_four CONFIG=ml_playground/experiments/connect_four/config.toml
```

### Sample
```bash
make sample connect_four CONFIG=ml_playground/experiments/connect_four/config.toml
```

### End-to-end loop
```bash
make loop connect_four CONFIG=ml_playground/experiments/connect_four/config.toml
```

## Configuration
- The default configuration generates 10,000 synthetic games with a 90/10 train/val split.
- Adjust dataset size and randomness via `[prepare.extras]` in [`config.toml`](./config.toml).

## Outputs
- **Data artifacts**: `ml_playground/experiments/connect_four/datasets/`
- **Training artifacts**: `out/connect_four/` (checkpoints, logs).

## Notes
- The preparer is auto-discovered by the experiment registry. No manual registry updates are required.
- Re-run `make prepare connect_four` with `CONFIG=...` to regenerate data; pass `prepare.extras.force_rebuild = true` to overwrite existing artifacts.
