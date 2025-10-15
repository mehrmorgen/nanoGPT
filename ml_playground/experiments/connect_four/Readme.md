# Connect Four Synthetic Games

Minimal end-to-end workflow that synthesizes Connect Four self-play games and trains
a small character-level Transformer to predict the next move.

## Overview
- Dataset: Synthetic Connect Four matches generated via random self-play (no external downloads)
- Encoding/Tokenizer: Character-level tokenizer with custom alphabet (`., X, O, digits, newline`)
- Method: nanoGPT-style Transformer trained on board-state + move pairs
- Pipeline: `prepare` → `train` → `sample` via the strict `ml_playground` CLI

## Data
- Inputs (raw): None required — data is generated on the fly during preparation
- Outputs (prepared):
  - `train.bin`, `val.bin`, `meta.pkl` under `ml_playground/experiments/connect_four/datasets/`
  - Metadata records the alphabet, board geometry, and total number of examples

## Method/Model
- Tokenization: character-level (each example encodes a 6×7 board, the chosen column `0-6`, and a newline)
- Model: lightweight GPT configured via `config.toml` (`n_layer=4`, `n_head=4`, `n_embd=128`, `block_size=128`)
- Checkpoints: saved in `out/connect_four` as `ckpt_last.pt` (and optionally `ckpt_best.pt`)
- Logging: TensorBoard summaries written to `out/connect_four/logs/tb`

## Environment Setup (preferred)

```bash
make setup
make verify
```

## How to Run

- Config path: `ml_playground/experiments/connect_four/config.toml`

Prepare dataset:

```bash
make prepare connect_four
```

Train:

```bash
make train connect_four CONFIG=ml_playground/experiments/connect_four/config.toml
```

Sample (play against the trained model):

```bash
make sample connect_four CONFIG=ml_playground/experiments/connect_four/config.toml
```

End-to-end loop:

```bash
make loop connect_four CONFIG=ml_playground/experiments/connect_four/config.toml
```

## Configuration

- `prepare.extras.num_games` controls how many self-play games are generated (default: 2000)
- `prepare.extras.train_split` sets the fraction of tokens routed to the training split
- Training hyperparameters mirror the tiny GPT baseline and can be tweaked for faster runs
- Sampling defaults read the best checkpoint and generate three short continuations

### Play interactively

The sampler loads the best checkpoint and runs a playable Connect Four match.
By default you control **X** (first player) and supply moves interactively when
prompted. You can also script moves for automated tests by setting
`sample.extras.human_moves` (a list of column indices) or switch sides via
`sample.extras.human_player = "O"`. The model selects legal moves using the
configured sampling policy (`sample.extras.policy` accepts `"greedy"` to force
argmax behaviour).

Example scripted session:

```toml
[sample.extras]
human_player = "O"
human_moves = [4, 4, 4, 4]
policy = "greedy"
```

## Outputs

- Data artifacts: `ml_playground/experiments/connect_four/datasets/`
- Training artifacts: `ml_playground/experiments/connect_four/out/` (checkpoints, logs)
- Sampling artifacts: written under the same `out/connect_four` directory

## Troubleshooting

- Empty dataset: ensure `prepare.extras.num_games` is greater than zero
- Determinism: set `prepare.extras.seed` to reproduce identical synthetic data across runs
- Memory pressure: lower `train.model.block_size` and `train.data.batch_size` together

## Notes

- The preparer is auto-discovered via the `ml_playground.experiments` registry and adheres to the strict class-based API
- All paths in the config are relative to this experiment folder for portability
