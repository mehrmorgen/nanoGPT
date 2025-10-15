# Connect Four Experiment – Kimi K2

Minimal Connect Four move-prediction experiment that generates its own synthetic games and trains a small GPT-style model to choose the next column.

## Overview
- Dataset: 10,000 synthetic Connect Four games produced via random, rules-respecting play
- Encoding/Tokenizer: character-level tokens representing the flattened 6×7 board plus a separator and column id
- Method: NanoGPT-style transformer predicting the next move as a single character
- Pipeline: prepare → train → sample via the ml_playground CLI

## Data
- Inputs (raw): generated in-memory; no external downloads are required
- Outputs (prepared): written to `ml_playground/experiments/connect_four/datasets/`
  - `train.bin`
  - `val.bin`
  - `meta.pkl` (includes Connect Four-specific metadata such as board size and example count)

## Method/Model
- Tokenization: character-level vocabulary built from the generated corpus (digits plus separator and newline)
- Sequence layout: 42 board slots (row-major) + `|` + move id (0–6)
- Model: 4-layer GPT with 4 heads and 128 embedding dim (block size 43)
- Checkpoints: saved under `./out/connect_four` (`ckpt_last.pt`, `ckpt_best.pt`)
- Logging: TensorBoard summaries in `./out/connect_four/logs/tb`

## Environment Setup (preferred)

```bash
make setup
make verify
```

## How to Run

Prepare the dataset (generates games and tokenizes them):

```bash
make prepare connect_four
```

Train the model using the bundled config:

```bash
make train connect_four CONFIG=ml_playground/experiments/connect_four/config.toml
```

Sample predicted moves from the trained checkpoint:

```bash
make sample connect_four CONFIG=ml_playground/experiments/connect_four/config.toml
```

Run the full loop (prepare → train → sample):

```bash
make loop connect_four CONFIG=ml_playground/experiments/connect_four/config.toml
```

## Configuration
- Config path: `ml_playground/experiments/connect_four/config.toml`
- `prepare.extras.num_games` controls how many synthetic games are generated (default 10,000)
- `prepare.extras.random_seed` seeds the random move generator for reproducible datasets
- Training overrides shrink the model and block size for the 43-token sequences
- Sampling constrains `top_k` to 7 so only valid columns are considered

## Outputs
- Data artifacts: `ml_playground/experiments/connect_four/datasets/train.bin`, `val.bin`, `meta.pkl`
- Training artifacts: `./out/connect_four/` containing checkpoints and logs
- Sampling artifacts: saved under the same `out_dir` (see CLI output for filenames)

## Troubleshooting
- To create a tiny dataset for debugging, set `prepare.extras.num_games` to a small value via `CONFIG` overrides or CLI injection
- Ensure you have enough disk space for the generated binaries (~1 MB for the default settings)
- If sampling returns unexpected characters, check that the correct checkpoint exists in `./out/connect_four`

## Notes
- The preparer is auto-discovered by the CLI (class `ConnectFourPreparer` in `preparer.py`)
- All paths in the config are relative to the experiment folder for portability
- The dataset is synthetic and purely illustrative—it does not implement advanced gameplay heuristics yet
