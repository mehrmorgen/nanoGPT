# Connect Four Self-Play Experiment

Minimal end-to-end pipeline that fabricates a Connect Four dataset via random self-play and trains a small GPT to predict legal moves from board snapshots.

## Overview
- Dataset: Synthetic Connect Four positions generated on-the-fly (board + player turn + executed move + ply index).
- Encoding/Tokenizer: Character-level tokenizer with a compact vocabulary over digits, player markers (`A`/`B`), move letters (`a`–`g`), ply digits, and newlines.
- Method: NanoGPT-style causal language model trained on flattened board sequences.
- Pipeline: `make prepare connect_four` → `make train connect_four CONFIG=...` → `make sample connect_four CONFIG=...`.

## Data
- Inputs (raw): None required; the preparer simulates games deterministically from the supplied seed.
- Outputs (prepared):
  - `datasets/train.bin` — flattened uint16 tokens for training.
  - `datasets/val.bin` — validation split encoded with the same tokenizer.
  - `datasets/meta.pkl` — tokenizer metadata plus board/token stats for downstream consumers.

## Method/Model
- Tokenization: Character-level vocabulary derived from the generated dataset.
- Model: 4-layer GPT (n_embd=128, n_head=4, block_size=96) sized to cover a full board snapshot plus auxiliary tokens.
- Checkpoints (rotated-only):
  - `ckpt_last.pt`
  - `ckpt_best.pt`
- Logging: TensorBoard summaries in `out/logs/tb` when enabled.

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

Sample (after a checkpoint exists):

```bash
make sample connect_four CONFIG=ml_playground/experiments/connect_four/config.toml
```

## Config Highlights
- `prepare.extras.num_games`: Number of self-play games to generate (defaults to 4096).
- `prepare.extras.train_fraction`: Fraction of move records routed to the training split.
- `train.model.block_size`: 96 tokens covering the 42-cell board plus player/move context.
- `train.runtime.max_iters`: Conservative 4k iterations for quick CPU convergence.
- `sample.sample.top_k`: Narrow (7) to bias sampling toward plausible columns.

## Outputs
- Prepared artifacts live under `datasets/`.
- Training outputs (checkpoints, logs) are written to `out/`.
- Sampling artifacts (if any) also emit under `out/` by default.

## Troubleshooting
- **Dataset too small**: Increase `prepare.extras.num_games` for more examples. Ensure validation split remains non-empty (keep `train_fraction < 1.0`).
- **Determinism**: Override `prepare.extras.seed` to vary random self-play runs.
- **Context errors**: Match `[train.data].block_size` ≤ `[train.model].block_size` if you tweak the config.

## Notes
- The preparer enforces positive `num_games`, valid `train_fraction` bounds, and ensures both train/val splits contain data.
- All artifacts are generated locally; no external downloads are required.
