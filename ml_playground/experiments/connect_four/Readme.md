# Connect Four Game Learning

Minimal experiment to train a language model on Connect Four move sequences generated via random self-play.

## Overview
- Dataset: 10k simulated Connect Four games produced on-the-fly during preparation
- Encoding/Tokenizer: Character-level tokenizer (digits and spaces)
- Method: NanoGPT-style Transformer trained on move transcripts
- Pipeline: prepare → train → sample via ml_playground CLI

## Data
- Inputs (raw): Generated in-memory by the preparer; no external files required
- Outputs (prepared):
  - `datasets/train.bin`
  - `datasets/val.bin`
  - `datasets/meta.pkl`

## Method/Model
- Tokenization: Character-level encoding created at prepare time
- Model: Small GPT (4 layers, 4 heads, 128 hidden size, 128 context)
- Checkpoints (rolling):
  - `ckpt_last.pt`
  - `ckpt_best.pt` (tracked by validation loss)
- Logging: TensorBoard summaries under `out/logs/tb`

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

Sample:

```bash
make sample connect_four CONFIG=ml_playground/experiments/connect_four/config.toml
```

End-to-end loop:

```bash
make loop connect_four CONFIG=ml_playground/experiments/connect_four/config.toml
```

## Configuration

- Preparation extras expose knobs for `num_games`, `min_moves`, and RNG `seed`
- Training configuration mirrors `config.toml`; adjust `n_layer`, `block_size`, or learning-rate schedule as needed
- Sampling uses a short prompt (`"3 4 2"`) and generates up to 20 new moves per sample

## Outputs

- Data artifacts: `ml_playground/experiments/connect_four/datasets/`
- Training artifacts: `ml_playground/experiments/connect_four/out/` (checkpoints, logs/tb)

## Troubleshooting

- If preparation yields zero games, check `prepare.extras.min_moves` and `num_games`
- To reproduce identical datasets, keep the `prepare.extras.seed` fixed
- Sampling outputs raw move sequences; validate legality via downstream tooling if needed

## Notes

- The preparer uses randomized self-play and is auto-discovered by the CLI registry
- All paths in the config are relative to the experiment directory for portability
