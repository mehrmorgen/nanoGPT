# Connect Four Experiment – Kimi K2 Implementation Plan

<details>
<summary>Related documentation</summary>

- [Experiment documentation standards](../.dev-guidelines/DOCUMENTATION.md) – Unified documentation standards that define structure, formatting, and linking rules for experiments and related plans.

</details>

## Overview

Step-by-step plan for implementing the naive **Connect Four Experiment – Kimi K2** in the `ml_playground` framework. The goal is to create a reproducible pipeline for preparing data, training a transformer model, and sampling moves.

## 1. Create experiment structure

```bash
ml_playground/experiments/connect_four/
├── preparer.py           # dataset preparation logic
├── config.toml           # baseline training + sampling configuration
├── Readme.md             # experiment documentation
├── datasets/             # prepared dataset artifacts
└── out/                  # training outputs (gitignored)
```

## 2. Model and data representation

- Board: 6 × 7 grid encoded as a flat 42-token sequence.
- Tokens: `0` empty, `1` player one, `2` player two.
- Moves: integers `0-6` representing columns.
- Sample sequence format: `board_state | move` (separator `|`).

## 3. Dataset generation strategy

1. Implement a minimal `ConnectFourGame` helper to enforce rules and provide legal moves.
1. Generate synthetic games via random play to bootstrap training data.
1. Record `(board_state, move)` pairs before each move is applied.
1. Optionally tag outcomes (win/loss/draw) for future supervised signals.

## 4. Preparer implementation

- Place the preparer in `ml_playground/experiments/connect_four/preparer.py` extending the experiment preparer protocol.
- Use repository helpers: tokenizer factory, `split_train_val`, `write_bin_and_meta`, and `create_standardized_metadata`.
- Encode each example as `"{board}|{move}"` joined by newlines before tokenization.
- Persist `train.bin`, `val.bin`, and `meta.pkl` to `datasets/`.
- Return a `PrepareReport` listing created artifacts and a success message.

## 5. Configuration (`config.toml`)

- Model: small GPT (4 layers, 4 heads, 128 embedding, block size 43).
- Training: batch size 16, gradient accumulation 4, cosine LR decay with warmup.
- Runtime: 10k iterations, eval every 100 steps, CPU-friendly defaults.
- Checkpointing: last and best checkpoints keyed on validation loss.
- Sampling: generate one move per call with `top_k = 7` to restrict to legal moves.

## 6. Documentation (`Readme.md`)

- Follow experiment README blueprint (overview, data, method, how to run, configuration summary).
- Reference shared utilities via `../../docs/framework_utilities.md` where needed.
- Include concise bash tree and command snippets for prepare/train/sample targets.

## 7. Testing plan

- Unit tests for `ConnectFourGame` (move validity, win detection, board resets).
- Tests ensuring the preparer emits correctly shaped datasets and metadata.
- Configuration loader test to validate the new `config.toml` against schema.
- Optional integration test covering prepare → train → sample smoke run with tiny settings.

## 8. Future enhancements

- Replace random move generator with minimax or heuristic agent for stronger signals.
- Add curriculum datasets combining random, heuristic, and solved positions.
- Extend model outputs to predict game outcome probabilities alongside next move.
- Explore reinforcement learning fine-tuning loops leveraging self-play.

## 9. Quality checklist

- [ ] Code fully typed and lint-clean (`make quality`).
- [ ] All new docs formatted with `mdformat`.
- [ ] Tests cover new logic (`make tests-unit`).
- [ ] Dataset artifacts excluded from version control.
- [ ] README links verified and follow relative-link policy.

## 10. Suggested workflow commands

```bash
make setup
make verify
make prepare connect_four
make train connect_four CONFIG=ml_playground/experiments/connect_four/config.toml
make sample connect_four CONFIG=ml_playground/experiments/connect_four/config.toml
```
