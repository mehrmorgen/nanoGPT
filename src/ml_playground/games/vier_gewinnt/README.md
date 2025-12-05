# Vier Gewinnt Game Runner

<details>
<summary>Related documentation</summary>

- [ml_playground Developer Guidelines](../../../../.dev-guidelines/README.md) – Binding quality, tooling, and documentation standards for this module.
- [Vier Gewinnt Experiment README](../../experiments/vier_gewinnt/README.md) – How to prepare datasets, train checkpoints, and sample GPT-based players.

</details>

## Overview

This directory contains the interactive Connect Four implementation used both for human play and for sampling moves from the trained nanoGPT agents. The CLI surface lets you quickly start head-to-head matches between humans, scripted bots, and GPT-based sampler players without digging into the source first.

## Folder Structure

```bash
vier_gewinnt/
├── README.md          # You are here – runtime entry points and usage tips
├── play.py            # Human/bot gameplay CLI
├── data_generator.py  # Deterministic self-play generator for training data
├── engine.py          # Board rules, move validation, victory detection
├── players.py         # Random/heuristic/minimax player strategies
└── sampler_player.py  # Wrapper that loads GPT checkpoints as move samplers
```

## Prerequisites

1. Install the repository's UV environment (`uv run env-tasks setup`).
1. Ensure the Vier Gewinnt experiment has prepared datasets and trained checkpoints if you plan to use the `*_ai` sampler players. See the linked experiment README for `uv run cli prepare`, `train`, and `sample` commands.

## Quick Start: Human vs AI

```bash
uv run python -m ml_playground.games.vier_gewinnt.play --player1 human --player2 easy_ai
```

Notes:

- `play.py` injects the project root into `sys.path`, so calling it as a module works from anywhere in the repo.
- Omit the flags to accept the defaults (`human` vs `easy`).
- The CLI blocks for keyboard input when a player type is `human`.

### Available Player Types

| Flag value  | Description                                              |
| ----------- | -------------------------------------------------------- |
| `human`     | Prompts on the command line for column numbers.          |
| `easy`      | Random player (uniform random moves).                    |
| `medium`    | Heuristic player (win/block + center preference).        |
| `hard`      | Minimax player (depth=4, alpha-beta pruning).            |
| `easy_ai`   | GPT sampler backed by `experiments/vier_gewinnt_easy`.   |
| `medium_ai` | GPT sampler backed by `experiments/vier_gewinnt_medium`. |
| `hard_ai`   | GPT sampler backed by `experiments/vier_gewinnt_hard`.   |

#### Player strategy details

- **Random (`easy`)** – Samples uniformly from the currently valid columns; useful for smoke-testing the UI or creating high-variance datasets. Implementation lives in `players.RandomPlayer`.
- **Heuristic (`medium`)** – Applies a short priority list inside `players.HeuristicPlayer`:
  1. If any move yields an immediate win, take it.
  1. Otherwise, block the opponent’s winning move if one exists.
  1. Fall back to a center-out preference order `[3, 2, 4, 1, 5, 0, 6]` to maximize board control.
- **Minimax (`hard`)** – Uses `players.MinimaxPlayer`, a depth-4 minimax search with alpha-beta pruning. The evaluator rewards center control and 2/3-in-a-row windows for the current player, heavily penalizing opponent threats and assigning ±100000 to terminal wins/losses. When no deterministic best move exists at the cutoff depth, it keeps the best-scoring candidate encountered.

Example: play heuristic vs minimax, letting minimax start as Player 2:

```bash
uv run python -m ml_playground.games.vier_gewinnt.play --player1 heuristic --player2 minimax
```

## Generating Training Data

Use the deterministic generator to create move histories for offline training or evaluation. Each file now stores **unique** games—the generator keeps simulating until it collects the requested number of distinct move histories, warning if the selected players are too deterministic to satisfy the quota.

```bash
uv run python -m ml_playground.games.vier_gewinnt.data_generator heuristic minimax 500 data/heuristic_vs_minimax.txt
```

Arguments:

1. `player1` – one of `random`, `heuristic`, `minimax`.
1. `player2` – same choices as above.
1. `num_games` – integer count of simulations.
1. `output_file` – destination text file (each line `winner:col0,col1,...`).

## Using GPT Sampler Players

Sampler players load nanoGPT checkpoints from `src/ml_playground/experiments/<variant>/`. Ensure the corresponding experiment folder contains:

1. `datasets/meta.pkl` (created by `uv run cli prepare ...`).
1. `out/<variant>/ckpt_last_*.pt` (produced by `uv run cli train ...`).

Without these artifacts the sampler will raise `FileNotFoundError`. Re-run the experiment pipeline or point the sampler to a valid experiment directory.

## Troubleshooting

1. **Import errors** – run commands via `uv run` to guarantee the environment and PYTHONPATH are consistent.
1. **Sampler checkpoints missing** – revisit the experiment README to regenerate datasets/checkpoints for the chosen difficulty.
1. **Model too slow on CPU** – set the `SamplerPlayer` `device` argument to `"cuda"` (if available) by editing `PLAYER_TYPES` or instantiating manually.
