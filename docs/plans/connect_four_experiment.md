# Connect Four Experiment Plan

<details>
<summary>Related documentation</summary>

- [Documentation Guidelines](../../.dev-guidelines/DOCUMENTATION.md) – Unified standards for all documentation in this repository with abstraction rules and formatting requirements.
- [Experiments Overview](../../ml_playground/experiments/Readme.md) – This directory hosts self-contained experiments with shared conventions for preparers, configs, and datasets.

</details>

## Objective

Establish a naive Connect Four experiment that follows the existing ml_playground workflow. The experiment should generate self-play data, tokenize it at the character level, and train a small GPT-style model to predict legal moves and game outcomes from sequential game states.

## Game representation

- Use a 7×6 board with two players.
- Represent the board textually with one character per cell: `.` for empty, `1` for player one, and `2` for player two.
- Separate columns with `|` and rows with `\n` to keep grid structure readable.
- Add action and outcome markers such as `[MOVE:3]`, `[WIN:1]`, and `[DRAW]` so the model sees explicit transitions.
- Start every record with `[START]` to anchor the tokenizer and assist sampling prompts.

## Implementation steps

1. **Game engine**
   - Create `ml_playground/experiments/connect_four/game_engine.py` with a board class, move validation, win detection, and serialization helpers.
   - Ensure functions are pure where possible so they are easy to test.
   - Provide utilities to enumerate valid moves and detect terminal states (win or draw).
1. **Data generation**
   - Add `data_generator.py` to create random or heuristic-guided self-play games.
   - Implement horizontal mirroring for augmentation to double examples cheaply.
   - Write helpers that emit full game transcripts using the agreed token format.
   - Target at least 10 000 games to start, keeping an escape hatch for future smarter agents.
1. **Dataset preparation**
   - Implement `preparer.py` that plugs into the experiment protocol, writes `train.bin`, `val.bin`, and `meta.pkl`, and reuses centralized utilities for tokenization and IO.
   - Split data 90/10 between train and validation with a deterministic seed.
   - Construct a character vocabulary from the generated transcripts and persist it in metadata.
1. **Configuration**
   - Provide `config.toml` with a moderate GPT configuration (e.g., 4 layers, 4 heads, 128 hidden size, 512 block size) and CPU-friendly defaults.
   - Mirror the structure used by existing experiments so CLI commands remain consistent.
1. **Documentation**
   - Author `Readme.md` for the experiment that follows documentation guidelines, includes the required folder tree, and references shared utilities rather than re-explaining them.
   - Highlight how to run `make prepare`, `make train`, `make sample`, and `make loop` for this experiment.
1. **Sampler (optional initial cut)**
   - If time permits, add a sampler that interprets model logits as column choices, filters illegal moves, and allows human or scripted play.
   - Otherwise, rely on the default text sampler and defer interactive tooling to a follow-up task.
1. **Testing**
   - Write unit tests for the game engine (move legality, win detection), data generator (transcript invariants), and preparer (dataset artifacts and metadata).
   - Add a slim integration test that runs the preparer with a tiny configuration to validate wiring.
   - Update or extend pytest markers if new suites are introduced.
1. **Validation workflow**
   - Run `make quality` locally before commits to satisfy the repository gate.
   - Execute a smoke training run with a drastically reduced config to confirm end-to-end wiring.
   - Capture results or follow-up questions in the experiment README as troubleshooting notes.

## Open questions / future enhancements

- Explore stronger self-play agents (minimax or MCTS) to create higher-quality training data.
- Consider structured output heads for legal-move masking once the baseline is stable.
- Track win-rate metrics by scripting evaluation matches against random or rule-based opponents.
- Add a CLI utility for playing against the trained model once sampling quality justifies it.
