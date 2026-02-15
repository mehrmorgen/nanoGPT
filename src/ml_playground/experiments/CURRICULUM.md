# Experiments Curriculum (Cross-Disciplinary)

<details>
<summary>Related documentation</summary>

- [Documentation Guidelines](../../../.dev-guidelines/project-specific/DOCUMENTATION.md) – Unified standards for documentation structure, abstraction levels, and formatting.
- [Experiments Overview](./README.md) – Operational conventions, experiment templates, and CLI usage for this directory.
- [Framework Utilities](../../../docs/framework_utilities.md) – Shared helpers used by preparers, training, and sampling flows.

</details>

## Purpose

This curriculum defines a shared learning ladder for `ml_playground` experiments.

It is intentionally cross-disciplinary:

- computer science framing (algorithms, state machines, search),
- statistical framing (estimators, optimization, uncertainty),
- mathematical framing (functions, operators, spaces),
- engineering framing (pipelines, artifacts, reproducibility).

The goal is to make equivalent ideas explicit across disciplines so contributors can reason about the same system with a shared vocabulary.

## Cross-Disciplinary Translation Table

| Computer Science | Statistics | Mathematics | ML Engineering |
| --- | --- | --- | --- |
| Algorithm | Estimator | Operator | Training loop |
| State | Random variable | Element of a space | Tensor/state object |
| Search | Optimization | Argmin/argmax | Backprop + optimizer step |
| Program | Model class | Function family | Runtime pipeline |
| Dataset | Sample | Empirical measure | Prepared artifacts (`train.bin`, `val.bin`, `meta.pkl`) |
| Evaluation | Hypothesis test | Metric/functional | Validation and regression checks |
| Heuristic | Prior | Regularizer | Inductive bias in architecture/config |

## Didactic Thesis

The curriculum progresses from deterministic symbolic behavior to strategic decision spaces:

1. deterministic systems (minimal learned copy),
2. fully solvable games,
3. combinatorial games with deeper search requirements.

This keeps complexity increases explicit and measurable at each stage.

## Stage Roadmap (0-6)

Status legend:

- `implemented`: exists in this repository as runnable experiment code.
- `planned`: design target for follow-up implementation.

### Stage 0: One-Symbol Learned Copy (`implemented`)

- Learning objective: demonstrate the smallest non-trivial learned mapping (`x -> x`) with full determinism.
- Minimal dataset/task: one symbol repeated in sequence; model learns to predict the same symbol.
- Success metric: near-zero loss on deterministic train/validation data; stable sampling that reproduces the symbol stream.
- Failure modes: collapse due to invalid metadata, incompatible block size, non-deterministic preprocessing.
- Next-stage unlock criteria: reproducible prepare/train/sample with stable checkpoints and deterministic outputs.

### Stage 1: Two-Symbol Learned Copy (`implemented`)

- Learning objective: learn identity mapping over `A/B` without collapsing to a constant output.
- Minimal dataset/task: balanced sequences over two symbols.
- Success metric: correct prediction accuracy per symbol; no majority-class collapse.
- Failure modes: token imbalance, representation collapse, incorrect tokenizer metadata.
- Next-stage unlock criteria: robust per-symbol accuracy and deterministic reruns.

### Stage 2: Add End-of-Sequence Token (`implemented`)

- Learning objective: introduce explicit output termination behavior.
- Minimal dataset/task: copy with appended `<EOS>`.
- Success metric: correct stop-token prediction and sequence completion.
- Failure modes: over-generation, premature stop, disallowed special token handling.
- Next-stage unlock criteria: consistent termination across fixed prompts.

### Stage 3: Add Start-of-Sequence Token (`implemented`)

- Learning objective: introduce explicit boundary conditioning (`<SOS> ... <EOS>`).
- Minimal dataset/task: bounded copy samples with both start/end delimiters.
- Success metric: correct boundary usage and stable autoregressive rollout.
- Failure modes: boundary token confusion, shifted labels, prompt alignment bugs.
- Next-stage unlock criteria: correct boundary-conditioned generation in deterministic tests.

### Stage 4: Variable-Length Copy (`implemented`)

- Learning objective: preserve order and length over variable-length sequences.
- Minimal dataset/task: mixed-length copy pairs over small symbol alphabet.
- Success metric: exact-match rate by sequence length bucket.
- Failure modes: position drift, truncation, bag-of-symbol behavior.
- Next-stage unlock criteria: strong exact-match performance over all configured lengths.

### Stage 5: Game 15 (`implemented`)

- Learning objective: transition from sequence copying to adversarial decision making.
- Minimal dataset/task: legal move generation and win-condition learning for Game 15.
- Success metric: legal-move rate, win/draw rate versus deterministic baselines.
- Failure modes: illegal actions, reward leakage, weak state encoding.
- Next-stage unlock criteria: reliable legal play and baseline-competitive strategy.

### Stage 6: Tic-Tac-Toe (`implemented`)

- Learning objective: spatial strategy with solvable optimal-play reference.
- Minimal dataset/task: board-state policy/value learning with legal move masking.
- Success metric: legal-move rate, no-loss rate versus perfect/minimax baselines.
- Failure modes: symmetry handling bugs, unstable self-play curriculum, value-target mismatch.
- Next-stage unlock criteria: robust play against deterministic evaluators and reproducible metrics.

## Stage Template (Implementation Contract)

Each stage implementation should include, at minimum:

- learning objective,
- minimal dataset/task definition,
- success metric,
- failure modes,
- next-stage unlock criteria.

For runnable stages, include:

- experiment-local `preparer.py`,
- `config.toml` and optional `test_config.toml`,
- experiment README with exact CLI commands,
- tests validating prepare/train/sample behavior.

## POC Status

- Implemented in this PR scope: stages 0 through 6.
- Planned (not implemented in this PR): none.

Further curriculum refinements should keep stage-level changes incremental to preserve reproducibility and green quality gates.
