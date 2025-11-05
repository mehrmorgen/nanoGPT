# Tiny Shakespeare (GPT-2 BPE)

<details>
<summary>Related documentation</summary>

- [.dev-guidelines/DOCUMENTATION.md](../../../../.dev-guidelines/DOCUMENTATION.md) – Required sections, abstraction levels, and folder tree standards for experiment docs.
- [Framework Utilities](../../../../docs/framework_utilities.md) – Shared helpers for tokenizer preparation, training, and sampling flows.

</details>

Minimal experiment to prepare, train, and sample on the Tiny Shakespeare corpus using GPT-2 BPE tokenization.

## Overview

- Dataset: Tiny Shakespeare (auto-downloaded)
- Encoding: GPT-2 BPE via tiktoken
- Method: Classic NanoGPT-style training (strictly typed, TOML-configured)
- Pipeline: prepare → train → sample via ml_playground CLI

## Data

- Preparer auto-downloads `input.txt` if missing.
- Prepared artifacts under `[train.data].dataset_dir` (default: `src/ml_playground/experiments/shakespeare/datasets/`).
- Universal meta: the preparer writes `meta.pkl` alongside `train.bin` and `val.bin`.

## Method/Model

- GPT-2 BPE via tiktoken; small GPT configured via TOML (see `[train.*]`).
- Rotated checkpoints and TensorBoard logs under `[train.runtime].out_dir`.
  For framework utilities, see [../../../../docs/framework_utilities.md](../../../../docs/framework_utilities.md).

## How to Run

- Config: `src/ml_playground/experiments/shakespeare/config.toml`

Prepare:

```bash
uv run cli --exp-config src/ml_playground/experiments/shakespeare/config.toml prepare shakespeare
```

Train:

```bash
uv run cli --exp-config src/ml_playground/experiments/shakespeare/config.toml train shakespeare
```

Sample:

```bash
uv run cli --exp-config src/ml_playground/experiments/shakespeare/config.toml sample shakespeare
```

## Configuration Highlights

- `[train.data].dataset_dir` default: `src/ml_playground/experiments/shakespeare/datasets`
- `[train.runtime].out_dir` default: `src/ml_playground/experiments/shakespeare/out/shakespeare_next`
- `[train.runtime].device`: `cpu` or `mps` (or `cuda` if available)

## Outputs

- Training artifacts under `[train.runtime].out_dir` (rotated checkpoints, `logs/tb`).
- Prepared data under `[train.data].dataset_dir` (`train.bin`, `val.bin`, `meta.pkl`).

## Folder structure

```bash
src/ml_playground/experiments/shakespeare/
├── README.md        # experiment documentation (this file)
├── config.toml      # sample/preset config for real runs
├── test_config.toml # tiny defaults for tests
├── preparer.py      # dataset preparation (download/tokenize, write bins/meta)
├── trainer.py       # NanoGPT-style training orchestration
├── sampler.py       # generation/sampling entrypoints
└── datasets/        # prepared dataset artifacts written here
```

## Troubleshooting

- If tokenization fails, ensure `tiktoken` is installed and accessible.

## Notes

- Prepared data is written only to this experiment's `datasets/` directory.

## Checklist

- Adheres to [.dev-guidelines/README.md](../../../../.dev-guidelines/README.md) (abstraction, required sections).
- Folder tree includes inline descriptions for each entry.
- Links to shared docs where applicable (e.g., `../../../../docs/framework_utilities.md`).
- Commands are copy-pasteable and minimal (setup, prepare/train/sample).
- Configuration Highlights only list essential keys; defaults are not restated.
- Outputs paths and filenames reflect current behavior (check `[train.runtime].out_dir`).
