# Bundestag (tiktoken BPE)

<details>
<summary>Related documentation</summary>

- [.dev-guidelines/project-specific/DOCUMENTATION.md](../../../../.dev-guidelines/project-specific/DOCUMENTATION.md) – Experiment README blueprint, folder tree standards, and abstraction rules.
- [Framework Utilities](../../../../docs/framework_utilities.md) – Shared helpers for tokenizer preparation, error handling, and progress reporting.

</details>

Byte-Pair Encoding (BPE) experiment using tiktoken to tokenize Bundestag speeches into uint32 token IDs.

## Overview

- Dataset: Custom text provided as input.txt
- Encoding: tiktoken (default: cl100k_base)
- Method: NanoGPT-style training with strict TOML configuration
- Pipeline: prepare → train → sample via ml_playground CLI

## Data

- Input: src/ml_playground/experiments/bundestag_tiktoken/datasets/input.txt
  - If missing, you may place a sample input.txt there manually (the preparer can also seed from a bundled resource if present).
- Outputs (prepared):
  - train.bin, val.bin (uint32 arrays)
  - meta.pkl (tokenizer metadata: encoding name and dtype)

## Method/Model

- Split corpus 90/10 into train/val
- Tokenize with tiktoken (cl100k_base by default) into uint32 arrays
- Model hyperparameters and runtime behavior controlled by TOML
- TensorBoard logs written to out_dir/logs/tb
  This experiment uses the centralized framework utilities for error handling, progress reporting, and file operations. For more information, see [Framework Utilities Documentation](../../../../docs/framework_utilities.md).

## Environment Setup (UV-only)

```bash
uv run tools env setup
uv run tools env verify
```

## Strict configuration injection

- This experiment does not read TOML directly. The CLI loads and validates the TOML and injects config objects into the experiment code.

## How to Run

- Config example: src/ml_playground/experiments/bundestag_tiktoken/config.toml

Prepare:

```bash
uv run cli --exp-config src/ml_playground/experiments/bundestag_tiktoken/config.toml prepare bundestag_tiktoken
```

Train:

```bash
uv run cli --exp-config src/ml_playground/experiments/bundestag_tiktoken/config.toml train bundestag_tiktoken
```

Sample:

```bash
uv run cli --exp-config src/ml_playground/experiments/bundestag_tiktoken/config.toml sample bundestag_tiktoken
```

## Configuration Highlights

- [training.data]
  - dataset_dir = "src/ml_playground/experiments/bundestag_tiktoken/datasets"
  - train_bin, val_bin, meta_pkl
  - batch_size, block_size, grad_accum_steps
- [training.runtime]
  - out_dir = "src/ml_playground/experiments/bundestag_tiktoken/out/bundestag_tiktoken"
  - device = "mps" (or "cpu"/"cuda")
- [sampling.runtime]
  - out_dir should match train.runtime.out_dir

## Outputs

- Data artifacts: src/ml_playground/experiments/bundestag_tiktoken/datasets/{train.bin,val.bin,meta.pkl}
- Training: out_dir contains checkpoints and logs/tb

## Folder structure

```bash
src/ml_playground/experiments/bundestag_tiktoken/
├── README.md        # experiment documentation (this file)
├── config.toml      # sample/preset config for real runs
├── test_config.toml # tiny defaults for tests
├── preparer.py      # dataset preparation (tiktoken BPE, write bins/meta)
├── trainer.py       # NanoGPT-style training orchestration
├── sampler.py       # generation/sampling entrypoints
└── datasets/        # prepared dataset artifacts written here
```

## Troubleshooting

- Ensure tiktoken is installed; otherwise, the sampler will fall back to a byte codec.
- If you see ID dtype mismatches, verify that meta.pkl indicates "uint32" and that your training config expects corresponding sizes.

## Notes

- The preparer is registered in ml_playground.experiments and invoked via CLI prepare bundestag_tiktoken.

## Checklist

- Adheres to [.dev-guidelines/README.md](../../../../.dev-guidelines/README.md) (abstraction, required sections).
- Folder tree includes inline descriptions for each entry.
- Links to shared docs where applicable (e.g., `../../../../docs/framework_utilities.md`).
- Commands are copy-pasteable and minimal (setup, prepare/train/sample).
- Configuration Highlights only list essential keys; defaults are not restated.
- Outputs paths and filenames reflect current behavior (check `[training.runtime].out_dir`).
