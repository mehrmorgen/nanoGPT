# Bundestag (Char-Level)

<details>
<summary>Related documentation</summary>

- [.dev-guidelines/project-specific/DOCUMENTATION.md](../../../../.dev-guidelines/project-specific/DOCUMENTATION.md) – Experiment README blueprint, folder tree standards, and abstraction rules.
- [Framework Utilities](../../../../docs/framework_utilities.md) – Shared helpers for tokenizer preparation, progress reporting, and training.

</details>

Character-level language modeling on Bundestag speeches serialized from GermaParlTEI.

## Overview

- Dataset source: GermaParlTEI only (`PolMine/GermaParlTEI`)
- Encoding: per-character IDs (`uint16`)
- Pipeline: `prepare -> train -> sample -> analyze`
- Runtime model/training settings are injected from TOML by the CLI

## Data Preparation Contract

`prepare bundestag_char` always performs a remote-head freshness check before deciding skip vs rebuild.

- Remote head SHA is resolved for `germaparl_repo@germaparl_ref`
- If prepared artifacts are valid and `meta.pkl` stores the same `source_head_sha`, preparation is skipped
- If the source head changed (or artifacts are stale), overwrite confirmation is required
- If confirmation is declined, preparation aborts without mutating existing artifacts

### Stored raw artifacts

- Cached source archive: `src/ml_playground/experiments/bundestag_char/raw/germaparl_cache/*.tar.gz`
- No persistent extracted XML tree is kept
- Canonical prepared text: `src/ml_playground/experiments/bundestag_char/datasets/input.txt`

## Prepared Outputs

- `input.txt` (serialized corpus text)
- `train.bin`, `val.bin` (`uint16` token IDs)
- `meta.pkl` with minimal contract:
  - `meta_version`
  - `tokenizer_type`, `tokenizer`
  - `vocab_size`, `stoi`, `itos`
  - `train_tokens`, `val_tokens`
  - `source_head_sha`, `source_repo`, `source_ref`

## Configuration Highlights

Example config: `src/ml_playground/experiments/bundestag_char/config.toml`

`[prepare.extras]` supports only:

- `dataset_dir_override`
- `germaparl_repo`
- `germaparl_ref`
- `germaparl_cache_dir`
- `germaparl_include_stage`
- `germaparl_include_speaker_attrs`
- `split`

## How to Run

Environment setup:

```bash
uv run tools env setup
uv run tools env verify
```

Prepare:

```bash
uv run cli --exp-config src/ml_playground/experiments/bundestag_char/config.toml prepare bundestag_char
```

Train:

```bash
uv run cli --exp-config src/ml_playground/experiments/bundestag_char/config.toml train bundestag_char
```

Sample:

```bash
uv run cli --exp-config src/ml_playground/experiments/bundestag_char/config.toml sample bundestag_char
```

Analyze:

```bash
uv run cli --exp-config src/ml_playground/experiments/bundestag_char/config.toml analyze bundestag_char
```

Analyze behavior:

- Uses TensorBoard UI when event files exist under `out/logs/tb`
- Falls back to LIT demo UI for `bundestag_char` if no TensorBoard event files are available

## Progress Logging

Prepare logs progress for:

- archive download size milestones
- TEI file serialization progress
- vocabulary scan and encoding progress
- final token/accounting summary

## Licensing Note

GermaParlTEI is distributed under CLARIN PUB+BY+NC+SA.

- https://raw.githubusercontent.com/PolMine/GermaParlTEI/main/README.md
- https://raw.githubusercontent.com/PolMine/GermaParlTEI/main/LICENSE.md

Do not commit downloaded corpus files or generated dataset artifacts.

## Folder Structure

```bash
src/ml_playground/experiments/bundestag_char/
├── README.md        # experiment documentation (this file)
├── config.toml      # sample/preset config for real runs
├── test_config.toml # tiny defaults for tests
├── extras.py        # strict extras schemas for prepare/train/sample
├── germaparl_tei.py # GermaParl remote-head, archive, and TEI serialization helpers
├── preparer.py      # GermaParl-only preparer with freshness + overwrite guard
├── trainer.py       # training orchestration
├── sampler.py       # sampling orchestration
├── ollama_export.py # GGUF/Ollama export helper
├── datasets/        # prepared outputs (ignored)
├── raw/             # cached source archives (ignored)
└── out/             # runtime outputs (ignored)
```
