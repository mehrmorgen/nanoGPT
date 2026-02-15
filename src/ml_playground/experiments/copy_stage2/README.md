# Copy Stage 2 (Add End-of-Sequence Token)

<details>
<summary>Related documentation</summary>

- [Documentation Guidelines](../../../../.dev-guidelines/project-specific/DOCUMENTATION.md) – Unified standards for documentation structure, abstraction levels, and formatting.
- [Experiments Overview](../README.md) – Shared experiment conventions and templates.
- [Curriculum](../CURRICULUM.md) – Stage roadmap and cross-disciplinary framing for this experiment.

</details>

Curriculum Stage 2: introduce explicit EOS termination behavior.

## Overview

- Dataset: deterministic two-symbol payloads with appended EOS token.
- Tokenization: character-level.
- Method: tiny GPT-style next-token prediction on CPU-safe defaults.
- Pipeline: , ,  via runtime CLI.

## How to Run

- Config: 
wtmp begins Wed Dec 17 14:28:15 2025
Microsoft Windows [Version 10.0.26200.7840]
(c) Microsoft Corporation. All rights reserved.

C:\Users\JensBickel\source\mehrmorgen\nanoGPT-eta-logging\coppy_Stage1>

Prepare:



Train:



Sample:



## Outputs

- Data artifacts: 
- Training and sampling artifacts: 
stage2
```

Sample:

```bash
uv run cli --exp-config src/ml_playground/experiments/copy_stage2/config.toml sample copy_stage2
```

## Outputs

- Data artifacts: `src/ml_playground/experiments/copy_stage2/datasets/`
- Training and sampling artifacts: `src/ml_playground/experiments/copy_stage2/out/copy_stage2/`
