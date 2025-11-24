# Analysis Package

<details>
<summary>Related documentation</summary>

- [.dev-guidelines/DOCUMENTATION.md](../../../.dev-guidelines/DOCUMENTATION.md) – README abstraction levels, folder tree standards, and formatting rules.
- [docs/LIT.md](../../../docs/LIT.md) – Detailed instructions for the Learning Interpretability Tool integration.
- [docs/framework_utilities.md](../../../docs/framework_utilities.md) – Shared helpers reused by analysis utilities (error handling, configuration, tokenizer protocol).

</details>

## Purpose

Visualization and evaluation helpers that complement the training and sampling workflows. Provides:

- A minimal Learning Interpretability Tool (LIT) integration for inspecting model behavior.
- Sample quality analysis utilities that score and compare generated text.

## Structure

```bash
src/ml_playground/tools/analysis/
├── README.md          # package overview (this file)
├── lit/               # LIT server entrypoints, configs, and adapters
├── lit_integration.py # high-level helpers for launching LIT against checkpoints
└── sample_quality.py  # deterministic quality metrics for generated text
```

## Key Components

- **`lit/`** contains the minimal LIT server setup described in `docs/LIT.md`. It exposes a Typer command for running the UI on a dedicated port with an isolated virtual environment.
- **`lit_integration.py`** wires `ml_playground` checkpoints and tokenizer metadata into LIT so you can inspect activations, gradients, and counterfactuals.
- **`sample_quality.py`** implements repeatable quality heuristics (e.g., distinct-n, repetition penalties) used by CLI sampling tests.

## Usage Examples

```bash
# boot the minimal LIT server (see docs/LIT.md for prerequisites)
uv run tools analysis lit --port 5432

# analyze a generated sample file
uv run tools analysis sample-quality output/samples/sample_001.txt
```

```python
from ml_playground.tools.analysis.sample_quality import analyze_sample_file, format_analysis

analysis = analyze_sample_file("output/samples/sample_001.txt")
print(format_analysis(analysis))
```

## Notes

- Keep analysis utilities dependency-light; heavy frameworks must be isolated behind optional extras or dedicated Typer tasks.
- When extending the LIT integration, document new flags or datasets in `docs/LIT.md` and reference them from this README.
- Follow the centralized tokenizer protocol when loading experiment metadata to guarantee compatibility with Core APIs.
