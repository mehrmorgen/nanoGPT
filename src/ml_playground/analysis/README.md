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
src/ml_playground/analysis/
├── README.md          # package overview (this file)
├── lit/               # LIT server entrypoints, configs, and adapters
├── lit_integration.py # high-level helpers for launching LIT against checkpoints
├── sample_quality.py  # deterministic quality metrics for generated text
└── sample_quality_public.py # lightweight public entrypoints for docs/demos
```

## Key Components

- **`lit/`** contains the minimal LIT server setup described in `docs/LIT.md`. It exposes a Typer command for running the UI on a dedicated port with an isolated virtual environment.
- **`lit_integration.py`** wires `ml_playground` checkpoints and tokenizer metadata into LIT so you can inspect activations, gradients, and counterfactuals.
- **`sample_quality.py`** implements repeatable quality heuristics (e.g., distinct-n, repetition penalties) used by CLI sampling tests.
- **`sample_quality_public.py`** provides a stripped-down variant suitable for external documentation examples where internal dependencies are avoided.

## Usage Examples

```bash
# boot the minimal LIT server (see docs/LIT.md for prerequisites)
uv run lit-tasks run --port 5432

# shutdown the LIT server
uv run lit-tasks stop --port 5432
```

```python
from ml_playground.analysis.sample_quality import compute_quality_report

report = compute_quality_report("Generated text", reference="Ground truth")
print(report.distinct_n)
```

## Notes

- Keep analysis utilities dependency-light; heavy frameworks must be isolated behind optional extras or dedicated Typer tasks.
- When extending the LIT integration, document new flags or datasets in `docs/LIT.md` and reference them from this README.
- Follow the centralized tokenizer protocol when loading experiment metadata to guarantee compatibility with Core APIs.
