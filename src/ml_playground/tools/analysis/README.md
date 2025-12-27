# Analysis Tools

<details>
<summary>Related documentation</summary>

- [.dev-guidelines/DOCUMENTATION.md](../../../.dev-guidelines/DOCUMENTATION.md) – README abstraction levels, folder tree standards, and formatting rules.
- [docs/LIT.md](../../../docs/LIT.md) – Detailed instructions for the Learning Interpretability Tool integration.
- [docs/framework_utilities.md](../../../docs/framework_utilities.md) – Shared helpers reused by analysis utilities (error handling, configuration, tokenizer protocol).

</details>

## Purpose

Visualization and evaluation helpers that complement the training and sampling workflows. Part of the `ml_playground.tools` suite. Provides:

- A minimal Learning Interpretability Tool (LIT) integration for inspecting model behavior.
- Sample quality analysis utilities that score and compare generated text.

## Structure

```bash
src/ml_playground/tools/analysis/
├── README.md          # package overview (this file)
├── lit/               # LIT server entrypoints, configs, and adapters
├── lit_integration.py # high-level helpers for launching LIT against checkpoints
├── sample_quality.py  # deterministic quality metrics for generated text
└── sample_quality_public.py # lightweight public entrypoints for docs/demos
```

## Key Components

- **`lit/`** contains the minimal LIT server setup. It is exposed via the `analysis lit` tool command.
- **`lit_integration.py`** wires `ml_playground` checkpoints and tokenizer metadata into LIT.
- **`sample_quality.py`** implements repeatable quality heuristics (e.g., distinct-n, repetition penalties).
- **`sample_quality_public.py`** provides a public API for the quality metrics.

## CLI Usage

These tools are accessible via the `uv run tools analysis` command:

```bash
# Analyze a sample file
uv run tools analysis sample-quality path/to/sample.txt

# Launch the LIT server (PoC mode)
uv run tools analysis lit --port 5432
```

## Programmatic Usage

```python
from ml_playground.tools.analysis import analyze_sample_text, format_analysis

analysis = analyze_sample_text("Generated text sample...")
print(format_analysis(analysis))
```

## Notes

- Keep analysis utilities dependency-light; heavy frameworks like `lit-nlp` are handled as optional extras.
- Follow the centralized tokenizer protocol when loading experiment metadata.
