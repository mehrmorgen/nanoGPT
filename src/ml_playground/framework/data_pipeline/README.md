# Data Pipeline Package

<details>
<summary>Related documentation</summary>

- [docs/framework_utilities.md](../../docs/framework_utilities.md) – Canonical reference for preparation helpers, tokenizer protocol usage, and metadata guarantees.
- [.dev-guidelines/project-specific/DOCUMENTATION.md](../../.dev-guidelines/project-specific/DOCUMENTATION.md) – README structure, abstraction levels, and folder tree standards.
- [.dev-guidelines/project-specific/DEVELOPMENT.md](../../.dev-guidelines/project-specific/DEVELOPMENT.md) – Data handling policies, typing expectations, and quality gates.

</details>

## Purpose

Shared preparation utilities that transform raw experiment data into standardized binaries and metadata. The package provides:

- A deterministic preparation pipeline that writes `train.bin`, `val.bin`, and `meta.pkl` using centralized IO helpers.
- Integration with the unified tokenizer protocol (`char`, `word`, `tiktoken`) through the factory in `ml_playground.framework.core.tokenizer`.
- File-state diffing so experiments can report which artifacts were created or updated during preparation runs.

## Structure

```bash
src/ml_playground/framework/data_pipeline/
├── README.md        # package overview (this file)
├── preparer.py      # Preparation pipeline orchestration and public factory
├── transforms/      # Tokenization and IO helpers (prepare_with_tokenizer, write_bin_and_meta)
├── sampling/        # Utilities reused by sampling flows (e.g., dataset iterators)
└── sources/         # Pluggable data sources and loaders
```

## Core Concepts

- **`PreparationOutcome`** – Captures created/updated/skipped files plus metadata returned by the pipeline.
- **`create_pipeline(..., text_provider=..., snapshot_provider=..., snapshot_differ=...)`** – Factory that wires config into a pipeline, optionally injecting providers for strict I/O control and testing.
- **Tokenization** – Delegates to `prepare_with_tokenizer()` in `transforms/tokenization.py`, which consumes a `Tokenizer` from `ml_playground.framework.core.tokenizer`. This honors the centralized tokenizer protocol (char, word, tiktoken) described in the repository memory.
- **File-state tracking** – `snapshot_file_states()` and `diff_file_states()` (re-exported from `transforms.io`) let experiments emit precise change logs for datasets. Providers can be swapped for testing.
- **Standardized metadata** – `write_bin_and_meta()` persists tensor binaries alongside a `meta.pkl` that records tokenizer details, vocabulary size, and other experiment-defined extras.

## Typical Usage

```python
from pathlib import Path

from ml_playground.framework.configuration.models import PreparerConfig, MetadataConfig
from ml_playground.framework.data_pipeline.preparer import create_pipeline

cfg = PreparerConfig(
    raw_text_path=Path("datasets/input.txt"),
    tokenizer_type="tiktoken",
    extras={},
)
shared = MetadataConfig(
    experiment="demo",
    config_path=Path("config.toml"),
    project_home=Path("."),
    dataset_dir=Path("datasets"),
    train_out_dir=Path("out/train"),
    sample_out_dir=Path("out/sample"),
)

# Optional: Inject text provider for custom loading
pipeline = create_pipeline(cfg, shared, text_provider=lambda p: "mock data")
outcome = pipeline.run()

print(outcome.created_files)
print(outcome.metadata["tokenizer_type"])  # e.g., "tiktoken"
```

## Notes

- All tokenizers must be created via `ml_playground.framework.core.tokenizer.create_tokenizer()` or injected through `PreparerConfig.tokenizer_factory` to stay compliant with the centralized protocol.
- Keep new transforms deterministic and side-effect free; any filesystem writes should go through `write_bin_and_meta()` or helpers that provide atomic semantics.
- Update `docs/framework_utilities.md` alongside significant pipeline changes so experiments and CLI documentation remain in sync.
