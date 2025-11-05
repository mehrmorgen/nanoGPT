# Core Package

<details>
<summary>Related documentation</summary>

- [.dev-guidelines/DOCUMENTATION.md](../../../.dev-guidelines/DOCUMENTATION.md) – Package README standards and abstraction levels.
- [docs/framework_utilities.md](../../../docs/framework_utilities.md) – Centralized error handling, tokenizer, and IO utilities referenced here.
- [.dev-guidelines/DEVELOPMENT.md](../../../.dev-guidelines/DEVELOPMENT.md) – Coding standards, import policies, and testing expectations for core modules.

</details>

## Purpose

Foundation utilities shared across the entire `ml_playground` runtime. Provides:

- Structured error handling primitives for deterministic diagnostics.
- Tokenizer protocol and concrete implementations (char, word, tiktoken) following the unified tokenizer design.
- Logging abstractions and file-state helpers reused by data preparation and training flows.

## Structure

```bash
src/ml_playground/core/
├── README.md            # package overview (this file)
├── error_handling.py    # MLPlaygroundError hierarchy, safe_call/file utilities, ProgressReporter
├── file_state.py        # snapshot/diff helpers for filesystem change detection
├── logging_protocol.py  # logging abstraction used across tools and runtime code
├── tokenizer.py         # centralized tokenizer factory and implementations
└── tokenizer_protocol.py # Tokenizer protocol definition shared by data_pipeline and sampling
```

## Key Components

- **Error Handling (`error_handling.py`)**: defines `MLPlaygroundError` and domain-specific subclasses along with helpers like `safe_file_operation()` and `ProgressReporter` to enforce structured diagnostics.
- **Tokenizer Stack (`tokenizer_protocol.py`, `tokenizer.py`)**: implements the tokenizer protocol and factory backing `char`, `word`, and `tiktoken` tokenizers. Used by `data_pipeline.preparer`, sampling, and experiments (see centralized tokenizer memory).
- **File State Utilities (`file_state.py`)**: capture before/after file metadata so preparation workflows can report created/updated artifacts.
- **Logging Protocol (`logging_protocol.py`)**: describes the minimal logger interface consumed by core services and Typer CLIs.

## Usage Example

```python
from ml_playground.core.tokenizer import create_tokenizer

# Build the tokenizer declared in experiment config
tokenizer = create_tokenizer("tiktoken", encoding_name="cl100k_base")

from ml_playground.core.error_handling import safe_file_operation

safe_file_operation(lambda: do_work(), logger=my_logger)
```

## Notes

- Keep new helpers protocol-first and generic; downstream modules depend on this package remaining lightweight and dependency-free.
- Follow the import standards in `.dev-guidelines/IMPORT_GUIDELINES.md` when exposing new symbols. Prefer explicit exports in `__all__`.
- Update `docs/framework_utilities.md` whenever core APIs change so experiments and tools remain aligned.
