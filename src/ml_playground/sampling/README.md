# Sampling Package

<details>
<summary>Related documentation</summary>

- [Framework Utilities](../../../docs/framework_utilities.md) – Sampling configuration and helpers referenced by this package.
- [LIT Integration](../../../docs/LIT.md) – Web-based analysis workflows that consume sampling outputs.

</details>

## Purpose

Model inference and text generation utilities for `ml_playground`. Provides checkpoint loading, model setup, and
sampling orchestration with proper error handling.

## Structure

- `runner.py` - Main sampling implementation and orchestration

## Key APIs

- `Sampler` - Sampling orchestrator class
- `sample()` - Functional sampling interface
- `run_server_bundestag_char()` - LIT integration demo

## Usage Example

```python
from ml_playground.sampling.runner import Sampler
from ml_playground.configuration.models import SamplerConfig

sampler = Sampler(config, shared_config)
sampler.run()
```
