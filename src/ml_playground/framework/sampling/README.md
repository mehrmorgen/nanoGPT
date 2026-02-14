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
- `run_sampling()` - Convenience helper for executing a `SamplingPlan`
- `SamplingPlan` / `SamplingRunner` - Structured sampling entrypoints used by the CLI
- `ml_playground.framework.sampling.api` - high-level `SamplingPlan`/`SamplingRunner` helpers used by the CLI.
- `run_server_bundestag_char()` - LIT integration demo

## Usage Example

```python
from pathlib import Path

from ml_playground.framework.configuration.models import MetadataConfig, SamplerConfig
from ml_playground.framework.sampling.api import SamplingPlan, run_sampling

sampler_cfg = SamplerConfig(...)
metadata = MetadataConfig(
    experiment="my-exp",
    config_path=Path("configs/exp.toml"),
    project_home=Path("."),
    dataset_dir=Path("data"),
    train_out_dir=Path("runs/train"),
    sample_out_dir=Path("runs/sample"),
)
plan = SamplingPlan(config=sampler_cfg, metadata=metadata)
summary = run_sampling(plan)
```
