# Data Pipeline Package

## Purpose

Data pipeline utilities for preparing, transforming, and batching training data in `ml_playground` experiments. Handles
tokenization, data loading, and batch sampling with strict typing and validation.

## Structure

- `preparer.py` - Main data preparation workflow
- `transforms/io.py` - Dataset artifact I/O helpers
- `transforms/depth_pools.py` - Depth-based pool partitioning and blending
- `transforms/ingestion.py` - Streaming CSV/text/JSONL ingestion helpers
- `transforms/streaming.py` - Append-only helpers for online/self-play data

## Key APIs

- `create_pipeline()` - Data pipeline factory
- `write_bin_and_meta()` - Persist train/val bins with metadata
- `append_bin_and_meta()` - Append-only updates with metadata refresh
- `partition_by_depth()` - Group records into per-depth pools
- `blend_pools()` - Blend depth pools with optional weights
- `validate_metadata_contract()` - Enforce metadata schema requirements
- `stream_csv_column()` - Stream text columns from CSV inputs
- `stream_jsonl()` - Stream JSONL records with layout validation

## Usage Example

```python
from ml_playground.data_pipeline.preparer import prepare_dataset
from ml_playground.core.tokenizer import create_tokenizer

pipeline = create_pipeline(config, shared_config)
outcome = pipeline.run()
```

## Related Documentation

- [Framework Utilities](../docs/framework_utilities.md) - Data preparation guidelines
- [Development Guidelines](../.dev-guidelines/DEVELOPMENT.md) - Data handling standards
