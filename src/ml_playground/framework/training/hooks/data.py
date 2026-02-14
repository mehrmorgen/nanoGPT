"""Data loading helpers for the training loop."""

from __future__ import annotations

from pathlib import Path

from ml_playground.framework.configuration.models import TrainerConfig, MetadataConfig
from ml_playground.framework.data_pipeline.sampling.batches import SimpleBatches


__all__ = ["initialize_batches"]


def initialize_batches(cfg: TrainerConfig, metadata: MetadataConfig) -> SimpleBatches:
    """Create a `SimpleBatches` iterator bound to the resolved dataset directory."""
    dataset_dir: Path = metadata.dataset_dir
    return SimpleBatches(
        data=cfg.data, device=cfg.runtime.device, dataset_dir=dataset_dir
    )
