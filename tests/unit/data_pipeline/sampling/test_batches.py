from __future__ import annotations

from pathlib import Path

import numpy as np

from ml_playground.configuration.models import DataConfig
from ml_playground.data_pipeline.sampling.batches import SimpleBatches


def test_simple_batches_uses_uint32_dtype_from_meta(tmp_path: Path) -> None:
    train_path = tmp_path / "train.bin"
    val_path = tmp_path / "val.bin"
    meta_path = tmp_path / "meta.pkl"

    train_path.write_bytes(np.array([1, 2, 3, 4], dtype=np.uint32).tobytes())
    val_path.write_bytes(np.array([5, 6, 7, 8], dtype=np.uint32).tobytes())
    meta_path.write_bytes(b'{"dtype": "uint32", "meta_version": 1}')

    cfg = DataConfig(batch_size=2, block_size=2)
    batches = SimpleBatches(cfg, device="cpu", dataset_dir=tmp_path)

    x, y = batches.get_batch("train")
    assert x.shape == (2, 2)
    assert y.shape == (2, 2)
