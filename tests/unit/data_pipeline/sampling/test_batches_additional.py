from __future__ import annotations


import numpy as np
import pytest

from ml_playground.data_pipeline.sampling.batches import sample_batch


class _FakeReader:
    def __init__(self, arr: np.ndarray):
        self.arr = arr
        self.length = arr.shape[0]


def test_sample_batch_raises_on_empty_reader() -> None:
    reader = _FakeReader(np.array([], dtype=np.int64))
    with pytest.raises(ValueError, match="Dataset is empty"):
        sample_batch(reader, batch_size=2, block_size=4, device="cpu")


def test_sample_batch_wraps_when_block_exceeds_length() -> None:
    arr = np.arange(4, dtype=np.int64)
    reader = _FakeReader(arr)
    x, y = sample_batch(reader, batch_size=2, block_size=8, device="cpu")
    assert x.shape == (2, 8)
    assert y.shape == (2, 8)
    # Ensure indices wrap rather than erroring
    assert x.numpy().min() >= 0
