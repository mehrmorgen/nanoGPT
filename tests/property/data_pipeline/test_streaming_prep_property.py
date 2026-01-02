from __future__ import annotations

import logging
import pickle
from pathlib import Path

import numpy as np
from hypothesis import given, settings, strategies as st

from ml_playground.data_pipeline.transforms.streaming import append_bin_and_meta


@settings(max_examples=12, deadline=100, derandomize=True)
@given(
    train_first=st.lists(st.integers(min_value=0, max_value=1024), max_size=10),
    val_first=st.lists(st.integers(min_value=0, max_value=1024), max_size=10),
    train_second=st.lists(st.integers(min_value=0, max_value=1024), max_size=10),
    val_second=st.lists(st.integers(min_value=0, max_value=1024), max_size=10),
)
def test_append_bin_and_meta_when_appending_then_updates_counts(
    tmp_path: Path,
    train_first: list[int],
    val_first: list[int],
    train_second: list[int],
    val_second: list[int],
) -> None:
    """Append bin and meta when appending then updates counts."""
    logger = logging.getLogger("ml_playground.streaming.test")

    first_meta = {
        "meta_version": 1,
        "train_tokens": 0,
        "val_tokens": 0,
    }

    train_first_arr = np.asarray(train_first, dtype=np.uint16)
    val_first_arr = np.asarray(val_first, dtype=np.uint16)

    append_bin_and_meta(
        tmp_path,
        train_first_arr,
        val_first_arr,
        first_meta,
        logger=logger,
    )

    train_path = tmp_path / "train.bin"
    val_path = tmp_path / "val.bin"
    meta_path = tmp_path / "meta.pkl"

    with meta_path.open("rb") as handle:
        meta = pickle.load(handle)

    assert meta["train_tokens"] == len(train_first)
    assert meta["val_tokens"] == len(val_first)

    train_second_arr = np.asarray(train_second, dtype=np.uint16)
    val_second_arr = np.asarray(val_second, dtype=np.uint16)
    second_meta = {
        "meta_version": 1,
        "train_tokens": 0,
        "val_tokens": 0,
    }

    append_bin_and_meta(
        tmp_path,
        train_second_arr,
        val_second_arr,
        second_meta,
        logger=logger,
    )

    with meta_path.open("rb") as handle:
        updated = pickle.load(handle)

    assert updated["train_tokens"] == len(train_first) + len(train_second)
    assert updated["val_tokens"] == len(val_first) + len(val_second)

    assert train_path.stat().st_size == (len(train_first) + len(train_second)) * 2
    assert val_path.stat().st_size == (len(val_first) + len(val_second)) * 2
