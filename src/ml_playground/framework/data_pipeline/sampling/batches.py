"""Batch sampling utilities backed by memory-mapped datasets."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Callable, Literal, cast

import pickle
from pathlib import Path

import numpy as np
import numpy.typing as npt
import torch
from torch import Tensor
from numpy.lib.stride_tricks import sliding_window_view

from ml_playground.framework.configuration.models import DataConfig, DeviceKind
from ml_playground.framework.data_pipeline.sources.memmap import MemmapReader

__all__ = ["sample_batch", "SimpleBatches"]


def sample_batch(
    reader: MemmapReader,
    batch_size: int,
    block_size: int,
    device: DeviceKind,
) -> tuple[torch.Tensor, torch.Tensor]:
    """Sample a random batch of token sequences from the given memmap reader."""
    L = int(reader.length)
    if L == 0:
        raise ValueError(
            "Dataset is empty: no tokens available. Ensure the dataset preparation wrote non-empty train/val bins."
        )

    base: npt.NDArray[np.int64] = np.asarray(reader.arr, dtype=np.int64)
    steps: npt.NDArray[np.int64] = np.arange(block_size, dtype=np.int64)

    if L <= block_size:
        idx: npt.NDArray[np.int64] = np.random.randint(
            0, L, size=(batch_size,), dtype=np.int64
        )
        x_indices = (idx[:, None] + steps) % L
        y_indices = (idx[:, None] + 1 + steps) % L
        x_np = base[x_indices]
        y_np = base[y_indices]
    else:
        idx = np.random.randint(0, L - block_size, size=(batch_size,), dtype=np.int64)
        windows: npt.NDArray[np.int64] = sliding_window_view(base, block_size)
        x_np = windows[idx]
        y_np = windows[idx + 1]

    x_np = np.asarray(x_np, dtype=np.int64)
    y_np = np.asarray(y_np, dtype=np.int64)

    x_arr: npt.NDArray[np.int64] = x_np
    y_arr: npt.NDArray[np.int64] = y_np

    x = _from_numpy_int64(x_arr).to(device)
    y = _from_numpy_int64(y_arr).to(device)
    return x, y


class SimpleBatches:
    """Iterate over memory-mapped training and validation datasets."""

    def __init__(
        self,
        data: DataConfig,
        device: DeviceKind,
        dataset_dir: Path,
    ) -> None:
        self.data = data
        self.device: DeviceKind = device
        self._dataset_dir = dataset_dir
        train_path = data.train_path(dataset_dir)
        val_path = data.val_path(dataset_dir)
        if not train_path.exists() or not val_path.exists():
            raise FileNotFoundError(
                f"Training data not found at {train_path} and/or {val_path}"
            )

        dtype: np.dtype[Any] = np.dtype(np.uint16)
        meta_path = data.meta_path(dataset_dir)
        meta_obj: object | None = None
        if meta_path.exists():
            try:
                with meta_path.open("rb") as f:
                    meta_obj = cast(object, pickle.load(f))
            except (OSError, pickle.UnpicklingError, EOFError):
                meta_obj = None
            if isinstance(meta_obj, dict):
                dtype_meta = cast(dict[str, object], meta_obj)
                dts_obj = dtype_meta.get("dtype")
                if isinstance(dts_obj, str):
                    if dts_obj == "uint32":
                        dtype = np.dtype(np.uint32)
                    elif dts_obj == "uint16":
                        dtype = np.dtype(np.uint16)
        self.train = MemmapReader.open(train_path, dtype=dtype)
        self.val = MemmapReader.open(val_path, dtype=dtype)
        self._cursor: dict[Literal["train", "val"], int] = {"train": 0, "val": 0}

    def get_batch(
        self, split: Literal["train", "val"]
    ) -> tuple[torch.Tensor, torch.Tensor]:
        reader = self.train if split == "train" else self.val
        sampler = cast(
            Literal["random", "sequential"],
            getattr(self.data, "sampler", "random"),
        )
        if sampler == "sequential":
            return self._get_sequential_batch(split, reader)
        if sampler == "random":
            return sample_batch(
                reader,
                self.data.batch_size,
                self.data.block_size,
                self.device,
            )
        raise ValueError(f"Unknown sampler '{sampler}'")

    def _get_sequential_batch(
        self, split: Literal["train", "val"], reader: MemmapReader
    ) -> tuple[torch.Tensor, torch.Tensor]:
        L = int(reader.length)
        if L == 0:
            raise ValueError(
                "Dataset is empty: no tokens available. Ensure the dataset preparation wrote non-empty train/val bins."
            )
        bsz = int(self.data.batch_size)
        T = int(self.data.block_size)
        cur = self._cursor[split]
        base: npt.NDArray[np.int64] = np.asarray(reader.arr, dtype=np.int64)
        starts: npt.NDArray[np.int64] = (cur + np.arange(bsz, dtype=np.int64) * T) % L
        steps: npt.NDArray[np.int64] = np.arange(T, dtype=np.int64)
        x_indices = (starts[:, None] + steps) % L
        y_indices = (starts[:, None] + 1 + steps) % L

        self._cursor[split] = int((cur + bsz * T) % L)

        x_seq_arr: npt.NDArray[np.int64] = base[x_indices].astype(np.int64, copy=False)
        x = _from_numpy_int64(x_seq_arr).to(self.device)
        y_seq_arr: npt.NDArray[np.int64] = base[y_indices].astype(np.int64, copy=False)
        y = _from_numpy_int64(y_seq_arr).to(self.device)
        return x, y


def _from_numpy_int64(array: npt.NDArray[np.int64]) -> Tensor:
    """Create an int64 tensor from a numpy array with precise typing."""

    return _torch_from_numpy_int64(array)


if TYPE_CHECKING:
    _torch_from_numpy_int64 = cast(
        "Callable[[npt.NDArray[np.int64]], Tensor]", torch.from_numpy
    )
else:
    _torch_from_numpy_int64 = torch.from_numpy
