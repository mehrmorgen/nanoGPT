"""Memory-mapped dataset sources used by the batching utilities."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np
import numpy.typing as npt


__all__ = ["MemmapReader"]


@dataclass
class MemmapReader:
    """Lightweight wrapper around NumPy memmap arrays with length metadata."""

    arr: npt.NDArray[Any]
    length: int

    def close(self) -> None:
        """Explicitly close the memmap array and release the file handle."""
        if self.arr is not None:
            self.arr.flush()
            del self.arr
            self.arr = None

    @classmethod
    def open(cls, path: Path, *, dtype: np.dtype[Any]) -> "MemmapReader":
        arr: npt.NDArray[Any] = np.memmap(path, dtype=dtype, mode="r")
        return cls(arr=arr, length=int(arr.shape[0]))
