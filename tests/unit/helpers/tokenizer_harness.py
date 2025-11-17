"""Base test harness for tokenizer classes providing common lookup array testing utilities."""

from __future__ import annotations

import numpy as np
import numpy.typing as npt
from abc import ABC, abstractmethod

from ml_playground.core.tokenizer_protocol import Tokenizer


class TokenizerTestHarness(ABC):
    """Base test harness exposing lookup array maintenance for tokenizers."""

    def invalidate_lookup_array(self) -> None:
        """Invalidate the cached lookup array to test rebuild logic."""
        if hasattr(self, "_itos_array"):
            delattr(self, "_itos_array")

    def expose_lookup_array(self) -> npt.NDArray[np.object_]:
        """Expose the internal lookup array for testing."""
        return self._ensure_lookup_array()

    def lookup_array_length(self) -> int:
        """Get the length of the lookup array for testing."""
        return self._ensure_lookup_array().shape[0]

    @abstractmethod
    def _ensure_lookup_array(self) -> npt.NDArray[np.object_]:
        """Abstract method that concrete tokenizers must implement."""
        pass
