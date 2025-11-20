from __future__ import annotations

import pytest

from ml_playground.models.core.model import GPT


def test_from_pretrained_not_implemented() -> None:
    """Test that from_pretrained raises NotImplementedError for legacy API compatibility."""

    with pytest.raises(
        NotImplementedError, match="from_pretrained is not supported in this port"
    ):
        GPT.from_pretrained("dummy_model")
