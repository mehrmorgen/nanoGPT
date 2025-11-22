from __future__ import annotations

import pytest

from ml_playground.models.core.model import GPT


def test_from_pretrained_not_implemented() -> None:
    """from_pretrained must remain unsupported to keep the GPT API strict."""

    with pytest.raises(
        NotImplementedError, match="from_pretrained is not supported in this port"
    ):
        GPT.from_pretrained("dummy_model")
