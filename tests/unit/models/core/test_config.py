from __future__ import annotations

import pytest

from ml_playground.configuration.models import ModelConfig
from ml_playground.models.core.config import build_gpt_config


def test_build_gpt_config_raises_when_vocab_size_not_set() -> None:
    cfg = ModelConfig(n_layer=1, n_head=1, n_embd=4, block_size=4, vocab_size=None)
    with pytest.raises(ValueError, match="vocab_size must be set"):
        build_gpt_config(cfg)
