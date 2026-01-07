from __future__ import annotations

import pytest

from ml_playground.configuration.models import ModelConfig
from ml_playground.models.core.config import build_gpt_config


def test_build_gpt_config_requires_vocab_size() -> None:
    """Building GPTConfig requires a vocab size."""
    cfg = ModelConfig(n_layer=1, n_head=1, n_embd=8, block_size=8, vocab_size=None)
    with pytest.raises(ValueError):
        build_gpt_config(cfg)


def test_build_gpt_config_builds_when_vocab_size_present() -> None:
    """Building GPTConfig succeeds when vocab size is present."""
    cfg = ModelConfig(n_layer=1, n_head=1, n_embd=8, block_size=8, vocab_size=16)
    gpt_cfg = build_gpt_config(cfg)
    assert gpt_cfg.vocab_size == 16
