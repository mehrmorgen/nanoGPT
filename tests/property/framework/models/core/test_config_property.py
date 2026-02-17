from __future__ import annotations

import pytest
from hypothesis import given, settings, strategies as st

from ml_playground.framework.configuration.models import ModelConfig
from ml_playground.framework.models.core.config import build_gpt_config, GPTConfig


# Generate ModelConfigs
@st.composite
def model_configs(draw: st.DrawFn) -> ModelConfig:
    return ModelConfig(
        block_size=draw(st.integers(min_value=1, max_value=2048)),
        vocab_size=draw(st.one_of(st.none(), st.integers(min_value=1, max_value=4096))),
        n_layer=draw(st.integers(min_value=1, max_value=48)),
        n_head=draw(st.integers(min_value=1, max_value=48)),
        n_embd=draw(st.integers(min_value=1, max_value=4096)),
        dropout=draw(st.floats(min_value=0.0, max_value=1.0)),
        bias=draw(st.booleans()),
    )


@given(cfg=model_configs())
@settings(max_examples=40, deadline=None, derandomize=True)
def test_build_gpt_config_properties(cfg: ModelConfig) -> None:
    if cfg.vocab_size is None:
        with pytest.raises(ValueError, match="vocab_size must be set"):
            build_gpt_config(cfg)
    else:
        gpt_cfg = build_gpt_config(cfg)
        assert isinstance(gpt_cfg, GPTConfig)
        assert gpt_cfg.block_size == cfg.block_size
        assert gpt_cfg.vocab_size == cfg.vocab_size
        assert gpt_cfg.n_layer == cfg.n_layer
        assert gpt_cfg.n_head == cfg.n_head
        assert gpt_cfg.n_embd == cfg.n_embd
        assert gpt_cfg.dropout == cfg.dropout
        assert gpt_cfg.bias == cfg.bias
