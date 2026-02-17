from __future__ import annotations

import pytest
from hypothesis import given, settings, strategies as st
from pydantic import ValidationError

from ml_playground.framework.configuration.models import ModelConfig
from ml_playground.framework.models.core.config import build_gpt_config, GPTConfig


# Generate raw ModelConfig payloads so validation rules are exercised explicitly.
@st.composite
def model_payloads(draw: st.DrawFn) -> dict[str, object]:
    return {
        "block_size": draw(st.integers(min_value=1, max_value=2048)),
        "vocab_size": draw(
            st.one_of(st.none(), st.integers(min_value=1, max_value=4096))
        ),
        "n_layer": draw(st.integers(min_value=1, max_value=48)),
        "n_head": draw(st.integers(min_value=1, max_value=48)),
        "n_embd": draw(st.integers(min_value=1, max_value=4096)),
        "dropout": draw(st.floats(min_value=0.0, max_value=1.0)),
        "bias": draw(st.booleans()),
    }


@given(payload=model_payloads())
@settings(max_examples=40, deadline=None, derandomize=True)
def test_build_gpt_config_properties(payload: dict[str, object]) -> None:
    cfg = ModelConfig.model_validate(payload)
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


@given(
    bad_int=st.integers(max_value=0),
    bad_dropout=st.one_of(st.floats(max_value=-0.01), st.floats(min_value=1.01)),
)
@settings(max_examples=25, deadline=None, derandomize=True)
def test_model_config_validation_rejects_invalid_payloads(
    bad_int: int, bad_dropout: float
) -> None:
    bad_payload = {
        "block_size": bad_int,
        "n_layer": bad_int,
        "n_head": bad_int,
        "n_embd": bad_int,
        "dropout": bad_dropout,
        "bias": True,
    }
    with pytest.raises(ValidationError):
        ModelConfig.model_validate(bad_payload)
