from __future__ import annotations

import pytest
import torch

from ml_playground.models.layers.attention import CausalSelfAttention


def test_causal_self_attention_init_with_valid_params() -> None:
    """CausalSelfAttention should initialize successfully with valid parameters."""
    n_embd, n_head = 64, 8
    attention = CausalSelfAttention(n_embd, n_head)
    assert attention is not None


def test_causal_self_attention_init_raises_on_invalid_n_embd() -> None:
    """CausalSelfAttention should raise ValueError when n_embd not divisible by n_head."""
    n_embd, n_head = 64, 7  # 64 not divisible by 7
    with pytest.raises(AssertionError):
        CausalSelfAttention(n_embd, n_head)


def test_causal_self_attention_init_raises_on_invalid_n_head() -> None:
    """CausalSelfAttention should raise ValueError when n_head <= 0."""
    n_embd, n_head = 64, 0
    with pytest.raises(AssertionError):
        CausalSelfAttention(n_embd, n_head)


def test_causal_self_attention_forward_produces_correct_shape() -> None:
    """CausalSelfAttention forward should produce output with correct shape."""
    n_embd, n_head = 64, 8
    batch_size, seq_len = 2, 10

    attention = CausalSelfAttention(n_embd, n_head)
    x = torch.randn(batch_size, seq_len, n_embd)

    output = attention(x)

    # Output should have same shape as input
    assert output.shape == (batch_size, seq_len, n_embd)
