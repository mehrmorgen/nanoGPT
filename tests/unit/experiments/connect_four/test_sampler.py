"""Tests for the Connect Four sampler."""

from __future__ import annotations

from types import SimpleNamespace

import torch
import pytest

from ml_playground.configuration.models import SampleConfig, SamplerConfig
from ml_playground.core.error_handling import DataError
from ml_playground.core.tokenizer import create_tokenizer
from ml_playground.experiments.connect_four.sampler import ConnectFourSampler
from tests.support.config_builders import create_basic_configs


class _FakeModel:
    def __init__(self, tokenizer) -> None:
        self._tokenizer = tokenizer
        self._moves = [3, 3, 3, 3]
        self.calls = 0

    def __call__(self, inputs: torch.Tensor) -> torch.Tensor:  # noqa: D401
        seq_len = inputs.shape[1]
        vocab = len(self._tokenizer.vocab)
        logits = torch.zeros((1, seq_len, vocab), dtype=torch.float32)
        move = self._moves[min(self.calls, len(self._moves) - 1)]
        token = self._tokenizer.encode(str(move))[0]
        logits[0, -1, token] = 10.0
        self.calls += 1
        return logits


def _tokenizer():
    return create_tokenizer(
        "char",
        vocab={ch: idx for idx, ch in enumerate(".XO0123456\n")},
    )


def test_connect_four_sampler_plays_game(tmp_path) -> None:
    _, _, base_sample_cfg, shared = create_basic_configs(tmp_path)
    tokenizer = _tokenizer()
    fake_model = _FakeModel(tokenizer)
    fake_sampler = SimpleNamespace(
        runtime_cfg=SimpleNamespace(device="cpu"),
        tokenizer=tokenizer,
        model=fake_model,
    )
    extras = {
        "sampler_factory": lambda cfg, shared_cfg: fake_sampler,
        "human_player": "O",
        "human_moves": [4, 4, 4, 4],
        "policy": "greedy",
    }
    sample_cfg = base_sample_cfg.model_copy(
        update={
            "sample": base_sample_cfg.sample.model_copy(
                update={"start": "\n", "max_new_tokens": 2, "num_samples": 1}
            ),
            "extras": extras,
        }
    )
    sampler = ConnectFourSampler()
    report = sampler.sample(sample_cfg, shared)
    assert fake_model.calls >= 1
    assert any("model" in msg for msg in report.messages)
    assert report.messages[-1].startswith("[connect_four] result:")


def test_connect_four_sampler_rejects_invalid_human_move(tmp_path) -> None:
    _, _, base_sample_cfg, shared = create_basic_configs(tmp_path)
    tokenizer = _tokenizer()
    fake_sampler = SimpleNamespace(
        runtime_cfg=SimpleNamespace(device="cpu"),
        tokenizer=tokenizer,
        model=_FakeModel(tokenizer),
    )
    extras = {
        "sampler_factory": lambda cfg, shared_cfg: fake_sampler,
        "human_player": "X",
        "human_moves": [7],
    }
    sample_cfg = base_sample_cfg.model_copy(
        update={
            "sample": base_sample_cfg.sample.model_copy(
                update={"start": "\n", "max_new_tokens": 2, "num_samples": 1}
            ),
            "extras": extras,
        }
    )
    sampler = ConnectFourSampler()
    with pytest.raises(DataError):
        sampler.sample(sample_cfg, shared)
