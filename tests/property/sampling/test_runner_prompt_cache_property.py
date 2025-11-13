from __future__ import annotations

import itertools
import pickle
from collections.abc import Mapping
from pathlib import Path

import torch
from hypothesis import HealthCheck, given, settings, strategies as st

from ml_playground.configuration.models import (
    RuntimeConfig,
    SampleConfig,
    SamplerConfig,
    SharedConfig,
)
from ml_playground.sampling.runner import Sampler
from ml_playground.core.tokenizer import CharTokenizer
from ml_playground.models.core.model import GPT
from ml_playground.configuration.models import ModelConfig
from ml_playground.core.logging_protocol import LoggerLike

_RUN_COUNTER = itertools.count()


class _StubModel(GPT):
    def __init__(self) -> None:
        cfg = ModelConfig(
            n_layer=1,
            n_head=1,
            n_embd=8,
            block_size=8,
            dropout=0.0,
            vocab_size=32,
        )

        class _NullLogger(LoggerLike):
            def debug(self, msg: str, *args: object, **kwargs: object) -> None:
                pass

            def info(self, msg: str, *args: object, **kwargs: object) -> None:
                pass

            def warning(self, msg: str, *args: object, **kwargs: object) -> None:
                pass

            def error(self, msg: str, *args: object, **kwargs: object) -> None:
                pass

        super().__init__(cfg, _NullLogger())
        self.generate_calls = 0

    def eval(self) -> "_StubModel":
        super().eval()
        return self

    def to(  # type: ignore[override]
        self,
        device: torch.device | str | None = None,
        dtype: torch.dtype | None = None,
        non_blocking: bool = False,
    ) -> "_StubModel":
        super().to(device=device, dtype=dtype, non_blocking=non_blocking)
        return self

    def load_state_dict(  # type: ignore[override]
        self,
        state_dict: Mapping[str, torch.Tensor],
        strict: bool = True,
        assign: bool = False,
    ) -> object:
        return super().load_state_dict(state_dict, strict=strict, assign=assign)

    def generate(  # type: ignore[override]
        self,
        idx: torch.Tensor,
        max_new_tokens: int,
        temperature: float = 1.0,
        top_k: int | None = None,
    ) -> torch.Tensor:
        self.generate_calls += 1
        b, t = idx.shape
        out = (
            torch.arange(t + max_new_tokens, dtype=torch.long).unsqueeze(0).repeat(b, 1)
        )
        return out


class _SamplerTokenizer(CharTokenizer):
    def decode_tensor(self, token_tensor: torch.Tensor) -> str:
        flattened: torch.Tensor = token_tensor.detach().cpu().view(-1)
        ids: list[int] = [int(value.item()) for value in flattened]
        return self.decode(ids)


def _create_shared(tmp_path: Path) -> tuple[SharedConfig, Path]:
    out_dir = tmp_path / f"out_{next(_RUN_COUNTER)}"
    out_dir.mkdir(exist_ok=True)
    meta = {
        "meta_version": 1,
        "kind": "char",
        "dtype": "uint16",
        "tokenizer_type": "char",
        "train_tokens": 0,
        "val_tokens": 0,
        "stoi": {"A": 1},
        "itos": {1: "A"},
    }
    with (out_dir / "meta.pkl").open("wb") as fh:
        pickle.dump(meta, fh)
    return (
        SharedConfig(
            experiment="hypo",
            config_path=out_dir / "cfg.toml",
            project_home=out_dir,
            dataset_dir=out_dir,
            train_out_dir=out_dir,
            sample_out_dir=out_dir,
        ),
        out_dir,
    )


def _build_sampler(tmp_path: Path, start: str) -> tuple[Sampler, _StubModel]:
    shared, out_dir = _create_shared(tmp_path)

    rt = RuntimeConfig(
        out_dir=out_dir, device="cpu", dtype="float32", compile=False, seed=42
    )
    sample_cfg = SampleConfig(
        start=start, num_samples=1, max_new_tokens=2, temperature=1.0, top_k=0
    )

    def _load_ckpt(**_: object) -> object:
        class _Ckpt:
            model: dict[str, torch.Tensor] = {
                "tok_emb.weight": torch.zeros(1, dtype=torch.float32)
            }
            model_args = {
                "block_size": 4,
                "vocab_size": 8,
                "n_layer": 1,
                "n_head": 1,
                "n_embd": 4,
            }

        return _Ckpt()

    model = _StubModel()
    sampler = Sampler(
        SamplerConfig(
            runtime=rt,
            sample=sample_cfg,
            checkpoint_load_fn=_load_ckpt,
            model_factory=lambda cfg, logger: model,
        ),
        shared,
    )
    sampler.tokenizer = _SamplerTokenizer({"A": 1})
    sampler.model = model
    return sampler, model


@given(
    start=st.text(
        alphabet=st.characters(min_codepoint=65, max_codepoint=67),
        min_size=1,
        max_size=4,
    )
)
@settings(
    max_examples=5,
    deadline=None,
    suppress_health_check=[HealthCheck.function_scoped_fixture],
)
def test_sampler_prompt_tensor_cache(tmp_path: Path, start: str) -> None:
    """Sampler should reuse cached prompt tensor for identical prompts."""
    sampler, stub_model = _build_sampler(tmp_path, start)

    sampler.run()
    tensor_before = sampler.prompt_tensor
    assert tensor_before is not None
    sampler.run()

    assert sampler.prompt_tensor is tensor_before
    assert stub_model.generate_calls == sampler.sample_cfg.num_samples * 2
