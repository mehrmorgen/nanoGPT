from __future__ import annotations

import logging
import pickle
from pathlib import Path
from typing import cast

import pytest
import torch

from ml_playground.framework.configuration.models import (
    ModelConfig,
    RuntimeConfig,
    SamplerConfig,
    SampleConfig,
    MetadataConfig,
)
from ml_playground.framework.sampling.runner import Sampler
from ml_playground.framework.models.core.model import GPT
from ml_playground.framework.training.checkpointing.checkpoint_manager import (
    Checkpoint,
    CheckpointManager,
)


def _write_minimal_meta(out_dir: Path) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    meta = {
        "meta_version": 1,
        "tokenizer_type": "char",
        "stoi": {"a": 1, "\n": 2},
    }
    with (out_dir / "meta.pkl").open("wb") as f:
        pickle.dump(meta, f)


def _shared_for_out_dir(out_dir: Path) -> MetadataConfig:
    return MetadataConfig(
        experiment="regression",
        config_path=out_dir / "config.toml",
        project_home=out_dir,
        dataset_dir=out_dir,
        train_out_dir=out_dir,
        sample_out_dir=out_dir,
    )


def _sampler_config(
    *, out_dir: Path, seed: int, model_cfg: ModelConfig
) -> SamplerConfig:
    runtime = RuntimeConfig(
        out_dir=out_dir,
        device="cpu",
        dtype="float32",
        seed=seed,
        compile=False,
        tensorboard_enabled=False,
    )

    cfg = SamplerConfig(
        runtime=runtime,
        sample=SampleConfig(
            start="a",
            num_samples=1,
            max_new_tokens=16,
            temperature=0.8,
            top_k=0,
            top_p=None,
        ),
    )

    def _checkpoint_load_fn(
        *, manager: CheckpointManager, cfg: SamplerConfig, logger: logging.Logger
    ) -> Checkpoint:
        _ = manager
        _ = logger
        _ = cfg
        return Checkpoint(
            model={},
            optimizer={},
            model_args=cast(dict[str, object], model_cfg.model_dump()),
            iter_num=0,
            best_val_loss=float("inf"),
            config={},
        )

    return cfg.model_copy(
        update={
            "checkpoint_load_fn": _checkpoint_load_fn,
            "model_factory": lambda mc, lg: GPT(mc, lg),
        }
    )


def test_sampler_is_deterministic_for_fixed_seed(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    out_dir = tmp_path / "out"
    _write_minimal_meta(out_dir)

    model_cfg = ModelConfig(
        n_layer=1,
        n_head=1,
        n_embd=8,
        block_size=8,
        dropout=0.0,
        vocab_size=16,
    )

    cfg = _sampler_config(out_dir=out_dir, seed=1234, model_cfg=model_cfg)
    shared = _shared_for_out_dir(out_dir)

    caplog.set_level(logging.INFO, logger="ml_playground.sampler")

    Sampler(cfg, shared).run()
    first = [r.message for r in caplog.records if r.name == "ml_playground.sampler"]

    caplog.clear()
    Sampler(cfg, shared).run()
    second = [r.message for r in caplog.records if r.name == "ml_playground.sampler"]

    assert first == second


def test_sampler_sets_model_to_eval_mode(tmp_path: Path) -> None:
    out_dir = tmp_path / "out"
    _write_minimal_meta(out_dir)

    model_cfg = ModelConfig(
        n_layer=1,
        n_head=1,
        n_embd=8,
        block_size=8,
        dropout=0.2,
        vocab_size=16,
    )

    cfg = _sampler_config(out_dir=out_dir, seed=1, model_cfg=model_cfg)
    shared = _shared_for_out_dir(out_dir)

    sampler = Sampler(cfg, shared)
    assert sampler.model.training is False


def test_generate_clamps_out_of_range_prompt_tokens() -> None:
    torch.manual_seed(0)

    model_cfg = ModelConfig(
        n_layer=1,
        n_head=1,
        n_embd=8,
        block_size=8,
        dropout=0.0,
        vocab_size=8,
    )

    model = GPT(model_cfg, logger=None)
    model.eval()

    prompt = torch.tensor([[0, 999, 3]], dtype=torch.long)
    out = model.generate(prompt, max_new_tokens=4, temperature=0.0, top_k=None)

    assert int(out.max().item()) < int(model.config.vocab_size)
