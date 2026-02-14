from __future__ import annotations

from pathlib import Path
from typing import Any, cast

import pytest

from ml_playground.framework.configuration.models import (
    MetadataConfig,
    RuntimeConfig,
    SampleConfig,
    SamplerConfig,
)
from ml_playground.framework.sampling.api import SamplingPlan, run_sampling
from ml_playground.framework.sampling.runner import Sampler


def _make_config(tmp_path: Path) -> SamplerConfig:
    return SamplerConfig(
        runtime=RuntimeConfig(out_dir=tmp_path),
        sample=SampleConfig(start="\n", num_samples=2, max_new_tokens=1),
    )


def test_run_sampling_uses_factory_and_metadata(tmp_path: Path) -> None:
    cfg = _make_config(tmp_path)
    metadata = MetadataConfig(
        experiment="unit",
        config_path=tmp_path / "cfg.toml",
        project_home=tmp_path,
        dataset_dir=tmp_path / "data",
        train_out_dir=tmp_path / "train",
        sample_out_dir=tmp_path / "sample",
    )
    calls: dict[str, Any] = {}

    def factory(config: SamplerConfig, metadata_cfg: MetadataConfig) -> Sampler:
        calls["config"] = config
        calls["metadata"] = metadata_cfg

        class _FakeSampler:
            cached_prompt_ids = (1, 2)

            def run(self) -> None:
                return None

        return cast(Sampler, _FakeSampler())

    summary = run_sampling(
        SamplingPlan(config=cfg, metadata=metadata, sampler_factory=factory)
    )

    assert calls["config"] is cfg
    assert calls["metadata"] is metadata
    assert summary.prompt_ids == (1, 2)
    assert summary.sample_count == 2
    assert summary.metadata is metadata


def test_run_sampling_requires_runtime_when_metadata_missing(tmp_path: Path) -> None:
    cfg = SamplerConfig.model_construct(
        runtime=None,
        sample=SampleConfig(start="\n", num_samples=1, max_new_tokens=1),
    )

    with pytest.raises(ValueError, match="Runtime configuration is missing"):
        run_sampling(SamplingPlan(config=cfg))
