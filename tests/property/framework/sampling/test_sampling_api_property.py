from __future__ import annotations

from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any, cast

from hypothesis import given, settings
from hypothesis import strategies as st

from ml_playground.framework.configuration.models import (
    MetadataConfig,
    RuntimeConfig,
    SampleConfig,
    SamplerConfig,
)
from ml_playground.framework.sampling.api import SamplingPlan, run_sampling
from ml_playground.framework.sampling.runner import Sampler

_SAFE_SUBDIR = st.from_regex(r"[A-Za-z0-9_-]{1,8}", fullmatch=True)


@given(  # type: ignore[reportAny]
    subdir=_SAFE_SUBDIR
)
@settings(max_examples=10, deadline=None, derandomize=True)
def test_run_sampling_creates_metadata_when_missing(subdir: str) -> None:
    with TemporaryDirectory() as tmpdir:
        out_dir = (Path(tmpdir) / subdir).resolve()
        cfg = SamplerConfig(
            runtime=RuntimeConfig(out_dir=out_dir),
            sample=SampleConfig(start="\n", num_samples=1, max_new_tokens=1),
        )

        def factory(config: SamplerConfig, metadata: MetadataConfig) -> Sampler:
            class _FakeSampler:
                cached_prompt_ids = None

                def run(self) -> None:
                    return None

            return cast(Any, _FakeSampler())

        summary = run_sampling(SamplingPlan(config=cfg, sampler_factory=factory))

        assert summary.metadata.train_out_dir.resolve() == out_dir
        assert summary.metadata.sample_out_dir.resolve() == out_dir
        assert summary.metadata.dataset_dir.resolve() == out_dir
        assert summary.metadata.config_path.resolve() == out_dir / "cfg.toml"
        assert out_dir.exists()


@given(  # type: ignore[reportAny]
    num_samples=st.integers(min_value=1, max_value=5),
    prompt_ids=st.lists(st.integers(min_value=0, max_value=10), min_size=0, max_size=5),
)
@settings(max_examples=10, deadline=None, derandomize=True)
def test_run_sampling_summary_tracks_factory_output(
    num_samples: int, prompt_ids: list[int]
) -> None:
    with TemporaryDirectory() as tmpdir:
        out_dir = Path(tmpdir) / "out"
        cfg = SamplerConfig(
            runtime=RuntimeConfig(out_dir=out_dir),
            sample=SampleConfig(start="\n", num_samples=num_samples, max_new_tokens=1),
        )

        def factory(config: SamplerConfig, metadata: MetadataConfig) -> Sampler:
            class _FakeSampler:
                cached_prompt_ids = tuple(prompt_ids)

                def run(self) -> None:
                    return None

            return cast(Any, _FakeSampler())

        summary = run_sampling(SamplingPlan(config=cfg, sampler_factory=factory))

        assert summary.prompt_ids == tuple(prompt_ids)
        assert summary.sample_count == num_samples
