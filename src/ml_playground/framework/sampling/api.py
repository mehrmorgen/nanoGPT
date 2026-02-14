from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Callable, cast

from ml_playground.framework.configuration.models import (
    MetadataConfig,
    RuntimeConfig,
    SamplerConfig,
)
from ml_playground.framework.sampling.runner import Sampler, SamplerDependencies


def _build_sampling_metadata(runtime_out_dir: Path) -> MetadataConfig:
    runtime_out_dir.mkdir(parents=True, exist_ok=True)
    return MetadataConfig(
        experiment="runtime",
        config_path=runtime_out_dir / "cfg.toml",
        project_home=runtime_out_dir,
        dataset_dir=runtime_out_dir,
        train_out_dir=runtime_out_dir,
        sample_out_dir=runtime_out_dir,
    )


SamplerFactory = Callable[[SamplerConfig, MetadataConfig], Sampler]


@dataclass(frozen=True)
class SamplingPlan:
    config: SamplerConfig
    metadata: MetadataConfig | None = None
    deps: SamplerDependencies | None = None
    sampler_factory: SamplerFactory | None = None


@dataclass(frozen=True)
class SamplingSummary:
    prompt_ids: tuple[int, ...] | None
    sample_count: int
    metadata: MetadataConfig


class SamplingRunner:
    def __init__(self, plan: SamplingPlan) -> None:
        self.plan = plan
        self.metadata: MetadataConfig | None = plan.metadata
        self.deps = plan.deps
        self.sampler: Sampler | None = None
        self.summary: SamplingSummary | None = None

    def _ensure_metadata(self) -> MetadataConfig:
        if self.metadata is not None:
            return self.metadata
        runtime = cast(RuntimeConfig | None, getattr(self.plan.config, "runtime", None))
        if runtime is None:
            raise ValueError("Runtime configuration is missing")
        runtime_metadata = _build_sampling_metadata(runtime.out_dir)
        self.metadata = runtime_metadata
        return runtime_metadata

    def run(self) -> SamplingSummary:
        metadata_cfg = self._ensure_metadata()
        factory = self.plan.sampler_factory or Sampler
        sampler = factory(self.plan.config, metadata_cfg)  # type: ignore[call-arg]
        self.sampler = sampler
        sampler.run()
        prompt_ids = getattr(sampler, "cached_prompt_ids", None)
        sample_cfg = self.plan.config.sample
        summary = SamplingSummary(
            prompt_ids=prompt_ids,
            sample_count=sample_cfg.num_samples,
            metadata=metadata_cfg,
        )
        self.summary = summary
        return summary


def run_sampling(plan: SamplingPlan) -> SamplingSummary:
    runner = SamplingRunner(plan)
    return runner.run()


__all__ = [
    "SamplingPlan",
    "SamplingRunner",
    "SamplingSummary",
    "run_sampling",
    "SamplerFactory",
]
