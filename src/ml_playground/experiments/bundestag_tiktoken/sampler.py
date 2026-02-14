from __future__ import annotations

from pathlib import Path
from ml_playground.framework.configuration.models import SamplerConfig, MetadataConfig
from ml_playground.framework.experiment_registry.protocol import (
    Sampler as _SamplerProto,
    SampleReport,
)
from ml_playground.framework.sampling.runner import Sampler as _CoreSampler


class BundestagTiktokenSampler(_SamplerProto):
    def sample(self, cfg: SamplerConfig) -> SampleReport:  # type: ignore[override]
        out_dir: Path = cfg.runtime.out_dir
        shared = MetadataConfig(
            experiment="bundestag_tiktoken",
            config_path=out_dir / "cfg.toml",
            project_home=out_dir.parent,
            dataset_dir=out_dir,
            train_out_dir=out_dir,
            sample_out_dir=out_dir,
        )
        _CoreSampler(cfg, shared).run()
        return SampleReport(
            created_files=(),
            updated_files=(),
            skipped_files=(),
            messages=("[bundestag_tiktoken] sampling finished",),
        )
