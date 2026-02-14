"""Shared test configuration builders for consistent test setup."""

from pathlib import Path

from ml_playground.framework.configuration.models import (
    DataConfig,
    LRSchedule,
    ModelConfig,
    OptimConfig,
    PreparerConfig,
    RuntimeConfig,
    SampleConfig,
    SamplerConfig,
    MetadataConfig,
    TrainerConfig,
)

__all__ = [
    "create_basic_configs",
    "create_metadata_config",
]


def create_basic_configs(
    tmp_path: Path,
) -> tuple[PreparerConfig, TrainerConfig, SamplerConfig, MetadataConfig]:
    """Create strict-ready configs for prepare/train/sample flows."""

    raw_dir = tmp_path / "raw"
    raw_dir.mkdir(parents=True, exist_ok=True)
    raw_text = raw_dir / "input.txt"
    raw_text.write_text("dummy", encoding="utf-8")

    dataset_dir = tmp_path / "datasets"
    dataset_dir.mkdir(parents=True, exist_ok=True)

    train_out = tmp_path / "train"
    train_out.mkdir(parents=True, exist_ok=True)
    sample_out = tmp_path / "sample"
    sample_out.mkdir(parents=True, exist_ok=True)

    prep_cfg = PreparerConfig.model_validate(
        {"raw_dir": raw_dir, "raw_text_path": raw_text}
    )

    tcfg = TrainerConfig(
        model=ModelConfig(),
        data=DataConfig(),
        optim=OptimConfig(),
        schedule=LRSchedule(),
        runtime=RuntimeConfig(out_dir=train_out),
        hf_model=TrainerConfig.HFModelConfig(
            model_name="hf/model",
            gradient_checkpointing=False,
            block_size=128,
        ),
        peft=TrainerConfig.PeftConfig(enabled=False),
    )
    scfg = SamplerConfig(
        runtime=RuntimeConfig(out_dir=sample_out),
        sample=SampleConfig(),
    )
    shared = MetadataConfig(
        experiment="exp",
        config_path=tmp_path / "cfg.toml",
        project_home=tmp_path,
        dataset_dir=dataset_dir,
        train_out_dir=train_out,
        sample_out_dir=sample_out,
    )

    return prep_cfg, tcfg, scfg, shared


def create_metadata_config(
    base_dir: Path,
    *,
    experiment: str = "demo",
    mkdir: bool = True,
    train_out_dir: Path | None = None,
    sample_out_dir: Path | None = None,
) -> MetadataConfig:
    """Build a ``MetadataConfig`` rooted at *base_dir*.

    Args:
        base_dir: Root temporary directory (typically ``tmp_path``).
        experiment: Experiment name stored in the config.
        mkdir: When ``True`` (default), create subdirectories and write a
            stub ``config.toml``.  Set to ``False`` if the caller manages
            the directory layout itself.
        train_out_dir: Override for ``train_out_dir``; defaults to
            ``base_dir / "train"``.
        sample_out_dir: Override for ``sample_out_dir``; defaults to
            ``base_dir / "sample"``.

    Returns:
        A fully-populated ``MetadataConfig``.
    """
    dataset_dir = base_dir / "dataset"
    _train = train_out_dir or base_dir / "train"
    _sample = sample_out_dir or base_dir / "sample"
    config_path = base_dir / "config.toml"

    if mkdir:
        dataset_dir.mkdir(parents=True, exist_ok=True)
        _train.mkdir(parents=True, exist_ok=True)
        _sample.mkdir(parents=True, exist_ok=True)
        config_path.write_text("{}", encoding="utf-8")

    return MetadataConfig(
        experiment=experiment,
        config_path=config_path,
        project_home=base_dir,
        dataset_dir=dataset_dir,
        train_out_dir=_train,
        sample_out_dir=_sample,
    )
