from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, Callable, cast

import pytest
from typer.testing import CliRunner

from ml_playground.framework.runtime.core.bootstrap import CLIDependencies
from ml_playground.runtime_cli.main import app
from ml_playground.tools.core.interfaces import ToolResult
from ml_playground.framework.configuration import loading as config_loading
from ml_playground.framework.configuration.models import (
    DataConfig,
    ExperimentConfig,
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

runner = CliRunner()


def _normalize_cli_path(path: Path) -> Path:
    if path.is_absolute():
        try:
            return Path("/") / path.relative_to("/private")
        except ValueError:
            return path
    return path


def _build_shared(tmp_path: Path, experiment: str = "demo") -> MetadataConfig:
    dataset_dir = tmp_path / "dataset"
    dataset_dir.mkdir(parents=True, exist_ok=True)
    (dataset_dir / "meta.pkl").write_text("meta", encoding="utf-8")

    train_out = tmp_path / "train_out"
    sample_out = tmp_path / "sample_out"
    train_out.mkdir(parents=True, exist_ok=True)
    sample_out.mkdir(parents=True, exist_ok=True)

    config_path = tmp_path / f"{experiment}.toml"
    config_path.write_text("{}", encoding="utf-8")

    return MetadataConfig(
        experiment=experiment,
        config_path=config_path,
        project_home=tmp_path,
        dataset_dir=dataset_dir,
        train_out_dir=train_out,
        sample_out_dir=sample_out,
    )


def _build_experiment(shared: MetadataConfig) -> ExperimentConfig:
    return ExperimentConfig(
        prepare=PreparerConfig(),
        training=TrainerConfig(
            model=ModelConfig(),
            data=DataConfig(),
            optim=OptimConfig(),
            schedule=LRSchedule(),
            runtime=RuntimeConfig(out_dir=shared.train_out_dir),
        ),
        sampling=SamplerConfig(
            runtime=RuntimeConfig(out_dir=shared.sample_out_dir),
            sample=SampleConfig(start="X"),
        ),
        metadata=shared,
    )


def _deps(
    *,
    load: Callable[[str, Path | None], ExperimentConfig],
    ensure_train: Callable[[ExperimentConfig], object] | None = None,
    ensure_sample: Callable[[ExperimentConfig], object] | None = None,
    run_prepare: Callable[[str, PreparerConfig, Path, MetadataConfig], None]
    | None = None,
    run_train: Callable[[str, TrainerConfig, Path, MetadataConfig], None] | None = None,
    run_sample: Callable[[str, SamplerConfig, Path, MetadataConfig], None]
    | None = None,
) -> CLIDependencies:
    def _ensure_train(exp_cfg: ExperimentConfig) -> object:
        if ensure_train is None:
            return None
        return ensure_train(exp_cfg)

    def _ensure_sample(exp_cfg: ExperimentConfig) -> object:
        if ensure_sample is None:
            return None
        return ensure_sample(exp_cfg)

    def _run_prepare_inner(
        experiment: str,
        prepare_cfg: PreparerConfig,
        config_path: Path,
        shared_cfg: MetadataConfig,
        _deps: CLIDependencies,
        learning_engine: Any = None,
    ) -> ToolResult:
        if run_prepare is not None:
            run_prepare(experiment, prepare_cfg, config_path, shared_cfg)
        return ToolResult.create(
            success=True,
            exit_code=0,
            namespace="ml",
            category="prepare",
            command=experiment,
        )

    def _run_train_inner(
        experiment: str,
        train_cfg: TrainerConfig,
        config_path: Path,
        shared_cfg: MetadataConfig,
        _deps: CLIDependencies,
        learning_engine: Any = None,
    ) -> ToolResult:
        if run_train is not None:
            run_train(experiment, train_cfg, config_path, shared_cfg)
        return ToolResult.create(
            success=True,
            exit_code=0,
            namespace="ml",
            category="train",
            command=experiment,
        )

    def _run_sample_inner(
        experiment: str,
        sample_cfg: SamplerConfig,
        config_path: Path,
        shared_cfg: MetadataConfig,
        _deps: CLIDependencies,
        learning_engine: Any = None,
    ) -> ToolResult:
        if run_sample is not None:
            run_sample(experiment, sample_cfg, config_path, shared_cfg)
        return ToolResult.create(
            success=True,
            exit_code=0,
            namespace="ml",
            category="sample",
            command=experiment,
        )

    return CLIDependencies(
        load_experiment=load,
        ensure_train_prerequisites=_ensure_train,
        ensure_sample_prerequisites=_ensure_sample,
        run_prepare=_run_prepare_inner,
        run_train=_run_train_inner,
        run_sample=_run_sample_inner,
    )


def test_prepare_command_invokes_injected_runner(tmp_path: Path) -> None:
    """Test prepare command invokes injected runner."""
    shared = _build_shared(tmp_path, "prep")
    experiment = _build_experiment(shared)
    calls: list[str] = []

    def load(_experiment: str, _config: Path | None) -> ExperimentConfig:
        return experiment

    def run_prepare(
        name: str,
        cfg: PreparerConfig,
        config_path: Path,
        shared_cfg: MetadataConfig,
    ) -> None:
        calls.append(name)
        assert cfg is experiment.prepare
        assert shared_cfg is shared
        assert config_path == _normalize_cli_path(shared.config_path)

    deps = _deps(load=load, run_prepare=run_prepare)
    result = runner.invoke(app, ["prepare", "prep"], obj={"cli_deps": deps})

    assert result.exit_code == 0
    assert calls == ["prep"]


def test_prepare_command_propagates_loader_error(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """Test prepare command propagates loader error."""
    caplog.set_level(logging.ERROR, logger="ml_playground.runtime_cli")

    def load(_name: str, _config: Path | None) -> ExperimentConfig:
        raise FileNotFoundError("Config missing")

    deps = _deps(load=load)
    result = runner.invoke(app, ["prepare", "demo"], obj={"cli_deps": deps})

    assert result.exit_code == 1
    assert any(
        "Config missing" in message
        for message in cast(list[str], cast(Any, caplog).messages)
    )


def test_train_command_invokes_injected_runner(tmp_path: Path) -> None:
    """Test train command invokes injected runner."""
    shared = _build_shared(tmp_path, "train-exp")
    experiment = _build_experiment(shared)
    calls: list[str] = []

    def load(_experiment: str, _config: Path | None) -> ExperimentConfig:
        return experiment

    def ensure_train_prereqs(exp_cfg: ExperimentConfig) -> Path:
        return exp_cfg.metadata.dataset_dir / "meta.pkl"

    def run_train(
        name: str,
        cfg: TrainerConfig,
        config_path: Path,
        shared_cfg: MetadataConfig,
    ) -> None:
        calls.append(name)
        assert cfg is experiment.training
        assert shared_cfg is shared
        assert config_path == _normalize_cli_path(shared.config_path)

    deps = _deps(
        load=load,
        ensure_train=ensure_train_prereqs,
        run_train=run_train,
    )
    result = runner.invoke(app, ["train", "train-exp"], obj={"cli_deps": deps})

    assert result.exit_code == 0
    assert calls == ["train-exp"]


def test_train_command_propagates_loader_error(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """Test train command propagates loader error."""
    caplog.set_level(logging.ERROR, logger="ml_playground.runtime_cli")

    def load(_name: str, _config: Path | None) -> ExperimentConfig:
        raise ValueError("Invalid train config")

    deps = _deps(load=load)
    result = runner.invoke(app, ["train", "broken"], obj={"cli_deps": deps})

    assert result.exit_code == 1
    assert any(
        "Invalid train config" in message
        for message in cast(list[str], cast(Any, caplog).messages)
    )


def test_sample_command_invokes_injected_runner(tmp_path: Path) -> None:
    """Test sample command invokes injected runner."""
    shared = _build_shared(tmp_path, "sample-exp")
    experiment = _build_experiment(shared)
    calls: list[str] = []

    def load(_experiment: str, _config: Path | None) -> ExperimentConfig:
        return experiment

    def ensure_sample_prereqs(exp_cfg: ExperimentConfig) -> tuple[Path, Path]:
        dataset_meta = exp_cfg.metadata.dataset_dir / "meta.pkl"
        sample_meta = (
            exp_cfg.metadata.sample_out_dir / exp_cfg.metadata.experiment / "meta.pkl"
        )
        return dataset_meta, sample_meta

    def run_sample(
        name: str,
        cfg: SamplerConfig,
        config_path: Path,
        shared_cfg: MetadataConfig,
    ) -> None:
        calls.append(name)
        assert cfg.sample.start == "X"
        assert shared_cfg is shared
        assert config_path == _normalize_cli_path(shared.config_path)

    deps = _deps(
        load=load,
        ensure_sample=ensure_sample_prereqs,
        run_sample=run_sample,
    )
    result = runner.invoke(app, ["sample", "sample-exp"], obj={"cli_deps": deps})

    assert result.exit_code == 0
    assert calls == ["sample-exp"]


def test_sample_command_propagates_loader_error(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """Test sample command propagates loader error."""
    caplog.set_level(logging.ERROR, logger="ml_playground.runtime_cli")

    def load(_name: str, _config: Path | None) -> ExperimentConfig:
        raise ValueError("Missing sample config")

    deps = _deps(load=load)
    result = runner.invoke(app, ["sample", "demo"], obj={"cli_deps": deps})

    assert result.exit_code == 1
    assert any(
        "Missing sample config" in message
        for message in cast(list[str], cast(Any, caplog).messages)
    )


def test_analyze_command_rejects_non_bundestag() -> None:
    """Test analyze command rejects non bundestag."""
    result = runner.invoke(app, ["analyze", "other"])
    assert result.exit_code == 1


def test_analyze_command_logs_message(caplog: pytest.LogCaptureFixture) -> None:
    """Test analyze command logs message."""
    caplog.set_level(logging.INFO, logger="ml_playground.runtime_cli")
    result = runner.invoke(app, ["analyze", "bundestag_char"])
    assert result.exit_code == 0
    assert any(
        "not implemented" in msg.lower()
        for msg in cast(list[str], cast(Any, caplog).messages)
    )


def test_global_option_missing_exp_config_exits(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """Test global option missing exp config exits."""
    caplog.set_level(logging.ERROR, logger="ml_playground.runtime_cli")
    missing = tmp_path / "missing.toml"

    shared = _build_shared(tmp_path)
    experiment = _build_experiment(shared)

    def load(_experiment: str, _config: Path | None) -> ExperimentConfig:
        return experiment

    deps = _deps(load=load)

    result = runner.invoke(
        app, ["--exp-config", str(missing), "prepare", "demo"], obj={"cli_deps": deps}
    )

    assert result.exit_code == 2
    assert any(
        "config file not found" in message.lower()
        for message in cast(list[str], cast(Any, caplog).messages)
    )


def test_cli_help_lists_core_commands() -> None:
    """Test cli help lists core commands."""
    result = runner.invoke(app, ["--help"])
    assert result.exit_code == 0
    out = result.stdout.lower()
    assert all(name in out for name in ("prepare", "train", "sample", "analyze"))


def test_config_loader_missing_sample_section(tmp_path: Path) -> None:
    """Test config loader missing sample section."""
    cfg_path = tmp_path / "config.toml"
    cfg_path.write_text("[training]\n", encoding="utf-8")

    with pytest.raises(ValueError, match=r"must contain a \[sampling\] section"):
        config_loading.load_sample_config(cfg_path)
