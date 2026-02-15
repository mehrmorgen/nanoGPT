from __future__ import annotations

from importlib import import_module
from pathlib import Path
from typing import cast, Any

import typer

from ml_playground.framework.configuration.models import (
    MetadataConfig,
    PreparerConfig,
    SamplerConfig,
    TrainerConfig,
)
from ml_playground.framework.core.logging_protocol import LoggerLike
from ml_playground.framework.runtime.core.bootstrap import (
    CLIDependencies,
    get_cli_dependencies,
)
from ml_playground.framework.runtime.core.results import LearningModeEngine, ToolResult
from ml_playground.framework.runtime.helpers import handle_tool_result
from ml_playground.framework.sampling.api import (
    SamplerFactory,
    SamplingPlan,
    run_sampling,
)
from ml_playground.framework.training.api import (
    TrainerFactory,
    TrainingPlan,
    run_training,
)

__all__ = [
    "handle_tool_result",
    "log_directory",
    "log_command_status",
    "run_prepare_impl",
    "run_train_impl",
    "run_sample_impl",
    "run_analyze",
]


def _deps_from_ctx(ctx: typer.Context) -> CLIDependencies:
    deps = ctx.ensure_object(dict).get("deps")
    if deps is None:
        return get_cli_dependencies()
    return cast(CLIDependencies, deps)


def _as_path(value: Path | str | object) -> Path | None:
    if isinstance(value, Path):
        return value
    if isinstance(value, str):
        return Path(value)
    return None


def _coerce_metadata_config(metadata: object | None) -> MetadataConfig | None:
    if isinstance(metadata, MetadataConfig):
        return metadata
    if metadata is None:
        return None

    dataset_dir = _as_path(
        cast(Path | str | None, getattr(metadata, "dataset_dir", None))
    )
    train_out_dir = _as_path(
        cast(Path | str | None, getattr(metadata, "train_out_dir", None))
    )
    sample_out_dir = _as_path(
        cast(Path | str | None, getattr(metadata, "sample_out_dir", None))
    )
    if dataset_dir is None or train_out_dir is None or sample_out_dir is None:
        return None

    config_path = cast(Path | str | None, getattr(metadata, "config_path", None))
    resolved_config_path: Path
    if config_path is not None:
        candidate_config_path = _as_path(config_path)
        if candidate_config_path is None:
            return None
        resolved_config_path = candidate_config_path
    else:
        resolved_config_path = train_out_dir / "cfg.toml"
    project_home = cast(
        Path | str | None, getattr(metadata, "project_home", train_out_dir)
    )
    resolved_project_home = _as_path(project_home)
    if resolved_project_home is None:
        resolved_project_home = train_out_dir
    experiment = getattr(metadata, "experiment", "runtime")

    return MetadataConfig(
        experiment=experiment,
        config_path=resolved_config_path,
        project_home=resolved_project_home,
        dataset_dir=dataset_dir,
        train_out_dir=train_out_dir,
        sample_out_dir=sample_out_dir,
    )


def run_prepare_impl(
    experiment: str,
    prepare_cfg: PreparerConfig,
    config_path: Path,
    metadata: object,
    deps: CLIDependencies | None = None,
    learning_mode_engine: LearningModeEngine | None = None,
) -> ToolResult:
    """Run the full prepare flow for an experiment."""

    try:
        prepare_cfg.logger.info(f"Running pipeline for experiment: {experiment}")

        if deps is None:
            deps = get_cli_dependencies()

        preparer = _resolve_experiment_preparer(experiment)
        if preparer is not None:
            prepare_fn = getattr(preparer, "prepare", None)
            if callable(prepare_fn):
                prepare_fn(prepare_cfg)
            else:
                raise RuntimeError(
                    f"Resolved preparer for {experiment} does not implement prepare(): {type(preparer)}"
                )
        else:
            pipeline = deps.create_pipeline(prepare_cfg, metadata)  # type: ignore[reportAny]
            run_fn = getattr(pipeline, "run", None)
            if callable(run_fn):
                run_fn()
            else:
                raise RuntimeError(
                    f"Pipeline produced by factory does not have a run() method: {type(pipeline)}"
                )

        prepare_cfg.logger.info(f"Pipeline for {experiment} finished.")

        learning_info = None
        if learning_mode_engine:
            learning_info = learning_mode_engine.explain_command(
                command=experiment,
                context="data preparation",
                category="prepare",
                executed_commands=[f"prepare {experiment}"],
            )

        return ToolResult.create(
            success=True,
            exit_code=0,
            namespace="ml",
            category="prepare",
            command=experiment,
            stdout=f"Successfully prepared data for experiment: {experiment}",
            learning_info=learning_info,
        )
    except Exception as e:
        prepare_cfg.logger.error(f"Pipeline for {experiment} failed: {e}")

        learning_info = None
        if learning_mode_engine:
            learning_info = learning_mode_engine.explain_command(
                command=experiment,
                context="data preparation",
                category="prepare",
                executed_commands=[f"prepare {experiment}"],
            )

        return ToolResult.create(
            success=False,
            exit_code=1,
            namespace="ml",
            category="prepare",
            command=experiment,
            stderr=f"Pipeline preparation failed: {e}",
            learning_info=learning_info,
        )


def _resolve_experiment_preparer(
    experiment: str, *, import_fn: Any = import_module
) -> object | None:
    mod_name = f"ml_playground.experiments.{experiment}.preparer"
    try:
        mod = import_fn(mod_name)
    except ImportError:
        return None

    for attr_name in dir(mod):
        candidate = getattr(mod, attr_name, None)
        if isinstance(candidate, type):
            prepare_attr = getattr(candidate, "prepare", None)
            if callable(prepare_attr):
                try:
                    return cast(Any, candidate)()
                except TypeError:
                    continue
    return None


def _missing_runtime_message(category: str) -> str:
    if category == "train":
        return "Runtime configuration is missing for training."
    if category == "sample":
        return "Runtime configuration is missing for sampling."
    return "Runtime configuration is missing."


def run_train_impl(
    experiment: str,
    train_cfg: TrainerConfig,
    config_path: Path,
    metadata: object,
    deps: CLIDependencies | None = None,
    learning_mode_engine: LearningModeEngine | None = None,
) -> ToolResult:
    """Run the full training flow for an experiment."""

    try:
        runtime = train_cfg.runtime
        if not runtime:
            train_cfg.logger.error(_missing_runtime_message("train"))
            return ToolResult.create(
                success=False,
                exit_code=1,
                namespace="ml",
                category="train",
                command=experiment,
                stderr=_missing_runtime_message("train"),
            )

        if deps is None:
            deps = get_cli_dependencies()

        deps_global_device_setup = deps.global_device_setup
        deps_log_command_status = deps.log_command_status
        deps_trainer_factory = deps.trainer_factory

        if not callable(deps_global_device_setup) or not callable(
            deps_log_command_status
        ):
            raise RuntimeError("CLI dependencies not provided")

        deps_global_device_setup(
            str(runtime.device),
            str(runtime.dtype),
            int(runtime.seed),
        )

        train_cfg.logger.info(f"Running trainer for experiment: {experiment}")

        train_out_dir = getattr(metadata, "train_out_dir", None)
        deps_log_command_status(
            "pre-train",
            cast(MetadataConfig, metadata),
            train_out_dir,
            train_cfg.logger,
        )

        trainer_factory_value: TrainerFactory | None = None
        if deps_trainer_factory is not None:
            if not callable(deps_trainer_factory):
                raise RuntimeError("Trainer factory dependency is not callable")
            trainer_factory_value = cast(TrainerFactory, deps_trainer_factory)
        plan = TrainingPlan(
            config=train_cfg,
            metadata=_coerce_metadata_config(metadata),
            trainer_factory=trainer_factory_value,
        )
        run_training(plan)

        train_cfg.logger.info(f"Trainer for {experiment} finished.")
        deps_log_command_status(
            "post-train",
            cast(MetadataConfig, metadata),
            train_out_dir,
            train_cfg.logger,
        )

        learning_info = None
        if learning_mode_engine:
            learning_info = learning_mode_engine.explain_command(
                command=experiment,
                context="model training",
                category="train",
                executed_commands=[f"train {experiment}"],
            )

        return ToolResult.create(
            success=True,
            exit_code=0,
            namespace="ml",
            category="train",
            command=experiment,
            stdout=f"Successfully completed training for experiment: {experiment}",
            learning_info=learning_info,
        )
    except Exception as e:
        train_cfg.logger.error(f"Training for {experiment} failed: {e}")

        learning_info = None
        if learning_mode_engine:
            learning_info = learning_mode_engine.explain_command(
                command=experiment,
                context="model training",
                category="train",
                executed_commands=[f"train {experiment}"],
            )

        return ToolResult.create(
            success=False,
            exit_code=1,
            namespace="ml",
            category="train",
            command=experiment,
            stderr=f"Training failed: {e}",
            learning_info=learning_info,
        )


def run_sample_impl(
    experiment: str,
    sample_cfg: SamplerConfig,
    config_path: Path,
    metadata: object,
    deps: CLIDependencies | None = None,
    learning_mode_engine: LearningModeEngine | None = None,
) -> ToolResult:
    """Run the full sampling flow for an experiment."""

    try:
        runtime = sample_cfg.runtime
        if not runtime:
            sample_cfg.logger.error(_missing_runtime_message("sample"))
            return ToolResult.create(
                success=False,
                exit_code=1,
                namespace="ml",
                category="sample",
                command=experiment,
                stderr=_missing_runtime_message("sample"),
            )

        if deps is None:
            deps = get_cli_dependencies()

        deps_global_device_setup = deps.global_device_setup
        deps_log_command_status = deps.log_command_status
        deps_sampler_factory = deps.sampler_factory

        if not callable(deps_global_device_setup) or not callable(
            deps_log_command_status
        ):
            raise RuntimeError("CLI dependencies not provided")

        deps_global_device_setup(
            str(runtime.device),
            str(runtime.dtype),
            int(runtime.seed),
        )

        sample_cfg.logger.info(f"Running sampler for experiment: {experiment}")
        sample_out_dir = getattr(metadata, "sample_out_dir", None)
        deps_log_command_status(
            "pre-sample",
            cast(MetadataConfig, metadata),
            sample_out_dir,
            sample_cfg.logger,
        )

        sampler_factory_value = cast(SamplerFactory, deps_sampler_factory)
        plan = SamplingPlan(
            config=sample_cfg,
            metadata=_coerce_metadata_config(metadata),
            sampler_factory=sampler_factory_value,
        )
        run_sampling(plan)

        sample_cfg.logger.info(f"Sampler for {experiment} finished.")
        deps_log_command_status(
            "post-sample",
            cast(MetadataConfig, metadata),
            sample_out_dir,
            sample_cfg.logger,
        )

        learning_info = None
        if learning_mode_engine:
            learning_info = learning_mode_engine.explain_command(
                command=experiment,
                context="model sampling",
                category="sample",
                executed_commands=[f"sample {experiment}"],
            )

        return ToolResult.create(
            success=True,
            exit_code=0,
            namespace="ml",
            category="sample",
            command=experiment,
            stdout=f"Successfully completed sampling for experiment: {experiment}",
            learning_info=learning_info,
        )
    except Exception as e:
        sample_cfg.logger.error(f"Sampling for {experiment} failed: {e}")

        learning_info = None
        if learning_mode_engine:
            learning_info = learning_mode_engine.explain_command(
                command=experiment,
                context="model sampling",
                category="sample",
                executed_commands=[f"sample {experiment}"],
            )

        return ToolResult.create(
            success=False,
            exit_code=1,
            namespace="ml",
            category="sample",
            command=experiment,
            stderr=f"Sampling failed: {e}",
            learning_info=learning_info,
        )


def log_directory(
    tag: str, dir_name: str, dir_path: object, logger: LoggerLike
) -> None:
    """Log information about a directory path."""
    if dir_path is None:
        logger.info(f"[{tag}] {dir_name}: <not set>")
        return

    # Runtime guard: tests may pass non-Path via object; avoid attribute errors
    if not isinstance(dir_path, Path):
        return

    if dir_path.exists():
        try:
            contents = sorted([p.name for p in dir_path.iterdir()])
            logger.info(f"[{tag}] {dir_name} (exists): {dir_path}")
            logger.info(f"[{tag}]   Contents: {contents}")
        except OSError:
            logger.info(f"[{tag}] {dir_name} (exists): {dir_path}")
    else:
        logger.info(f"[{tag}] {dir_name} (missing): {dir_path}")


def log_command_status(
    tag: str,
    metadata: MetadataConfig,
    out_dir: Path | None,
    logger: LoggerLike,
) -> None:
    """Log known file-based artifacts for the given config."""
    try:
        log_directory(tag, "out_dir", out_dir, logger)
        log_directory(tag, "dataset_dir", metadata.dataset_dir, logger)
    except Exception as e:
        logger.warning(f"[{tag}] Failed to log artifacts: {e}", exc_info=True)


def run_analyze(
    experiment: str,
    host: str,
    port: int,
    open_browser: bool,
    learning_mode_engine: LearningModeEngine | None = None,
) -> ToolResult:
    """Run analysis for an experiment (bundestag_char only)."""
    try:
        if experiment != "bundestag_char":
            return ToolResult.create(
                success=False,
                exit_code=1,
                namespace="ml",
                category="analyze",
                command=experiment,
                stderr=f"analyze currently supports only 'bundestag_char', got: {experiment}",
            )

        import logging as cli_logging

        pkg_name = "ml_playground.runtime_cli"

        raw_logger = cli_logging.getLogger(pkg_name)
        raw_logger.info(
            "Analysis for '%s' not implemented. Host=%s, Port=%s, Open=%s",
            experiment,
            host,
            port,
            open_browser,
        )

        learning_info = None
        if learning_mode_engine:
            learning_info = learning_mode_engine.explain_command(
                command=experiment,
                context="model analysis",
                category="analyze",
                executed_commands=[f"analyze {experiment}"],
            )

        return ToolResult.create(
            success=True,
            exit_code=0,
            namespace="ml",
            category="analyze",
            command=experiment,
            stdout=(
                f"Analysis placeholder executed for {experiment} (Host={host}, Port={port}, Open={open_browser})"
            ),
            learning_info=learning_info,
        )
    except Exception as e:
        return ToolResult.create(
            success=False,
            exit_code=1,
            namespace="ml",
            category="analyze",
            command=experiment,
            stderr=f"Analysis failed: {e}",
        )
