from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable

from ml_playground.core.logging_protocol import LoggerLike
from ml_playground.data_pipeline.preparer import create_pipeline
from ml_playground.sampling.runner import Sampler
from ml_playground.training.loop.runner import Trainer as CoreTrainer
from ml_playground.runtime.core.results import LearningModeEngine, ToolResult
from ml_playground.runtime.device import global_device_setup
from ml_playground.runtime.helpers import log_command_status
from ml_playground.runtime.protocols import (
    PrepareConfigLike,
    SampleConfigLike,
    TrainConfigLike,
)

# Constants for error messages
_MISSING_TRAIN_RUNTIME_MSG = "Runtime configuration is missing for training."
_MISSING_SAMPLE_RUNTIME_MSG = "Runtime configuration is missing for sampling."


@dataclass(frozen=True)
class RuntimeRunHooks:
    """Injectable hooks for runtime execution flows."""

    pipeline_factory: Callable[[Any, Any], Any]
    trainer_factory: Callable[[Any, Any], Any]
    sampler_factory: Callable[[Any, Any], Any]
    device_setup: Callable[[str, str, int], None]
    log_status: Callable[[str, Any, Path | None, LoggerLike], None]
    resolve_seed: Callable[[str, Any, int], int | None] | None = None


def _default_runtime_run_hooks() -> RuntimeRunHooks:
    return RuntimeRunHooks(
        pipeline_factory=create_pipeline,
        trainer_factory=CoreTrainer,
        sampler_factory=Sampler,
        device_setup=global_device_setup,
        log_status=log_command_status,
    )


_DEFAULT_RUNTIME_RUN_HOOKS = _default_runtime_run_hooks()


def run_prepare_impl(
    experiment: str,
    prepare_cfg: PrepareConfigLike,
    config_path: Path,
    shared: Any,
    learning_mode_engine: LearningModeEngine | None = None,
    *,
    hooks: RuntimeRunHooks | None = None,
) -> ToolResult:
    """Run the full prepare flow for an experiment."""
    active_hooks = hooks or _DEFAULT_RUNTIME_RUN_HOOKS
    try:
        prepare_cfg.logger.info(f"Running pipeline for experiment: {experiment}")
        pipeline = active_hooks.pipeline_factory(prepare_cfg, shared)
        pipeline.run()
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


def run_train_impl(
    experiment: str,
    train_cfg: TrainConfigLike,
    config_path: Path,
    shared: Any,
    learning_mode_engine: LearningModeEngine | None = None,
    *,
    hooks: RuntimeRunHooks | None = None,
) -> ToolResult:
    """Run the full training flow for an experiment."""
    active_hooks = hooks or _DEFAULT_RUNTIME_RUN_HOOKS
    try:
        if not train_cfg.runtime:
            train_cfg.logger.error(_MISSING_TRAIN_RUNTIME_MSG)
            return ToolResult.create(
                success=False,
                exit_code=1,
                namespace="ml",
                category="train",
                command=experiment,
                stderr=_MISSING_TRAIN_RUNTIME_MSG,
            )

        device_seed = train_cfg.runtime.seed
        seed_resolver = getattr(active_hooks, "resolve_seed", None)
        if callable(seed_resolver):
            resolved = seed_resolver("train", shared, train_cfg.runtime.seed)
            if isinstance(resolved, int):
                device_seed = resolved
        active_hooks.device_setup(
            train_cfg.runtime.device,
            train_cfg.runtime.dtype,
            device_seed,
        )

        train_cfg.logger.info(f"Running trainer for experiment: {experiment}")
        train_out_dir = getattr(shared, "train_out_dir", None)
        active_hooks.log_status("pre-train", shared, train_out_dir, train_cfg.logger)

        trainer = active_hooks.trainer_factory(train_cfg, shared)
        trainer.run()

        train_cfg.logger.info(f"Trainer for {experiment} finished.")
        active_hooks.log_status("post-train", shared, train_out_dir, train_cfg.logger)

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
    sample_cfg: SampleConfigLike,
    config_path: Path,
    shared: Any,
    learning_mode_engine: LearningModeEngine | None = None,
    *,
    hooks: RuntimeRunHooks | None = None,
) -> ToolResult:
    """Run the full sampling flow for an experiment."""
    active_hooks = hooks or _DEFAULT_RUNTIME_RUN_HOOKS
    try:
        if not sample_cfg.runtime:
            sample_cfg.logger.error(_MISSING_SAMPLE_RUNTIME_MSG)
            return ToolResult.create(
                success=False,
                exit_code=1,
                namespace="ml",
                category="sample",
                command=experiment,
                stderr=_MISSING_SAMPLE_RUNTIME_MSG,
            )

        device_seed = sample_cfg.runtime.seed
        seed_resolver = getattr(active_hooks, "resolve_seed", None)
        if callable(seed_resolver):
            resolved = seed_resolver("sample", shared, sample_cfg.runtime.seed)
            if isinstance(resolved, int):
                device_seed = resolved
        active_hooks.device_setup(
            sample_cfg.runtime.device,
            sample_cfg.runtime.dtype,
            device_seed,
        )

        sample_cfg.logger.info(f"Running sampler for experiment: {experiment}")
        sample_out_dir = getattr(shared, "sample_out_dir", None)
        active_hooks.log_status("pre-sample", shared, sample_out_dir, sample_cfg.logger)
        sampler = active_hooks.sampler_factory(sample_cfg, shared)
        sampler.run()
        sample_cfg.logger.info(f"Sampler for {experiment} finished.")
        active_hooks.log_status(
            "post-sample", shared, sample_out_dir, sample_cfg.logger
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


def run_analyze(
    experiment: str,
    host: str,
    port: int,
    open_browser: bool,
    learning_mode_engine: LearningModeEngine | None = None,
    *,
    logger_factory: Callable[[str], Any] | None = None,
) -> ToolResult:
    """Run analysis for an experiment.

    Only 'bundestag_char' is currently supported.
    """
    from logging import getLogger

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

        logger_name = "ml_playground.runtime.cli"
        logger_getter = logger_factory or getLogger
        logger = logger_getter(logger_name)
        logger.info(
            f"Analysis for '{experiment}' not implemented. Host={host}, Port={port}, Open={open_browser}"
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
            stdout=f"Analysis placeholder executed for {experiment} (Host={host}, Port={port}, Open={open_browser})",
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
