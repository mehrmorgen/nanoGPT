from __future__ import annotations
from pathlib import Path
import sys
import importlib
from typing import Any

import typer

from ml_playground.configuration.models import (
    PreparerConfig,
    SamplerConfig,
    SharedConfig,
    TrainerConfig,
)
from ml_playground.core.logging_protocol import LoggerLike
from ml_playground.runtime.core.results import LearningModeEngine, ToolResult

__all__ = [
    "handle_tool_result",
    "log_directory",
    "log_command_status",
    "run_prepare_impl",
    "run_train_impl",
    "run_sample_impl",
]


def handle_tool_result(result: ToolResult, learning_mode: bool) -> None:
    """Handle ToolResult output and exit appropriately."""
    if result.stdout:
        typer.echo(result.stdout)
    if result.stderr:
        typer.echo(result.stderr, err=True)

    if learning_mode and result.learning_info:
        if result.learning_info.explanations:
            typer.echo("\n📚 Learning Mode - What this command does:")
            for explanation in result.learning_info.explanations:
                typer.echo(f"  • {explanation}")

        if result.learning_info.best_practices:
            typer.echo("\n💡 Best Practices:")
            for practice in result.learning_info.best_practices:
                typer.echo(f"  • {practice}")

        if result.learning_info.related_concepts:
            typer.echo("\n🔗 Related Concepts:")
            for concept in result.learning_info.related_concepts:
                typer.echo(f"  • {concept}")

    if not result.success:
        raise typer.Exit(result.exit_code)


def _cli_module():
    mod = sys.modules.get("ml_playground.runtime.cli")
    if mod is None:
        mod = importlib.import_module("ml_playground.runtime.cli")
    return mod


def run_prepare_impl(
    experiment: str,
    prepare_cfg: PreparerConfig,
    config_path: Path,
    shared: Any,
    learning_mode_engine: LearningModeEngine | None = None,
) -> ToolResult:
    """Run the full prepare flow for an experiment."""
    try:
        cli_pkg = _cli_module()

        prepare_cfg.logger.info(f"Running pipeline for experiment: {experiment}")
        pipeline = cli_pkg.create_pipeline(prepare_cfg, shared)
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
    shared: Any,
    learning_mode_engine: LearningModeEngine | None = None,
) -> ToolResult:
    """Run the full training flow for an experiment."""
    try:
        cli_pkg = _cli_module()

        if not train_cfg.runtime:
            train_cfg.logger.error(_missing_runtime_message("train"))
            return ToolResult.create(
                success=False,
                exit_code=1,
                namespace="ml",
                category="train",
                command=experiment,
                stderr=_missing_runtime_message("train"),
            )

        cli_pkg.global_device_setup(
            train_cfg.runtime.device,
            train_cfg.runtime.dtype,
            train_cfg.runtime.seed,
        )

        train_cfg.logger.info(f"Running trainer for experiment: {experiment}")
        cli_pkg.log_command_status(
            "pre-train", shared, shared.train_out_dir, train_cfg.logger
        )

        trainer_cls = getattr(cli_pkg, "CoreTrainer")
        trainer = trainer_cls(train_cfg, shared)
        trainer.run()

        train_cfg.logger.info(f"Trainer for {experiment} finished.")
        cli_pkg.log_command_status(
            "post-train", shared, shared.train_out_dir, train_cfg.logger
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
    shared: Any,
    learning_mode_engine: LearningModeEngine | None = None,
) -> ToolResult:
    """Run the full sampling flow for an experiment."""

    try:
        cli_pkg = _cli_module()

        if not sample_cfg.runtime:
            sample_cfg.logger.error(_missing_runtime_message("sample"))
            return ToolResult.create(
                success=False,
                exit_code=1,
                namespace="ml",
                category="sample",
                command=experiment,
                stderr=_missing_runtime_message("sample"),
            )

        cli_pkg.global_device_setup(
            sample_cfg.runtime.device,
            sample_cfg.runtime.dtype,
            sample_cfg.runtime.seed,
        )

        sample_cfg.logger.info(f"Running sampler for experiment: {experiment}")
        cli_pkg.log_command_status(
            "pre-sample", shared, shared.sample_out_dir, sample_cfg.logger
        )
        sampler_cls = getattr(cli_pkg, "Sampler")
        sampler = sampler_cls(sample_cfg, shared)
        sampler.run()
        sample_cfg.logger.info(f"Sampler for {experiment} finished.")
        cli_pkg.log_command_status(
            "post-sample", shared, shared.sample_out_dir, sample_cfg.logger
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


def log_directory(tag: str, dir_name: str, dir_path: Any, logger: LoggerLike) -> None:
    """Log information about a directory path."""
    if dir_path is None:
        logger.info(f"[{tag}] {dir_name}: <not set>")
        return

    # Runtime guard: tests may pass non-Path via Any; avoid attribute errors
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
    shared: SharedConfig,
    out_dir: Path,
    logger: LoggerLike,
) -> None:
    """Log known file-based artifacts for the given config."""
    try:
        cli_pkg = _cli_module()
        cli_pkg.log_directory(tag, "out_dir", out_dir, logger)
        cli_pkg.log_directory(tag, "dataset_dir", shared.dataset_dir, logger)
    except (OSError, ValueError, TypeError, AttributeError):
        pass


def run_analyze(
    experiment: str,
    host: str,
    port: int,
    open_browser: bool,
    learning_mode_engine: LearningModeEngine | None = None,
) -> ToolResult:
    """Run analysis for an experiment (bundestag_char only)."""
    try:
        cli_pkg = _cli_module()

        if experiment != "bundestag_char":
            return ToolResult.create(
                success=False,
                exit_code=1,
                namespace="ml",
                category="analyze",
                command=experiment,
                stderr=f"analyze currently supports only 'bundestag_char', got: {experiment}",
            )

        try:
            logger = cli_pkg.logging.getLogger(cli_pkg.__name__)
            logger.info(
                "Analysis for '%s' not implemented. Host=%s, Port=%s, Open=%s",
                experiment,
                host,
                port,
                open_browser,
            )
        except Exception:
            return ToolResult.create(
                success=False,
                exit_code=1,
                namespace="ml",
                category="analyze",
                command=experiment,
                stderr="Analysis failed: logging unavailable",
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
