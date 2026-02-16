from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import time
from typing import Any, Callable, cast, Optional
import webbrowser

from ml_playground.framework.analysis.lit.integration import run_server_bundestag_char

from ml_playground.framework.core.logging_protocol import LoggerLike
from ml_playground.framework.core.error_handling import (
    DataError,
    FileOperationError,
    CheckpointError,
)
from ml_playground.framework.data_pipeline.preparer import create_pipeline
from ml_playground.framework.sampling.runner import Sampler
from ml_playground.framework.training.loop.runner import Trainer as CoreTrainer
from ml_playground.framework.runtime.core.results import LearningModeEngine, ToolResult
from ml_playground.framework.runtime.device import global_device_setup
from ml_playground.framework.runtime.helpers import log_command_status
from ml_playground.framework.runtime.protocols import (
    PrepareConfigLike,
    SampleConfigLike,
    TrainConfigLike,
    DeviceSetup,
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
    device_setup: DeviceSetup
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
    metadata: object,
    learning_mode_engine: LearningModeEngine | None = None,
    *,
    hooks: RuntimeRunHooks | None = None,
) -> ToolResult:
    """Run the full prepare flow for an experiment."""
    active_hooks = hooks or _DEFAULT_RUNTIME_RUN_HOOKS
    try:
        prepare_cfg.logger.info(f"Running pipeline for experiment: {experiment}")
        raw_pipeline_res: object = cast(
            object, active_hooks.pipeline_factory(prepare_cfg, metadata)
        )
        # Type narrow without using Any to satisfy reportAny
        run_fn_raw: object = getattr(raw_pipeline_res, "run", None)
        if callable(run_fn_raw):
            cast(Callable[[], object], run_fn_raw)()
        else:
            raise RuntimeError(
                f"Pipeline factory produced an object without a run() method: {type(raw_pipeline_res)}"
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
    except (DataError, ValueError, FileNotFoundError, RuntimeError, OSError) as e:
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
    metadata: object,
    learning_mode_engine: LearningModeEngine | None = None,
    *,
    hooks: RuntimeRunHooks | None = None,
) -> ToolResult:
    """Run the full training flow for an experiment."""
    active_hooks = hooks or _DEFAULT_RUNTIME_RUN_HOOKS
    try:
        runtime = train_cfg.runtime
        if not runtime:
            train_cfg.logger.error(_MISSING_TRAIN_RUNTIME_MSG)
            return ToolResult.create(
                success=False,
                exit_code=1,
                namespace="ml",
                category="train",
                command=experiment,
                stderr=_MISSING_TRAIN_RUNTIME_MSG,
            )

        device_seed = int(cast(int, runtime.seed))
        seed_resolver = getattr(active_hooks, "resolve_seed", None)
        if callable(seed_resolver):
            resolved = seed_resolver("train", metadata, int(cast(int, runtime.seed)))
            if isinstance(resolved, int):
                device_seed = resolved
        active_hooks.device_setup(
            str(cast(object, runtime.device)),
            str(cast(object, runtime.dtype)),
            device_seed,
        )

        train_cfg.logger.info(f"Running trainer for experiment: {experiment}")
        train_out_dir = cast(Optional[Path], getattr(metadata, "train_out_dir", None))
        active_hooks.log_status("pre-train", metadata, train_out_dir, train_cfg.logger)

        trainer_obj: object = cast(
            object, active_hooks.trainer_factory(train_cfg, metadata)
        )
        # Type narrow without using Any to satisfy reportAny
        run_fn_raw: object = getattr(trainer_obj, "run", None)
        if callable(run_fn_raw):
            cast(Callable[[], object], run_fn_raw)()
        else:
            raise RuntimeError(
                f"Trainer factory produced an object without a run() method: {type(trainer_obj)}"
            )

        train_cfg.logger.info(f"Trainer for {experiment} finished.")
        active_hooks.log_status("post-train", metadata, train_out_dir, train_cfg.logger)

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
    except (
        RuntimeError,
        ValueError,
        CheckpointError,
        OSError,
        AttributeError,
        TypeError,
    ) as e:
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
    metadata: object,
    learning_mode_engine: LearningModeEngine | None = None,
    *,
    hooks: RuntimeRunHooks | None = None,
) -> ToolResult:
    """Run the full sampling flow for an experiment."""
    active_hooks = hooks or _DEFAULT_RUNTIME_RUN_HOOKS
    try:
        runtime = sample_cfg.runtime
        if not runtime:
            sample_cfg.logger.error(_MISSING_SAMPLE_RUNTIME_MSG)
            return ToolResult.create(
                success=False,
                exit_code=1,
                namespace="ml",
                category="sample",
                command=experiment,
                stderr=_MISSING_SAMPLE_RUNTIME_MSG,
            )

        device_seed = int(cast(int, runtime.seed))
        seed_resolver = getattr(active_hooks, "resolve_seed", None)
        if callable(seed_resolver):
            resolved = seed_resolver("sample", metadata, int(cast(int, runtime.seed)))
            if isinstance(resolved, int):
                device_seed = resolved
        active_hooks.device_setup(
            str(cast(object, runtime.device)),
            str(cast(object, runtime.dtype)),
            device_seed,
        )

        sample_cfg.logger.info(f"Running sampler for experiment: {experiment}")
        sample_out_dir = cast(Optional[Path], getattr(metadata, "sample_out_dir", None))
        active_hooks.log_status(
            "pre-sample", metadata, sample_out_dir, sample_cfg.logger
        )

        raw_sampler_res: object = cast(
            object, active_hooks.sampler_factory(sample_cfg, metadata)
        )
        # Type narrow without using Any to satisfy reportAny
        run_fn_raw: object = getattr(raw_sampler_res, "run", None)
        if callable(run_fn_raw):
            cast(Callable[[], object], run_fn_raw)()
        else:
            raise RuntimeError(
                f"Sampler factory produced an object without a run() method: {type(raw_sampler_res)}"
            )
        sample_cfg.logger.info(f"Sampler for {experiment} finished.")
        active_hooks.log_status(
            "post-sample", metadata, sample_out_dir, sample_cfg.logger
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
    except (
        DataError,
        ValueError,
        FileOperationError,
        RuntimeError,
        AttributeError,
        TypeError,
        OSError,
    ) as e:
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
    metadata: object | None = None,
    exp_config_path: Path | None = None,
    analyze_runner: Callable[[str | None, int, bool, Any], None] | None = None,
) -> ToolResult:
    """Run analysis for an experiment.

    Prefers TensorBoard event-data visualization (`out/logs/tb`) when available.
    Falls back to the LIT demo server for `bundestag_char` if no event files exist.
    """
    from logging import getLogger

    try:
        logger_name = "ml_playground.runtime_cli"
        logger_getter = logger_factory or getLogger
        logger = logger_getter(logger_name)
        if analyze_runner is not None:
            analyze_runner(host, port, open_browser, logger)
        else:
            tb_logdir = _resolve_tensorboard_logdir(
                experiment, metadata=metadata, exp_config_path=exp_config_path
            )
            has_event_files = tb_logdir.exists() and any(
                tb_logdir.rglob("events.out.tfevents.*")
            )
            if has_event_files:
                logger.info(
                    "Launching TensorBoard for '%s' from %s on %s:%s (open_browser=%s)",
                    experiment,
                    tb_logdir,
                    host,
                    port,
                    open_browser,
                )
                _run_tensorboard_server(tb_logdir, host, port, open_browser, logger)
            elif experiment == "bundestag_char":
                logger.info(
                    "No TensorBoard event files found at %s. Falling back to LIT for '%s' on %s:%s (open_browser=%s)",
                    tb_logdir,
                    experiment,
                    host,
                    port,
                    open_browser,
                )
                run_server_bundestag_char(host, port, open_browser, logger)
            else:
                raise RuntimeError(
                    f"No TensorBoard event files found for '{experiment}' at {tb_logdir}. "
                    "Run training first or provide event files under out/logs/tb."
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
            stdout=f"Analysis completed for {experiment}",
            learning_info=learning_info,
        )
    except (ValueError, RuntimeError, AttributeError, TypeError, OSError) as e:
        return ToolResult.create(
            success=False,
            exit_code=1,
            namespace="ml",
            category="analyze",
            command=experiment,
            stderr=f"Analysis failed: {e}",
        )


def _resolve_tensorboard_logdir(
    experiment: str,
    *,
    metadata: object | None,
    exp_config_path: Path | None,
) -> Path:
    train_out_dir = getattr(metadata, "train_out_dir", None)
    if isinstance(train_out_dir, Path):
        return train_out_dir / "logs" / "tb"
    if exp_config_path is not None:
        return exp_config_path.parent / "out" / "logs" / "tb"
    exp_base = Path(__file__).resolve().parents[2] / "experiments" / experiment
    return exp_base / "out" / "logs" / "tb"


def _run_tensorboard_server(
    logdir: Path, host: str, port: int, open_browser: bool, logger: LoggerLike
) -> None:
    from tensorboard import program as tb_program

    tb = tb_program.TensorBoard()
    tb.configure(
        argv=[
            None,
            "--logdir",
            str(logdir),
            "--host",
            host,
            "--port",
            str(port),
        ]
    )
    url = tb.launch()
    logger.info("TensorBoard running at %s", url)
    if open_browser:
        webbrowser.open(url)
    while True:
        time.sleep(1)
