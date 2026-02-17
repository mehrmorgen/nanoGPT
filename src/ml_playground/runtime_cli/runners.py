from __future__ import annotations

from pathlib import Path
from typing import Any, cast
import sys

from ml_playground.framework.configuration.models import (
    MetadataConfig,
    PreparerConfig,
    SamplerConfig,
    TrainerConfig,
)
from ml_playground.framework.core.checkpoint_lock import (
    checkpoint_lock,
    checkpoint_lock_path,
)
from ml_playground.framework.runtime.core import bootstrap as runtime_bootstrap
from ml_playground.framework.runtime.core.results import LearningModeEngine, ToolResult
from ml_playground.framework.runtime.helpers import (
    extract_exp_config,
    handle_tool_result,
    run_or_exit,
)
from ml_playground.framework.runtime.protocols import LoadedExperiment

from .commands import (
    log_directory,
    run_analyze,
)

CLIDependencies = runtime_bootstrap.CLIDependencies
create_default_cli_dependencies = runtime_bootstrap.create_default_cli_dependencies
configure_cli_dependencies = runtime_bootstrap.configure_cli_dependencies
reset_cli_dependencies = runtime_bootstrap.reset_cli_dependencies
override_cli_dependencies = runtime_bootstrap.override_cli_dependencies


def get_cli_dependencies() -> CLIDependencies:
    """Get the current CLI dependencies (managed by bootstrap)."""
    try:
        return runtime_bootstrap.get_cli_dependencies()
    except RuntimeError:
        runtime_bootstrap.configure_cli_dependencies(
            runtime_bootstrap.create_default_cli_dependencies
        )
        return runtime_bootstrap.get_cli_dependencies()


__all__ = [
    "CLIDependencies",
    "create_default_cli_dependencies",
    "configure_cli_dependencies",
    "reset_cli_dependencies",
    "override_cli_dependencies",
    "get_cli_dependencies",
    "handle_tool_result",
    "log_directory",
    "run_analyze",
    "run_prepare",
    "run_prepare_cmd",
    "run_train",
    "run_sample",
    "run_train_cmd",
    "run_sample_cmd",
    "run_analyze_cmd",
    "run_or_exit",
    "extract_exp_config",
]


def run_prepare(
    experiment: str,
    prepare_cfg: PreparerConfig,
    config_path: Path,
    metadata: MetadataConfig,
    deps: CLIDependencies,
    learning_mode_engine: LearningModeEngine | None = None,
    learning_mode: bool = False,
) -> ToolResult:
    """Run prepare command with explicit dependency injection."""
    result = cast(
        ToolResult,
        deps.run_prepare(
            experiment,
            prepare_cfg,
            config_path,
            metadata,
            deps,
            learning_mode_engine,
        ),
    )
    deps.handle_tool_result(result, learning_mode)
    return result


def run_train(
    experiment: str,
    train_cfg: TrainerConfig,
    config_path: Path,
    metadata: MetadataConfig,
    deps: CLIDependencies,
    learning_mode_engine: LearningModeEngine | None = None,
    learning_mode: bool = False,
) -> ToolResult:
    """Run train command with explicit dependency injection."""
    result = cast(
        ToolResult,
        deps.run_train(
            experiment,
            train_cfg,
            config_path,
            metadata,
            deps,
            learning_mode_engine,
        ),
    )
    deps.handle_tool_result(result, learning_mode)
    return result


def run_sample(
    experiment: str,
    sample_cfg: SamplerConfig,
    config_path: Path,
    metadata: MetadataConfig,
    deps: CLIDependencies,
    learning_mode_engine: LearningModeEngine | None = None,
    learning_mode: bool = False,
) -> ToolResult:
    """Run sample command with explicit dependency injection."""
    result = cast(
        ToolResult,
        deps.run_sample(
            experiment,
            sample_cfg,
            config_path,
            metadata,
            deps,
            learning_mode_engine,
        ),
    )
    deps.handle_tool_result(result, learning_mode)
    return result


def run_train_cmd(
    experiment: str,
    exp_config_path: Path | None,
    deps: CLIDependencies,
    learning_engine: LearningModeEngine | None = None,
    learning_mode: bool = False,
) -> None:
    """Run train command with explicit dependency injection."""
    exp = cast(LoadedExperiment, deps.load_experiment(experiment, exp_config_path))
    train_cfg = exp.training
    metadata = exp.metadata
    if train_cfg is None:
        raise RuntimeError("training config is required for training")
    try:
        config_path = metadata.config_path
    except AttributeError as exc:
        raise RuntimeError("metadata.config_path is required for training") from exc
    metadata_path = _normalize_cli_path(config_path)
    if metadata_path is None:
        raise RuntimeError("metadata.config_path is required for training")
    train_out_dir = getattr(metadata, "train_out_dir", None)

    # Ensure prerequisites are checked safely
    ensure_fn = getattr(deps, "ensure_train_prerequisites", None)
    if callable(ensure_fn):
        ensure_fn(exp)

    ckpt_last = cast(
        str,
        getattr(
            getattr(train_cfg, "runtime", None), "ckpt_last_filename", "ckpt_last.pt"
        ),
    )
    if train_out_dir is None:
        raise RuntimeError("metadata.train_out_dir is required for checkpoint locking")
    lock_path = checkpoint_lock_path(train_out_dir, ckpt_last)
    owner = f"train:{experiment}"
    with checkpoint_lock(lock_path, owner=owner):
        result_raw: object = cast(
            object,
            deps.run_train(
                experiment,
                train_cfg,
                metadata_path,
                metadata,
                deps,
                learning_engine,
            ),
        )
    result = cast(ToolResult, result_raw)

    handler = deps.handle_tool_result
    if handler is runtime_bootstrap.CLIDependencies.handle_tool_result:
        handler = handle_tool_result
    handler(result, learning_mode)


def run_prepare_cmd(
    experiment: str,
    exp_config_path: Path | None,
    deps: CLIDependencies,
    learning_engine: LearningModeEngine | None = None,
    learning_mode: bool = False,
) -> None:
    """Run prepare command with explicit dependency injection."""
    exp = cast(LoadedExperiment, deps.load_experiment(experiment, exp_config_path))
    prepare_cfg = exp.prepare
    metadata = exp.metadata
    if prepare_cfg is None:
        raise RuntimeError("prepare config is required for preparation")
    try:
        config_path = metadata.config_path
    except AttributeError as exc:
        raise RuntimeError("metadata.config_path is required for preparation") from exc
    metadata_path = _normalize_cli_path(config_path)
    if metadata_path is None:
        raise RuntimeError("metadata.config_path is required for preparation")

    result_raw: object = cast(
        object,
        deps.run_prepare(
            experiment,
            prepare_cfg,
            metadata_path,
            metadata,
            deps,
            learning_engine,
        ),
    )
    result = cast(ToolResult, result_raw)

    handler = deps.handle_tool_result
    if handler is runtime_bootstrap.CLIDependencies.handle_tool_result:
        handler = handle_tool_result
    handler(result, learning_mode)


def run_sample_cmd(
    experiment: str,
    exp_config_path: Path | None,
    deps: CLIDependencies,
    learning_engine: LearningModeEngine | None = None,
    learning_mode: bool = False,
) -> None:
    """Run sample command with explicit dependency injection."""
    exp = cast(LoadedExperiment, deps.load_experiment(experiment, exp_config_path))
    sample_cfg = exp.sampling
    metadata = exp.metadata
    if sample_cfg is None:
        raise RuntimeError("sampling config is required for sampling")
    try:
        config_path = metadata.config_path
    except AttributeError as exc:
        raise RuntimeError("metadata.config_path is required for sampling") from exc
    metadata_path = _normalize_cli_path(config_path)
    if metadata_path is None:
        raise RuntimeError("metadata.config_path is required for sampling")
    train_out_dir = getattr(metadata, "train_out_dir", None)

    # Ensure prerequisites are checked safely
    ensure_fn = getattr(deps, "ensure_sample_prerequisites", None)
    if callable(ensure_fn):
        ensure_fn(exp)

    ckpt_last = cast(
        str,
        getattr(
            getattr(sample_cfg, "runtime", None), "ckpt_last_filename", "ckpt_last.pt"
        ),
    )
    if train_out_dir is None:
        raise RuntimeError("metadata.train_out_dir is required for checkpoint locking")
    lock_path = checkpoint_lock_path(train_out_dir, ckpt_last)
    owner = f"sample:{experiment}"
    with checkpoint_lock(lock_path, owner=owner):
        result_raw: object = cast(
            object,
            deps.run_sample(
                experiment,
                sample_cfg,
                metadata_path,
                metadata,
                deps,
                learning_engine,
            ),
        )
    result = cast(ToolResult, result_raw)

    handler = deps.handle_tool_result
    if handler is runtime_bootstrap.CLIDependencies.handle_tool_result:
        handler = handle_tool_result
    handler(result, learning_mode)


def run_analyze_cmd(
    experiment: str,
    exp_config_path: Path | None,
    deps: CLIDependencies,
    host: str,
    port: int,
    open_browser: bool,
    learning_engine: LearningModeEngine | None = None,
    learning_mode: bool = False,
) -> None:
    """Run analyze command with explicit dependency injection."""
    exp_obj = deps.load_experiment(experiment, exp_config_path)
    metadata = getattr(exp_obj, "metadata", None)
    analyze_fn = deps.run_analyze
    result_raw: object = cast(
        object,
        cast(Any, analyze_fn)(
            experiment,
            host,
            port,
            open_browser,
            learning_engine,
            metadata=metadata,
            exp_config_path=exp_config_path,
        ),
    )
    result = cast(ToolResult, result_raw)
    handler = deps.handle_tool_result
    if handler is runtime_bootstrap.CLIDependencies.handle_tool_result:
        handler = handle_tool_result
    handler(result, learning_mode)


def _normalize_cli_path(path: Path | None) -> Path | None:
    """Normalize paths to avoid macOS /private prefix discrepancies."""
    if path is None:
        return None
    if sys.platform == "darwin" and path.is_absolute():
        try:
            return Path("/") / path.relative_to("/private")
        except ValueError:
            return path
    return path
