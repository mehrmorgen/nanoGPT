from __future__ import annotations

from pathlib import Path
import importlib
import sys

from ml_playground.configuration import cli as config_cli
from ml_playground.configuration.models import (
    PreparerConfig,
    SamplerConfig,
    SharedConfig,
    TrainerConfig,
)
from ml_playground.runtime.core import bootstrap as runtime_bootstrap
from ml_playground.runtime.core.results import LearningModeEngine, ToolResult

from .commands import run_prepare_impl, run_sample_impl, run_train_impl

CLIDependencies = runtime_bootstrap.CLIDependencies

__all__ = [
    "CLIDependencies",
    "default_cli_dependencies",
    "configure_cli_dependencies",
    "reset_cli_dependencies",
    "get_cli_dependencies",
    "override_cli_dependencies",
    "run_prepare",
    "run_prepare_cmd",
    "run_train",
    "run_sample",
    "run_train_cmd",
    "run_sample_cmd",
]


def default_cli_dependencies() -> CLIDependencies:
    return CLIDependencies(
        load_experiment=config_cli.load_experiment,
        ensure_train_prerequisites=config_cli.ensure_train_prerequisites,
        ensure_sample_prerequisites=config_cli.ensure_sample_prerequisites,
        run_prepare=run_prepare_impl,
        run_train=run_train_impl,
        run_sample=run_sample_impl,
    )


runtime_bootstrap.configure_runtime_cli_dependencies(default_cli_dependencies)


configure_cli_dependencies = runtime_bootstrap.configure_runtime_cli_dependencies
reset_cli_dependencies = runtime_bootstrap.reset_runtime_cli_dependencies
get_cli_dependencies = runtime_bootstrap.get_runtime_cli_dependencies
override_cli_dependencies = runtime_bootstrap.override_runtime_cli_dependencies


def run_prepare(
    experiment: str,
    prepare_cfg: PreparerConfig,
    config_path: Path,
    shared: SharedConfig,
    learning_mode_engine: LearningModeEngine | None = None,
    learning_mode: bool = False,
) -> ToolResult:
    cli_pkg = _cli_module()
    result = cli_pkg.run_prepare_impl(
        experiment,
        prepare_cfg,
        config_path,
        shared,
        learning_mode_engine,
    )
    cli_pkg.handle_tool_result(result, learning_mode)
    return result


def run_train(
    experiment: str,
    train_cfg: TrainerConfig,
    config_path: Path,
    shared: SharedConfig,
    learning_mode_engine: LearningModeEngine | None = None,
    learning_mode: bool = False,
) -> ToolResult:
    cli_pkg = _cli_module()
    result = cli_pkg.run_train_impl(
        experiment,
        train_cfg,
        config_path,
        shared,
        learning_mode_engine,
    )
    cli_pkg.handle_tool_result(result, learning_mode)
    return result


def run_sample(
    experiment: str,
    sample_cfg: SamplerConfig,
    config_path: Path,
    shared: SharedConfig,
    learning_mode_engine: LearningModeEngine | None = None,
    learning_mode: bool = False,
) -> ToolResult:
    cli_pkg = _cli_module()
    result = cli_pkg.run_sample_impl(
        experiment,
        sample_cfg,
        config_path,
        shared,
        learning_mode_engine,
    )
    cli_pkg.handle_tool_result(result, learning_mode)
    return result


def run_train_cmd(
    experiment: str,
    exp_config_path: Path | None,
    deps: CLIDependencies | None = None,
    learning_engine: LearningModeEngine | None = None,
    learning_mode: bool = False,
) -> None:
    cli_pkg = _cli_module()
    deps = deps or get_cli_dependencies()
    exp = deps.load_experiment(experiment, exp_config_path)
    deps.ensure_train_prerequisites(exp)
    result = deps.run_train(
        experiment, exp.train, exp.shared.config_path, exp.shared, learning_engine
    )
    cli_pkg.handle_tool_result(result, learning_mode)


def run_prepare_cmd(
    experiment: str,
    exp_config_path: Path | None,
    deps: CLIDependencies | None = None,
    learning_engine: LearningModeEngine | None = None,
    learning_mode: bool = False,
) -> None:
    cli_pkg = _cli_module()
    deps = deps or get_cli_dependencies()
    exp = deps.load_experiment(experiment, exp_config_path)
    result = deps.run_prepare(
        experiment, exp.prepare, exp.shared.config_path, exp.shared, learning_engine
    )
    cli_pkg.handle_tool_result(result, learning_mode)


def run_sample_cmd(
    experiment: str,
    exp_config_path: Path | None,
    deps: CLIDependencies | None = None,
    learning_engine: LearningModeEngine | None = None,
    learning_mode: bool = False,
) -> None:
    cli_pkg = _cli_module()
    deps = deps or get_cli_dependencies()
    exp = deps.load_experiment(experiment, exp_config_path)
    deps.ensure_sample_prerequisites(exp)
    result = deps.run_sample(
        experiment, exp.sample, exp.shared.config_path, exp.shared, learning_engine
    )
    cli_pkg.handle_tool_result(result, learning_mode)


def _cli_module():
    mod = sys.modules.get("ml_playground.runtime.cli")
    if mod is None:
        mod = importlib.import_module("ml_playground.runtime.cli")
    return mod
