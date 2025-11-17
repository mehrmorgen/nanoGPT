from __future__ import annotations

from typing import Callable

from ml_playground.configuration import cli as config_cli
from ml_playground.runtime import runners as runtime_runners
from ml_playground.runtime.core import bootstrap as runtime_bootstrap

CLIDependencies = runtime_bootstrap.CLIDependencies


def default_cli_dependencies() -> CLIDependencies:
    return CLIDependencies(
        load_experiment=config_cli.load_experiment,
        ensure_train_prerequisites=config_cli.ensure_train_prerequisites,
        ensure_sample_prerequisites=config_cli.ensure_sample_prerequisites,
        run_prepare=runtime_runners.run_prepare_impl,
        run_train=runtime_runners.run_train_impl,
        run_sample=runtime_runners.run_sample_impl,
    )


runtime_bootstrap.configure_runtime_cli_dependencies(default_cli_dependencies)


def configure_cli_dependencies(factory: Callable[[], CLIDependencies]) -> None:
    runtime_bootstrap.configure_runtime_cli_dependencies(factory)


def reset_cli_dependencies() -> None:
    runtime_bootstrap.reset_runtime_cli_dependencies()


def get_cli_dependencies() -> CLIDependencies:
    return runtime_bootstrap.get_runtime_cli_dependencies()


def override_cli_dependencies(deps: CLIDependencies):
    return runtime_bootstrap.override_runtime_cli_dependencies(deps)
