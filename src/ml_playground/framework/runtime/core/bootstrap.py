from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable


def _default_load_experiment(_name: str, _path: Path | None) -> Any:
    return None


def _default_noop(*_args: Any, **_kwargs: Any) -> Any:
    return None


@dataclass(frozen=True)
class CLIDependencies:
    """Container holding injectable runtime CLI dependencies."""

    load_experiment: Callable[[str, Path | None], Any] = _default_load_experiment
    ensure_train_prerequisites: Callable[[Any], Any] = _default_noop
    ensure_sample_prerequisites: Callable[[Any], Any] = _default_noop
    run_prepare: Callable[[str, Any, Path, Any, "CLIDependencies", Any | None], Any] = (
        _default_noop
    )
    run_train: Callable[[str, Any, Path, Any, "CLIDependencies", Any | None], Any] = (
        _default_noop
    )
    run_sample: Callable[[str, Any, Path, Any, "CLIDependencies", Any | None], Any] = (
        _default_noop
    )
    run_analyze: Callable[..., Any] = _default_noop
    global_device_setup: Callable[..., None] = _default_noop
    log_command_status: Callable[[str, Any, Path | None, Any], None] = (
        lambda _s, _d, _p, _a: None  # type: ignore[reportAny]
    )
    handle_tool_result: Callable[[Any, bool], None] = (
        lambda _r, _l: None  # type: ignore[reportAny]
    )
    create_pipeline: Callable[[Any, Any], Any] = (
        lambda _c, _m: None  # type: ignore[reportAny]
    )
    trainer_factory: Callable[..., Any] = (
        lambda *args, **kwargs: None  # type: ignore[reportAny]
    )
    sampler_factory: Callable[..., Any] = (
        lambda *args, **kwargs: None  # type: ignore[reportAny]
    )
    confirm_fn: Callable[[str], bool] | None = None
    app: Any = None
    echo: Callable[..., None] | None = None


Factory = Callable[[], CLIDependencies]
_default_factory: Factory | None = None
_current: CLIDependencies | None = None


def create_default_cli_dependencies() -> CLIDependencies:
    """Create a new instance of default CLI dependencies."""

    from ml_playground.framework.configuration import cli as config_cli
    from ml_playground.framework.data_pipeline.preparer import create_pipeline
    from ml_playground.framework.training.loop.runner import Trainer
    from ml_playground.framework.sampling.runner import Sampler
    from ml_playground.runtime_cli import commands as cli_commands
    from ml_playground.runtime_cli import device as cli_device
    import typer

    def _create_trainer(cfg: Any, metadata: Any, deps: Any | None = None) -> Any:  # type: ignore[reportAny]
        return Trainer(cfg, metadata, deps)

    def _create_sampler(cfg: Any, metadata: Any, deps: Any | None = None) -> Any:  # type: ignore[reportAny]
        return Sampler(cfg, metadata, deps=deps)

    return CLIDependencies(
        load_experiment=config_cli.load_experiment,
        ensure_train_prerequisites=config_cli.ensure_train_prerequisites,
        ensure_sample_prerequisites=config_cli.ensure_sample_prerequisites,
        run_prepare=cli_commands.run_prepare_impl,
        run_train=cli_commands.run_train_impl,
        run_sample=cli_commands.run_sample_impl,
        run_analyze=cli_commands.run_analyze,
        global_device_setup=cli_device.global_device_setup,
        log_command_status=cli_commands.log_command_status,
        handle_tool_result=cli_commands.handle_tool_result,
        create_pipeline=create_pipeline,
        trainer_factory=_create_trainer,
        sampler_factory=_create_sampler,
        confirm_fn=typer.confirm,
    )


default_cli_dependencies: Factory = create_default_cli_dependencies


def configure_cli_dependencies(factory: Factory) -> None:
    """Set the factory used to create CLI dependencies."""

    global _default_factory, _current
    _default_factory = factory
    _current = None


def reset_cli_dependencies() -> None:
    """Reset to the default dependency factory and clear current instance."""

    global _current
    _current = None


def clear_config_for_tests() -> None:
    """Clear the default factory (unconfigure) for testing."""

    global _default_factory, _current
    _default_factory = None
    _current = None


def get_cli_dependencies() -> CLIDependencies:
    """Return the active CLI dependencies, creating them if needed."""

    global _current
    if _current is None:
        if _default_factory is None:
            raise RuntimeError("Runtime CLI dependencies have not been configured")
        _current = _default_factory()
    return _current


def override_cli_dependencies(deps: CLIDependencies):
    """Temporarily override CLI dependencies within a context manager."""

    from contextlib import contextmanager

    @contextmanager
    def _manager():
        global _current, _default_factory
        previous_current = _current
        previous_factory = _default_factory
        _current = deps
        try:
            yield
        finally:
            _current = previous_current
            _default_factory = previous_factory

    return _manager()


__all__ = [
    "CLIDependencies",
    "Factory",
    "create_default_cli_dependencies",
    "configure_cli_dependencies",
    "reset_cli_dependencies",
    "clear_config_for_tests",
    "get_cli_dependencies",
    "override_cli_dependencies",
    "default_cli_dependencies",
]
