from __future__ import annotations

import logging
from pathlib import Path
from typing import Annotated, Any, Callable, Optional, cast

import torch
import typer
from typer.main import get_command
import click

from ml_playground.configuration.models import (
    PreparerConfig,
    SamplerConfig,
    SharedConfig,
    TrainerConfig,
)
from ml_playground.configuration import loading as config_loading
from ml_playground.configuration import cli as config_cli
from ml_playground.data_pipeline.preparer import create_pipeline
from ml_playground.sampling.runner import Sampler
from ml_playground.training.loop.runner import Trainer as CoreTrainer
from ml_playground.core.logging_protocol import LoggerLike
from ml_playground.experiments import registry
from ml_playground.runtime.core import bootstrap as runtime_bootstrap
from ml_playground.runtime.core.results import (
    LearningModeEngine,
    ToolResult,
    VerbosityLevel,
)

# Constants for error messages
_MISSING_TRAIN_RUNTIME_MSG = "Runtime configuration is missing for training."
_MISSING_SAMPLE_RUNTIME_MSG = "Runtime configuration is missing for sampling."


def handle_tool_result(result: ToolResult, learning_mode: bool = False) -> None:
    """Handle ToolResult output and exit appropriately."""
    if result.stdout:
        typer.echo(result.stdout)
    if result.stderr:
        typer.echo(result.stderr, err=True)

    # Display learning mode information if enabled
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


# (Removed unused type aliases)


__all__ = [
    "main",
    "CLIDependencies",
    "default_cli_dependencies",
    "configure_cli_dependencies",
    "reset_cli_dependencies",
    "get_cli_dependencies",
    "override_cli_dependencies",
    "run_prepare_impl",
    "run_train_impl",
    "run_sample_impl",
    "run_prepare",
    "run_train_cmd",
    "run_sample_cmd",
    "run_analyze",
    "handle_tool_result",
    "log_command_status",
    "log_directory",
    "global_device_setup",
    "extract_exp_config",
]


CLIDependencies = runtime_bootstrap.CLIDependencies


def run_prepare_impl(
    experiment: str,
    prepare_cfg: PreparerConfig,
    config_path: Path,
    shared: Any,
    learning_mode_engine: LearningModeEngine | None = None,
) -> ToolResult:
    """Run the full prepare flow for an experiment."""
    try:
        prepare_cfg.logger.info(f"Running pipeline for experiment: {experiment}")
        pipeline = create_pipeline(prepare_cfg, shared)
        pipeline.run()
        prepare_cfg.logger.info(f"Pipeline for {experiment} finished.")

        # Generate learning info if learning mode is enabled
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

        # Generate learning info even for failures if learning mode is enabled
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
    train_cfg: TrainerConfig,
    config_path: Path,
    shared: Any,
    learning_mode_engine: LearningModeEngine | None = None,
) -> ToolResult:
    """Run the full training flow for an experiment."""

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

        global_device_setup(
            train_cfg.runtime.device,
            train_cfg.runtime.dtype,
            train_cfg.runtime.seed,
        )

        train_cfg.logger.info(f"Running trainer for experiment: {experiment}")
        log_command_status("pre-train", shared, shared.train_out_dir, train_cfg.logger)

        trainer = CoreTrainer(train_cfg, shared)
        trainer.run()

        train_cfg.logger.info(f"Trainer for {experiment} finished.")
        log_command_status("post-train", shared, shared.train_out_dir, train_cfg.logger)

        # Generate learning info if learning mode is enabled
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

        # Generate learning info even for failures if learning mode is enabled
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

        global_device_setup(
            sample_cfg.runtime.device,
            sample_cfg.runtime.dtype,
            sample_cfg.runtime.seed,
        )

        sample_cfg.logger.info(f"Running sampler for experiment: {experiment}")
        log_command_status(
            "pre-sample", shared, shared.sample_out_dir, sample_cfg.logger
        )
        sampler = Sampler(sample_cfg, shared)
        sampler.run()
        sample_cfg.logger.info(f"Sampler for {experiment} finished.")
        log_command_status(
            "post-sample", shared, shared.sample_out_dir, sample_cfg.logger
        )

        # Generate learning info if learning mode is enabled
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

        # Generate learning info even for failures if learning mode is enabled
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


def run_prepare(
    experiment: str,
    prepare_cfg: PreparerConfig,
    config_path: Path,
    shared: Any,
    learning_mode_engine: LearningModeEngine | None = None,
    learning_mode: bool = False,
) -> ToolResult:
    """Backward-compatible wrapper around `_run_prepare_impl`."""

    result = run_prepare_impl(
        experiment,
        prepare_cfg,
        config_path,
        shared,
        learning_mode_engine,
    )
    handle_tool_result(result, learning_mode)
    return result


def run_train(
    experiment: str,
    train_cfg: TrainerConfig,
    config_path: Path,
    shared: Any,
    learning_mode_engine: LearningModeEngine | None = None,
    learning_mode: bool = False,
) -> ToolResult:
    """Backward-compatible wrapper around `_run_train_impl`."""

    result = run_train_impl(
        experiment,
        train_cfg,
        config_path,
        shared,
        learning_mode_engine,
    )
    handle_tool_result(result, learning_mode)
    return result


def run_sample(
    experiment: str,
    sample_cfg: SamplerConfig,
    config_path: Path,
    shared: Any,
    learning_mode_engine: LearningModeEngine | None = None,
    learning_mode: bool = False,
) -> ToolResult:
    """Backward-compatible wrapper around `_run_sample_impl`."""

    result = run_sample_impl(
        experiment,
        sample_cfg,
        config_path,
        shared,
        learning_mode_engine,
    )
    handle_tool_result(result, learning_mode)
    return result


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


def configure_cli_dependencies(factory: Callable[[], CLIDependencies]) -> None:
    runtime_bootstrap.configure_runtime_cli_dependencies(factory)


def reset_cli_dependencies() -> None:
    runtime_bootstrap.reset_runtime_cli_dependencies()


def get_cli_dependencies() -> CLIDependencies:
    return runtime_bootstrap.get_runtime_cli_dependencies()


def override_cli_dependencies(deps: CLIDependencies):
    return runtime_bootstrap.override_runtime_cli_dependencies(deps)


# --- Global device setup ---------------------------------------------------
def global_device_setup(
    device: str,
    dtype: str,
    seed: int,
    *,
    cuda_is_available: Optional[Callable[[], bool]] = None,
    torch_module: Any = None,
) -> None:
    """Set global seeds and enable TF32 as needed.

    Centralizes side-effectful setup so other modules don't repeat it.
    """
    torch_mod = cast(Any, torch_module if torch_module is not None else torch)
    try:
        manual_seed = cast(Callable[[int], object], torch_mod.manual_seed)
        manual_seed(seed)
        _cuda_available = (
            cuda_is_available()
            if cuda_is_available is not None
            else torch_mod.cuda.is_available()
        )
        if _cuda_available:
            cuda_manual_seed = cast(Callable[[int], None], torch_mod.cuda.manual_seed)
            cuda_manual_seed(seed)
            try:
                torch_mod.backends.cuda.matmul.fp32_precision = "tf32"
            except AttributeError:
                pass
            try:
                torch_mod.backends.cudnn.fp32_precision = "tf32"
            except AttributeError:
                pass
    except (RuntimeError, AssertionError, AttributeError):
        # Never fail CLI due to environment-specific torch issues
        pass


# --- Typer helpers ---------------------------------------------------------
# (Typer helper implementations omitted for brevity in this excerpt.)
def _complete_experiments(ctx: typer.Context, incomplete: str) -> list[str]:
    """Auto-complete experiment names based on directories with a config.toml."""
    # Delegate to loader to keep FS access centralized for configuration
    return config_loading.list_experiments_with_config(incomplete)


# --- CLI plumbing ----------------------------------------------------------


def extract_exp_config(ctx: typer.Context) -> Path | None:
    """Extract the --exp-config path from the Typer context."""
    obj = getattr(ctx, "obj", None)
    if not isinstance(obj, dict):
        logging.getLogger(__name__).debug(
            "Context object missing or not a dict; no exp_config."
        )
        return None
    mapping = cast(dict[str, object], obj)
    exp_config_obj = mapping.get("exp_config")
    logger = logging.getLogger(__name__)
    logger.debug("Context exp_config resolved to %s", exp_config_obj)
    if exp_config_obj is None:
        return None
    if isinstance(exp_config_obj, Path):
        return exp_config_obj
    logger.debug("Unexpected exp_config value type %s; ignoring.", type(exp_config_obj))
    return None


def run_or_exit(
    func: Callable[[], None],
    *,
    keyboard_interrupt_msg: str | None = None,
    exception_exit_code: int = 1,
) -> None:
    """Run a function and exit gracefully on exceptions.

    - KeyboardInterrupt: print optional message and return (no exit), per tests.
    - Other exceptions: echo message and exit with provided code.
    """
    try:
        func()
    except FileNotFoundError as e:
        logger = logging.getLogger(__name__)
        logger.error(f"{e}")
        raise typer.Exit(exception_exit_code)
    except (ValueError, TypeError) as e:
        logger = logging.getLogger(__name__)
        logger.error(f"{e}")
        raise typer.Exit(exception_exit_code)
    except KeyboardInterrupt:
        if keyboard_interrupt_msg:
            logger = logging.getLogger(__name__)
            logger.info(keyboard_interrupt_msg)
        # Do not exit on KeyboardInterrupt in this helper
        return
    except (RuntimeError, OSError, ImportError) as e:
        # Generic mapping for unexpected exceptions: echo and exit with provided code
        logger = logging.getLogger(__name__)
        logger.error(f"{e}")
        raise typer.Exit(exception_exit_code)


def log_directory(
    tag: str,
    dir_name: str,
    dir_path: Path | None,
    logger: LoggerLike,
) -> None:
    """Log information about a directory path."""
    if dir_path is None:
        logger.info(f"[{tag}] {dir_name}: <not set>")
        return

    # Runtime guard: tests may pass non-Path via Any; avoid attribute errors
    if not isinstance(dir_path, Path):  # pyright: ignore[reportUnnecessaryIsInstance]
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


# --- Command runners -------------------------------------------------------


def log_command_status(
    tag: str,
    shared: "SharedConfig",
    out_dir: Path,
    logger: LoggerLike,
) -> None:
    """Log known file-based artifacts for the given config."""
    try:
        log_directory(tag, "out_dir", out_dir, logger)
        log_directory(tag, "dataset_dir", shared.dataset_dir, logger)
    except (OSError, ValueError, TypeError):
        # Never fail due to logging
        pass


def run_analyze(
    experiment: str,
    host: str,
    port: int,
    open_browser: bool,
    learning_mode_engine: LearningModeEngine | None = None,
) -> ToolResult:
    """Run analysis for an experiment.

    Only 'bundestag_char' is currently supported.
    """
    try:
        # Raise for any experiment other than 'bundestag_char'
        if experiment != "bundestag_char":
            return ToolResult.create(
                success=False,
                exit_code=1,
                namespace="ml",
                category="analyze",
                command=experiment,
                stderr=f"analyze currently supports only 'bundestag_char', got: {experiment}",
            )

        # Placeholder for actual analysis logic for bundestag_char
        logger = logging.getLogger(__name__)
        logger.info(
            f"Analysis for '{experiment}' not implemented. Host={host}, Port={port}, Open={open_browser}"
        )

        # Generate learning info if learning mode is enabled
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


# --- CLI definition --------------------------------------------------------


EXPERIMENT_HELP = "Experiment name (directory in src/ml_playground/experiments)"


ExperimentArg = Annotated[
    str,
    typer.Argument(
        help=EXPERIMENT_HELP,
        autocompletion=_complete_experiments,
    ),
]


# Typer-based CLI
app = typer.Typer(
    no_args_is_help=True,
    help=(
        "ML Playground CLI: prepare data, train models, sample outputs, and export models.\n"
        "This CLI loads and validates TOML configs and injects the resulting configuration\n"
        "objects into experiment code. Experiments must not read TOML directly."
    ),
)


@app.callback()
def global_options(
    ctx: typer.Context,
    exp_config: Annotated[
        Path | None,
        typer.Option(
            "--exp-config",
            help=(
                "Path to an experiment-specific config TOML. When provided, it replaces "
                "the experiment's config.toml. default_config.toml is still loaded first."
            ),
        ),
    ] = None,
    learning_mode: Annotated[
        bool,
        typer.Option(
            "--learning-mode",
            help="Enable educational explanations for ML workflow operations",
        ),
    ] = False,
    verbosity: Annotated[
        int,
        typer.Option(
            "--verbosity",
            "-v",
            help="Learning mode verbosity: 0=minimal, 1=standard, 2=comprehensive",
            min=0,
            max=2,
        ),
    ] = 1,
) -> None:
    """Global options applied to all subcommands."""
    # Validate --exp-config immediately if provided
    if exp_config is not None and not exp_config.exists():
        logger = logging.getLogger(__name__)
        msg = f"Config file not found: {exp_config}"
        logger.error(msg)
        typer.echo(msg, err=True)
        raise typer.Exit(2)

    # Ensure INFO-level logs (including status) are visible by default
    root_logger = logging.getLogger()
    if not root_logger.handlers:
        logging.basicConfig(level=logging.INFO, format="%(message)s")

    try:
        ctx.ensure_object(dict)
    except (AttributeError, TypeError):
        return

    ctx_dict = ctx.obj if isinstance(ctx.obj, dict) else {}
    if ctx_dict is not ctx.obj:
        ctx.obj = ctx_dict

    verbosity_level = VerbosityLevel(verbosity)

    ctx_dict["exp_config"] = exp_config
    if learning_mode:
        ctx_dict["learning_mode"] = True
    if verbosity_level != VerbosityLevel.STANDARD:
        ctx_dict["verbosity"] = verbosity_level

    context = click.get_current_context(silent=True)
    if context is not None and context.invoked_subcommand is None:
        typer.echo("Welcome to ML Playground runtime CLI!", err=True)
        typer.echo(
            "No workflow command was provided. Try `uv run ml-playground prepare <experiment>`.",
            err=True,
        )
        typer.echo("", err=True)
        typer.echo(context.get_help(), err=True)
        raise typer.Exit(0)


@app.command()
def prepare(
    ctx: typer.Context,
    experiment: ExperimentArg,
) -> None:
    """Prepare data for an experiment."""
    exp_config_path = extract_exp_config(ctx)
    deps = get_cli_dependencies()

    # Get learning mode settings from context
    learning_mode = ctx.obj.get("learning_mode", False) if ctx.obj else False
    verbosity = (
        ctx.obj.get("verbosity", VerbosityLevel.STANDARD)
        if ctx.obj
        else VerbosityLevel.STANDARD
    )
    learning_engine = LearningModeEngine(verbosity) if learning_mode else None

    def _do_prepare() -> None:
        exp = deps.load_experiment(experiment, exp_config_path)
        result = deps.run_prepare(
            experiment, exp.prepare, exp.shared.config_path, exp.shared, learning_engine
        )
        handle_tool_result(result, learning_mode)

    run_or_exit(
        _do_prepare,
        keyboard_interrupt_msg="\nData preparation cancelled.",
    )


@app.command()
def train(
    ctx: typer.Context,
    experiment: ExperimentArg,
) -> None:
    """Train a model for an experiment."""
    exp_config_path = extract_exp_config(ctx)
    deps = get_cli_dependencies()

    # Get learning mode settings from context
    learning_mode = ctx.obj.get("learning_mode", False) if ctx.obj else False
    verbosity = (
        ctx.obj.get("verbosity", VerbosityLevel.STANDARD)
        if ctx.obj
        else VerbosityLevel.STANDARD
    )
    learning_engine = LearningModeEngine(verbosity) if learning_mode else None

    run_or_exit(
        lambda: run_train_cmd(
            experiment, exp_config_path, deps, learning_engine, learning_mode
        ),
        keyboard_interrupt_msg="\nTraining cancelled.",
    )


@app.command()
def sample(
    ctx: typer.Context,
    experiment: ExperimentArg,
) -> None:
    """Sample from a trained model."""
    exp_config_path = extract_exp_config(ctx)
    deps = get_cli_dependencies()

    # Get learning mode settings from context
    learning_mode = ctx.obj.get("learning_mode", False) if ctx.obj else False
    verbosity = (
        ctx.obj.get("verbosity", VerbosityLevel.STANDARD)
        if ctx.obj
        else VerbosityLevel.STANDARD
    )
    learning_engine = LearningModeEngine(verbosity) if learning_mode else None

    run_or_exit(
        lambda: run_sample_cmd(
            experiment, exp_config_path, deps, learning_engine, learning_mode
        ),
        keyboard_interrupt_msg="\nSampling cancelled.",
    )


@app.command()
def analyze(
    ctx: typer.Context,
    experiment: ExperimentArg,
    host: str = typer.Option(
        "127.0.0.1", help="Host for the analysis server (not implemented)"
    ),
    port: int = typer.Option(
        8050, help="Port for the analysis server (not implemented)"
    ),
    open_browser: bool = typer.Option(
        True, help="Whether to open the browser automatically (not implemented)"
    ),
) -> None:
    """Run analysis for an experiment (not implemented)."""
    # Get learning mode settings from context
    learning_mode = ctx.obj.get("learning_mode", False) if ctx.obj else False
    verbosity = (
        ctx.obj.get("verbosity", VerbosityLevel.STANDARD)
        if ctx.obj
        else VerbosityLevel.STANDARD
    )
    learning_engine = LearningModeEngine(verbosity) if learning_mode else None

    result = run_analyze(experiment, host, port, open_browser, learning_engine)
    handle_tool_result(result, learning_mode)


def main(argv: list[str] | None = None) -> int | None:
    """Programmatic entry point used by tests; does not sys.exit.

    Passes standalone_mode=False so Click returns instead of exiting.
    """
    # Load experiment preparers explicitly at startup
    registry.load_preparers()

    cmd = get_command(app)
    return cmd.main(args=argv, standalone_mode=False)


def main_entry() -> None:
    """Console entry point wrapping the Typer application."""
    try:
        app()
    except KeyboardInterrupt:
        typer.echo("\nOperation cancelled by user", err=True)
        raise typer.Exit(1)
    except Exception as exc:  # pragma: no cover - defensive for console entry point
        typer.echo(f"Runtime CLI execution failed: {exc}", err=True)
        raise typer.Exit(1) from exc


# ---------------------------------------------------------------------------
# Simplified command implementations
# ---------------------------------------------------------------------------


def run_train_cmd(
    experiment: str,
    exp_config_path: Path | None,
    deps: CLIDependencies | None = None,
    learning_engine: LearningModeEngine | None = None,
    learning_mode: bool = False,
) -> None:
    """Run train command: load full ExperimentConfig once and pass section."""
    if deps is None:
        deps = get_cli_dependencies()
    exp = deps.load_experiment(experiment, exp_config_path)
    deps.ensure_train_prerequisites(exp)
    result = deps.run_train(
        experiment, exp.train, exp.shared.config_path, exp.shared, learning_engine
    )
    handle_tool_result(result, learning_mode)


def run_sample_cmd(
    experiment: str,
    exp_config_path: Path | None,
    deps: CLIDependencies | None = None,
    learning_engine: LearningModeEngine | None = None,
    learning_mode: bool = False,
) -> None:
    """Run sample command: load full ExperimentConfig once and pass section."""
    if deps is None:
        deps = get_cli_dependencies()
    exp = deps.load_experiment(experiment, exp_config_path)
    deps.ensure_sample_prerequisites(exp)
    result = deps.run_sample(
        experiment, exp.sample, exp.shared.config_path, exp.shared, learning_engine
    )
    handle_tool_result(result, learning_mode)


if __name__ == "__main__":
    # When executed as a script, run with default behavior (may exit)
    get_command(app)()
