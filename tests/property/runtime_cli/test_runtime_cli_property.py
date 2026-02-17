from __future__ import annotations

import logging
from dataclasses import dataclass, field
from pathlib import Path
from types import SimpleNamespace
from typing import List, Sequence

import hypothesis.strategies as st
from hypothesis import assume, example, given, settings
from click.testing import Result
import typer
from typer.testing import CliRunner

import ml_playground.runtime_cli.main as cli_main
import ml_playground.runtime_cli.runners as cli_runners
from ml_playground.framework.runtime.core.results import ToolResult

CLI_RUNNER = CliRunner()
RUNTIME_COMMANDS = ("prepare", "train", "sample")
VALID_TOKENS = set(RUNTIME_COMMANDS + ("analyze",))


@dataclass
class DependencyCallLog:
    loaded: list[tuple[str, Path | None]] = field(default_factory=list)
    prepare_runs: list[str] = field(default_factory=list)
    train_runs: list[str] = field(default_factory=list)
    sample_runs: list[str] = field(default_factory=list)
    analyze_runs: list[str] = field(default_factory=list)
    ensure_train: list[str] = field(default_factory=list)
    ensure_sample: list[str] = field(default_factory=list)


def _success_result(category: str, command: str) -> ToolResult:
    return ToolResult.create(
        success=True,
        exit_code=0,
        namespace="ml",
        category=category,
        command=command,
        stdout=f"{category}:{command}:ok",
    )


def _build_stub_dependencies(log: DependencyCallLog) -> cli_runners.CLIDependencies:
    def load_experiment(experiment: str, exp_config: Path | None) -> SimpleNamespace:
        log.loaded.append((experiment, exp_config))
        logger = logging.getLogger("ml_playground.framework.runtime.tests")
        runtime_cfg = SimpleNamespace(device="cpu", dtype="float32", seed=42)
        metadata = SimpleNamespace(
            config_path=Path.cwd() / "config.toml",
            dataset_dir=Path.cwd(),
            train_out_dir=Path.cwd(),
            sample_out_dir=Path.cwd(),
        )
        prepare_cfg = SimpleNamespace(logger=logger)
        training_cfg = SimpleNamespace(logger=logger, runtime=runtime_cfg)
        sampling_cfg = SimpleNamespace(logger=logger, runtime=runtime_cfg)
        return SimpleNamespace(
            prepare=prepare_cfg,
            training=training_cfg,
            sampling=sampling_cfg,
            metadata=metadata,
        )

    def run_prepare(
        experiment: str,
        _prepare_cfg: SimpleNamespace,
        _config_path: Path,
        metadata: SimpleNamespace,
        deps: cli_runners.CLIDependencies,
        _learning_engine: object | None = None,
    ) -> ToolResult:
        metadata.experiment = experiment
        log.prepare_runs.append(experiment)
        return _success_result("prepare", experiment)

    def run_train(
        experiment: str,
        _train_cfg: SimpleNamespace,
        _config_path: Path,
        metadata: SimpleNamespace,
        deps: cli_runners.CLIDependencies,
        _learning_engine: object | None = None,
    ) -> ToolResult:
        metadata.experiment = experiment
        log.train_runs.append(experiment)
        return _success_result("train", experiment)

    def run_sample(
        experiment: str,
        _sample_cfg: SimpleNamespace,
        _config_path: Path,
        metadata: SimpleNamespace,
        deps: cli_runners.CLIDependencies,
        _learning_engine: object | None = None,
    ) -> ToolResult:
        metadata.experiment = experiment
        log.sample_runs.append(experiment)
        return _success_result("sample", experiment)

    def run_analyze(
        experiment: str,
        _host: str,
        _port: int,
        _open_browser: bool,
        _learning_engine: object | None = None,
        *,
        metadata: object | None = None,
        exp_config_path: Path | None = None,
    ) -> ToolResult:
        del metadata, exp_config_path
        log.analyze_runs.append(experiment)
        if experiment != "bundestag_char":
            return ToolResult.create(
                success=False,
                exit_code=1,
                namespace="ml",
                category="analyze",
                command=experiment,
                stdout="analyze currently supports only 'bundestag_char'",
            )
        return _success_result("analyze", experiment)

    def handle_tool_result(result: ToolResult, _learning_mode: bool) -> None:
        if not result.success:
            raise typer.Exit(result.exit_code)

    return cli_runners.CLIDependencies(
        load_experiment=load_experiment,
        ensure_train_prerequisites=lambda exp: log.ensure_train.append(
            getattr(exp.metadata, "experiment", "")
        ),
        ensure_sample_prerequisites=lambda exp: log.ensure_sample.append(
            getattr(exp.metadata, "experiment", "")
        ),
        run_prepare=run_prepare,
        run_train=run_train,
        run_sample=run_sample,
        run_analyze=run_analyze,
        handle_tool_result=handle_tool_result,
    )


def _build_flags(learning_mode: bool, verbosity: int | None) -> List[str]:
    flags: List[str] = []
    if learning_mode:
        flags.append("--learning-mode")
    if verbosity is not None:
        flags.extend(["--verbosity", str(verbosity)])
    return flags


GLOBAL_FLAGS_STRATEGY = st.builds(
    _build_flags,
    learning_mode=st.booleans(),
    verbosity=st.none() | st.integers(min_value=0, max_value=2),
)

COMMAND_STRATEGY = st.sampled_from(RUNTIME_COMMANDS)
EXPERIMENT_NAME_STRATEGY = st.text(
    alphabet="abcdefghijklmnopqrstuvwxyz0123456789",
    min_size=1,
    max_size=8,
)
INVALID_TOKEN_POOL = [f"invalid-{idx}" for idx in range(64)]
INVALID_TOKEN_STRATEGY = st.sampled_from(
    [token for token in INVALID_TOKEN_POOL if token not in VALID_TOKENS]
)


def _invoke_runtime_cli(
    raw_args: Sequence[str], log: DependencyCallLog | None = None
) -> Result:
    if log is None:
        return CLI_RUNNER.invoke(cli_main.app, list(raw_args))
    deps = _build_stub_dependencies(log)
    return CLI_RUNNER.invoke(cli_main.app, list(raw_args), obj={"cli_deps": deps})


@given(flags=GLOBAL_FLAGS_STRATEGY)
@example(flags=[])
@settings(max_examples=30, deadline=None, derandomize=True)
def test_runtime_cli_without_subcommand_shows_guidance(flags: List[str]) -> None:
    result = _invoke_runtime_cli(flags, log=None)
    assert result.exit_code == 2


@given(
    flags=GLOBAL_FLAGS_STRATEGY,
    command=COMMAND_STRATEGY,
    experiment=EXPERIMENT_NAME_STRATEGY,
)
@example(flags=[], command="prepare", experiment="demo")
@settings(max_examples=30, deadline=None, derandomize=True)
def test_runtime_cli_commands_use_dependencies(
    flags: List[str], command: str, experiment: str
) -> None:
    log = DependencyCallLog()
    args = [*flags, command, experiment]
    result = _invoke_runtime_cli(args, log)
    assert result.exit_code == 0
    assert (experiment, None) in log.loaded
    if command == "prepare":
        assert experiment in log.prepare_runs
    elif command == "train":
        assert experiment in log.train_runs
    else:
        assert experiment in log.sample_runs


@given(flags=GLOBAL_FLAGS_STRATEGY, invalid=INVALID_TOKEN_STRATEGY)
@example(flags=[], invalid="bogus")
@settings(max_examples=30, deadline=None, derandomize=True)
def test_runtime_cli_reports_unknown_commands(flags: List[str], invalid: str) -> None:
    assume(invalid)
    result = _invoke_runtime_cli([*flags, invalid], log=None)
    assert result.exit_code != 0
    stream = (result.stderr or result.stdout).lower()
    assert "no such command" in stream or "unknown command" in stream


@settings(max_examples=30, deadline=None, derandomize=True)
@given(flags=GLOBAL_FLAGS_STRATEGY)
def test_runtime_cli_invalid_exp_config_is_rejected(flags: List[str]) -> None:
    missing = Path("nonexistent-config-path.toml")
    assume(not missing.exists())
    result = _invoke_runtime_cli(
        [*flags, "--exp-config", str(missing), "prepare", "demo"],
        log=DependencyCallLog(),
    )
    assert result.exit_code == 2
    if result.stderr:
        assert "Config file not found" in result.stderr


@given(flags=GLOBAL_FLAGS_STRATEGY, experiment=EXPERIMENT_NAME_STRATEGY)
@example(flags=[], experiment="invalid")
@settings(max_examples=30, deadline=None, derandomize=True)
def test_runtime_cli_analyze_unknown_experiment(
    flags: List[str], experiment: str
) -> None:
    assume(experiment != "bundestag_char")
    log = DependencyCallLog()
    result = _invoke_runtime_cli([*flags, "analyze", experiment], log=log)
    assert result.exit_code == 1
    stream = result.stderr or result.stdout
    if stream:
        assert "supports only 'bundestag_char'" in stream
