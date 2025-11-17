"""Property-based tests for the runtime CLI bootstrap and command runners."""

from __future__ import annotations

import logging
from pathlib import Path
from types import SimpleNamespace
from typing import Callable

import hypothesis.strategies as st
from hypothesis import given, settings
import pytest
import typer

from ml_playground.runtime.cli.deps import (
    CLIDependencies,
    get_cli_dependencies,
    override_cli_dependencies,
)
from ml_playground.runtime.cli.result import run_or_exit
from ml_playground.runtime.cli.runners import (
    log_command_status,
    run_prepare_command,
    run_sample_command,
    run_train_command,
)
from ml_playground.runtime.core.results import ToolResult
from ml_playground.configuration.models import SharedConfig


EXPERIMENT_NAMES = st.text(
    alphabet="abcdefghijklmnopqrstuvwxyz0123456789_",
    min_size=1,
    max_size=10,
)
EXCEPTION_TYPES = st.sampled_from([FileNotFoundError, RuntimeError, ValueError])


class LoggerProbe:
    """Minimal logger fake that satisfies the LoggerLike protocol."""

    def __init__(self) -> None:
        self.infos: list[str] = []
        self.debugs: list[str] = []
        self.warnings: list[str] = []
        self.errors: list[str] = []

    def debug(self, msg: str, *args: object, **kwargs: object) -> None:
        self.debugs.append(msg % args if args else msg)

    def info(self, msg: str, *args: object, **kwargs: object) -> None:
        self.infos.append(msg % args if args else msg)

    def warning(self, msg: str, *args: object, **kwargs: object) -> None:
        self.warnings.append(msg % args if args else msg)

    def error(self, msg: str, *args: object, **kwargs: object) -> None:
        self.errors.append(msg % args if args else msg)


class StubDependencies:
    """In-memory CLIDependencies implementation used for property tests."""

    def __init__(self) -> None:
        self.loaded: list[tuple[str, Path | None]] = []
        self.prepare_calls: list[str] = []
        self.train_calls: list[str] = []
        self.sample_calls: list[str] = []
        self.ensure_train_calls: list[str] = []
        self.ensure_sample_calls: list[str] = []

        def load_experiment(
            experiment: str, exp_config: Path | None
        ) -> SimpleNamespace:
            self.loaded.append((experiment, exp_config))
            base = Path("/tmp")
            shared = SharedConfig(
                experiment=experiment,
                config_path=base / "config.toml",
                project_home=base,
                dataset_dir=base / "dataset",
                train_out_dir=base / "train_out",
                sample_out_dir=base / "sample_out",
            )
            runtime_cfg = SimpleNamespace(
                device="cpu",
                dtype="float32",
                seed=1337,
                out_dir=Path("out"),
            )
            logger = logging.getLogger("ml_playground.tests.runtime_cli")
            prepare_cfg = SimpleNamespace(logger=logger)
            train_cfg = SimpleNamespace(runtime=runtime_cfg, logger=logger)
            sample_cfg = SimpleNamespace(runtime=runtime_cfg, logger=logger)
            return SimpleNamespace(
                prepare=prepare_cfg,
                train=train_cfg,
                sample=sample_cfg,
                shared=shared,
            )

        def ensure_train_prerequisites(exp: SimpleNamespace) -> None:
            self.ensure_train_calls.append(exp.shared.experiment)

        def ensure_sample_prerequisites(exp: SimpleNamespace) -> None:
            self.ensure_sample_calls.append(exp.shared.experiment)

        def run_prepare(
            experiment: str,
            _cfg: SimpleNamespace,
            _config_path: Path,
            _shared: SimpleNamespace,
            _engine: object | None,
        ) -> ToolResult:
            self.prepare_calls.append(experiment)
            return ToolResult.create(
                success=True,
                exit_code=0,
                namespace="ml",
                category="prepare",
                command=experiment,
                stdout="prepare ok",
            )

        def run_train(
            experiment: str,
            _cfg: SimpleNamespace,
            _config_path: Path,
            _shared: SimpleNamespace,
            _engine: object | None,
        ) -> ToolResult:
            self.train_calls.append(experiment)
            return ToolResult.create(
                success=True,
                exit_code=0,
                namespace="ml",
                category="train",
                command=experiment,
                stdout="train ok",
            )

        def run_sample(
            experiment: str,
            _cfg: SimpleNamespace,
            _config_path: Path,
            _shared: SimpleNamespace,
            _engine: object | None,
        ) -> ToolResult:
            self.sample_calls.append(experiment)
            return ToolResult.create(
                success=True,
                exit_code=0,
                namespace="ml",
                category="sample",
                command=experiment,
                stdout="sample ok",
            )

        self.deps = CLIDependencies(
            load_experiment=load_experiment,
            ensure_train_prerequisites=ensure_train_prerequisites,
            ensure_sample_prerequisites=ensure_sample_prerequisites,
            run_prepare=run_prepare,
            run_train=run_train,
            run_sample=run_sample,
        )


def _invoke_direct(action: Callable[[], None]) -> None:
    """Run invoker used in tests to avoid run_or_exit indirection."""

    action()


@settings(max_examples=15, deadline=None, derandomize=True)
@given(experiment=EXPERIMENT_NAMES)
def test_override_cli_dependencies_restores_previous(experiment: str) -> None:
    """override_cli_dependencies should temporarily replace the active dependencies."""

    baseline = get_cli_dependencies()
    stub = StubDependencies()

    with override_cli_dependencies(stub.deps):
        active = get_cli_dependencies()
        assert active is stub.deps
        active.load_experiment(experiment, None)
        assert stub.loaded == [(experiment, None)]

    assert get_cli_dependencies() is baseline


@settings(max_examples=15, deadline=None, derandomize=True)
@given(experiment=EXPERIMENT_NAMES)
def test_run_prepare_command_uses_stub_dependencies(experiment: str) -> None:
    """run_prepare_command should delegate to the configured dependencies."""

    stub = StubDependencies()
    with override_cli_dependencies(stub.deps):
        result = run_prepare_command(
            experiment,
            exp_config_path=None,
            learning_mode=False,
            run_invoker=_invoke_direct,
        )

    assert result.success
    assert stub.prepare_calls == [experiment]
    assert stub.loaded == [(experiment, None)]


@settings(max_examples=15, deadline=None, derandomize=True)
@given(experiment=EXPERIMENT_NAMES)
def test_run_train_command_calls_prerequisites(experiment: str) -> None:
    """run_train_command should invoke prerequisite checks and the runner."""

    stub = StubDependencies()
    with override_cli_dependencies(stub.deps):
        result = run_train_command(
            experiment,
            exp_config_path=None,
            learning_mode=True,
            run_invoker=_invoke_direct,
        )

    assert result.success
    assert stub.ensure_train_calls == [experiment]
    assert stub.train_calls == [experiment]


@settings(max_examples=15, deadline=None, derandomize=True)
@given(experiment=EXPERIMENT_NAMES)
def test_run_sample_command_calls_prerequisites(experiment: str) -> None:
    """run_sample_command should invoke prerequisite checks and the runner."""

    stub = StubDependencies()
    with override_cli_dependencies(stub.deps):
        result = run_sample_command(
            experiment,
            exp_config_path=None,
            learning_mode=False,
            run_invoker=_invoke_direct,
        )

    assert result.success
    assert stub.ensure_sample_calls == [experiment]
    assert stub.sample_calls == [experiment]


@settings(max_examples=10, deadline=None, derandomize=True)
@given(exit_code=st.integers(min_value=1, max_value=20), exc_type=EXCEPTION_TYPES)
def test_run_or_exit_maps_exceptions_to_exit(
    exc_type: type[Exception], exit_code: int
) -> None:
    """run_or_exit should convert known exceptions into typer.Exit with the requested code."""

    def _raise() -> None:
        raise exc_type("boom")

    with pytest.raises(typer.Exit) as excinfo:
        run_or_exit(_raise, exception_exit_code=exit_code)

    assert excinfo.value.exit_code == exit_code


def test_run_or_exit_handles_keyboard_interrupt(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """KeyboardInterrupt should be logged and not re-raised as an error."""

    with caplog.at_level(logging.INFO, logger="ml_playground.runtime"):
        run_or_exit(
            lambda: (_ for _ in ()).throw(KeyboardInterrupt),
            keyboard_interrupt_msg="stopped",
        )

    assert "stopped" in caplog.text


def test_log_command_status_reports_directories(tmp_path: Path) -> None:
    """log_command_status should emit entries for dataset and output directories."""

    dataset_dir = tmp_path / "dataset"
    dataset_dir.mkdir()
    out_dir = tmp_path / "outputs"
    out_dir.mkdir()
    train_dir = tmp_path / "train"
    train_dir.mkdir()
    sample_dir = tmp_path / "sample"
    sample_dir.mkdir()

    (tmp_path / "config.toml").write_text("{}", encoding="utf-8")

    shared = SharedConfig(
        experiment="demo",
        config_path=tmp_path / "config.toml",
        project_home=tmp_path,
        dataset_dir=dataset_dir,
        train_out_dir=train_dir,
        sample_out_dir=sample_dir,
    )

    logger = LoggerProbe()
    log_command_status("tag", shared, out_dir, logger)

    combined = "\n".join(logger.infos)
    assert str(shared.dataset_dir) in combined
    assert str(out_dir) in combined
