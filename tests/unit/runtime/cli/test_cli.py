from __future__ import annotations

import logging
from pathlib import Path
from types import SimpleNamespace

import pytest
from typer.testing import CliRunner

import ml_playground.runtime.cli as cli
from ml_playground.runtime.cli import CLIDependencies, override_cli_dependencies
from ml_playground.runtime.core.results import ToolResult


def test_cli_prepare_invokes_overridden_dependency(tmp_path: Path) -> None:
    runner = CliRunner()
    calls: list[str] = []

    shared = SimpleNamespace(
        experiment="demo",
        config_path=tmp_path / "config.toml",
        project_home=tmp_path,
        dataset_dir=tmp_path / "dataset",
        train_out_dir=tmp_path / "train",
        sample_out_dir=tmp_path / "sample",
    )

    def _load_experiment(name: str, exp_config: Path | None) -> SimpleNamespace:
        assert name == "demo"
        assert exp_config is None
        return SimpleNamespace(
            prepare="prepare_cfg", train="train_cfg", sample="sample_cfg", shared=shared
        )

    def _run_prepare(
        experiment: str,
        prepare_cfg: object,
        config_path: Path,
        shared_cfg: object,
        learning_mode_engine: object | None,
    ) -> ToolResult:
        assert experiment == "demo"
        assert config_path == shared.config_path
        calls.append("prepare")
        return ToolResult.create(
            success=True,
            exit_code=0,
            namespace="ml",
            category="prepare",
            command="demo",
        )

    def _run_train(
        experiment: str,
        train_cfg: object,
        config_path: Path,
        shared_cfg: object,
        learning_mode_engine: object | None,
    ) -> ToolResult:
        return ToolResult.create(
            success=True, exit_code=0, namespace="ml", category="train", command="demo"
        )

    def _run_sample(
        experiment: str,
        sample_cfg: object,
        config_path: Path,
        shared_cfg: object,
        learning_mode_engine: object | None,
    ) -> ToolResult:
        return ToolResult.create(
            success=True, exit_code=0, namespace="ml", category="sample", command="demo"
        )

    deps = CLIDependencies(
        load_experiment=_load_experiment,
        ensure_train_prerequisites=lambda _: None,
        ensure_sample_prerequisites=lambda _: None,
        run_prepare=_run_prepare,
        run_train=_run_train,
        run_sample=_run_sample,
    )

    with override_cli_dependencies(deps):
        result = runner.invoke(cli.app, ["prepare", "demo"])

    assert result.exit_code == 0
    assert calls == ["prepare"]


@pytest.mark.parametrize(
    "exc_type, exit_code", [(FileNotFoundError, 7), (ValueError, 5)]
)
def test_run_or_exit_maps_known_exceptions(
    exc_type: type[Exception], exit_code: int
) -> None:
    def _raise() -> None:
        raise exc_type("boom")

    with pytest.raises(cli.typer.Exit) as excinfo:
        cli.run_or_exit(_raise, exception_exit_code=exit_code)

    assert excinfo.value.exit_code == exit_code


def test_global_options_basic_logging(tmp_path: Path) -> None:
    root_logger = logging.getLogger()
    original_handlers = list(root_logger.handlers)
    for handler in original_handlers:
        root_logger.removeHandler(handler)
    try:
        ctx = cli.typer.Context(cli.get_command(cli.app))
        cli.global_options(ctx, exp_config=None)
        assert ctx.obj == {"exp_config": None}
    finally:
        for handler in list(root_logger.handlers):
            root_logger.removeHandler(handler)
        for handler in original_handlers:
            root_logger.addHandler(handler)
