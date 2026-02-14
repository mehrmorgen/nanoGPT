from __future__ import annotations

import pytest
from _pytest.capture import CaptureFixture

from typing import Any, Iterator
from contextlib import contextmanager

from pathlib import Path

from click.exceptions import Exit as ClickExit
from typer.testing import CliRunner
import ml_playground.tools.cli.main as cli
from ml_playground.tools.cli.commands import (
    ci as ci_commands,
    env as env_commands,
    quality as quality_commands,
    test as test_commands,
)
from ml_playground.tools.cli import helpers as cli_helpers
from ml_playground.tools.core.interfaces import OperationId, ToolResult
from ml_playground.tools.cli.state import reset_state
from ml_playground.tools.core.config import ToolsConfig


def make_result(*, success: bool, stdout: str = "", stderr: str = "") -> ToolResult:
    return ToolResult(
        success=success,
        exit_code=0 if success else 1,
        stdout=stdout,
        stderr=stderr,
        operation_id=OperationId(namespace="tools", category="dev", command="noop"),
    )


def test_handle_tool_result_success(capsys: CaptureFixture[str]) -> None:
    cli_helpers.handle_tool_result(make_result(success=True, stdout="ok", stderr=""))
    out, err = capsys.readouterr()
    assert "ok" in out
    assert err == ""


def test_handle_tool_result_failure_raises_exit(
    capsys: CaptureFixture[str],
) -> None:
    with pytest.raises(ClickExit):
        cli_helpers.handle_tool_result(make_result(success=False, stderr="bad"))
    out, err = capsys.readouterr()
    assert out == ""
    assert "bad" in err


def test_invoke_tests_invalid_suite_exits(capsys: CaptureFixture[str]) -> None:
    with pytest.raises(ClickExit):
        test_commands._invoke_tests("tests/unknown", None, [])
    _out, err = capsys.readouterr()
    assert "Unsupported test suite" in err


def test_load_config_with_error_handling_exit(
    tmp_path: Path, capsys: CaptureFixture[str]
) -> None:
    with pytest.raises(ClickExit):
        cli.load_config_with_error_handling(tmp_path)
    _out, err = capsys.readouterr()
    assert "Configuration error" in err


def test_tools_cli_getters_return_instances(tmp_path: Path) -> None:
    # Initialize state using repository root config
    cli.main(learning_mode=False, verbosity=0, dry_run=False, project_root=None)

    q = cli_helpers.get_quality_tools()
    t = cli_helpers.get_testing_tools()
    e = cli_helpers.get_environment_tools()
    c = cli_helpers.get_ci_tools()

    from ml_playground.tools.quality.quality import QualityTools as QT
    from ml_playground.tools.testing.testing import TestingTools as TT
    from ml_playground.tools.environment.environment import EnvironmentTools as ET
    from ml_playground.tools.ci.ci import CITools as CT

    assert isinstance(q, QT)
    assert isinstance(t, TT)
    assert isinstance(e, ET)
    assert isinstance(c, CT)


def test_get_dev_tools_requires_config() -> None:
    reset_state()
    with pytest.raises(RuntimeError):
        cli_helpers.get_dev_tools()


@pytest.mark.parametrize(  # type: ignore[misc]
    "getter",
    [
        cli_helpers.get_quality_tools,
        cli_helpers.get_testing_tools,
        cli_helpers.get_environment_tools,
        cli_helpers.get_ci_tools,
        cli_helpers.get_agentic_tools,
    ],
)
def test_tool_getters_require_config(getter: Any) -> None:
    reset_state()
    with pytest.raises(RuntimeError):
        getter()


def test_quality_getter_uses_project_root(tmp_path: Path) -> None:
    reset_state()
    state = cli.state
    state.config = ToolsConfig()  # type: ignore[attr-defined]
    state.project_root = tmp_path  # type: ignore[attr-defined]
    qt = cli_helpers.get_quality_tools()
    from ml_playground.tools.quality.quality import QualityTools as QT

    assert isinstance(qt, QT)


@contextmanager
def swap_attr(target: object, name: str, value: Any) -> Iterator[None]:
    original = getattr(target, name)
    object.__setattr__(target, name, value)
    try:
        yield
    finally:
        object.__setattr__(target, name, original)


def test_quality_commands_use_tool_getter_stubs() -> None:
    class StubQuality:
        def lint(self, *a: object, **k: object):
            return make_result(success=True, stdout="lint ok")

        def format(self, *a: object, **k: object):
            return make_result(success=True, stdout="format ok")

        def deadcode(self, *a: object, **k: object):
            return make_result(success=True, stdout="deadcode ok")

        def typecheck(self, *a: object, **k: object):
            return make_result(success=True, stdout="typecheck ok")

    with swap_attr(cli_helpers, "get_quality_tools", lambda: StubQuality()):
        quality_commands.quality_lint(None)
        quality_commands.quality_format(None)
        quality_commands.quality_deadcode(None)
        quality_commands.quality_typecheck(None)


def test_env_and_ci_commands_use_tool_getter_stubs() -> None:
    class StubEnv:
        def sync(self, *a: object, **k: object):
            return make_result(success=True, stdout="sync ok")

    class StubCI:
        def quality_fast(self, *a: object, **k: object):
            return make_result(success=True, stdout="fast ok")

        def quality_ext(self, *a: object, **k: object):
            return make_result(success=True, stdout="ext ok")

    with swap_attr(cli_helpers, "get_environment_tools", lambda: StubEnv()):
        env_commands.env_sync(groups=None, frozen=False, args=None)
    with swap_attr(cli_helpers, "get_ci_tools", lambda: StubCI()):
        ci_commands.ci_quality_fast(None)
        ci_commands.ci_quality_ext(None)


def test_testing_coverage_threshold_failure_raises() -> None:
    class StubTesting:
        def coverage(self, *a: object, **k: object):
            return make_result(success=False, stderr="threshold fail")

    runner = CliRunner()
    with swap_attr(cli_helpers, "get_testing_tools", lambda: StubTesting()):
        result = runner.invoke(test_commands.build_app(), ["coverage"])
    assert result.exit_code == 1
    assert "threshold fail" in (result.stderr or result.stdout)


def test_testing_command_dispatch_with_stubs() -> None:
    class StubTesting:
        def __init__(self) -> None:
            self.calls: list[str] = []

        def unit(self, args: list[str], *, learning_mode: bool, verbosity_level: int):
            self.calls.append("unit")
            return make_result(success=True, stdout="unit ok")

        def property_tests(
            self, args: list[str], *, learning_mode: bool, verbosity_level: int
        ):
            self.calls.append("property")
            return make_result(success=True, stdout="property ok")

        def regression(
            self, args: list[str], *, learning_mode: bool, verbosity_level: int
        ):
            self.calls.append("regression")
            return make_result(success=True, stdout="regression ok")

        def all_tests(
            self, args: list[str], *, learning_mode: bool, verbosity_level: int
        ):
            self.calls.append("all")
            return make_result(success=True, stdout="all ok")

        def coverage(
            self,
            args: list[str],
            *,
            line_threshold: float | None = None,
            branch_threshold: float | None = None,
            verbose: bool = False,
            learning_mode: bool,
            verbosity_level: int,
            force_regen: bool = False,
        ):
            self.calls.append("coverage")
            return make_result(success=True, stdout="coverage ok")

        def clean(self, args: list[str], *, learning_mode: bool, verbosity_level: int):
            self.calls.append("clean")
            return make_result(success=True, stdout="clean ok")

    runner = CliRunner()
    stub = StubTesting()
    with swap_attr(cli_helpers, "get_testing_tools", lambda: stub):
        assert runner.invoke(test_commands.build_app(), ["unit"]).exit_code == 0
        assert runner.invoke(test_commands.build_app(), ["property"]).exit_code == 0
        assert runner.invoke(test_commands.build_app(), ["regression"]).exit_code == 0
        assert runner.invoke(test_commands.build_app(), ["all"]).exit_code == 0
        assert runner.invoke(test_commands.build_app(), ["coverage"]).exit_code == 0
        assert runner.invoke(test_commands.build_app(), ["clean"]).exit_code == 0
    assert stub.calls == ["unit", "property", "regression", "all", "coverage", "clean"]


def test_ci_badge_command_with_stub() -> None:
    class StubCI:
        def coverage_badge(self, *a: object, **k: object):
            return make_result(success=True, stdout="badge ok")

    runner = CliRunner()
    with swap_attr(cli_helpers, "get_ci_tools", lambda: StubCI()):
        result = runner.invoke(ci_commands.app, ["coverage-badge"])
    assert result.exit_code == 0
    assert "badge ok" in result.stdout
