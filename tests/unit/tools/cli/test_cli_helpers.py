from __future__ import annotations

from contextlib import contextmanager
import pytest

from ml_playground.tools.core.interfaces import OperationId, ToolResult
from click.exceptions import Exit as ClickExit
import ml_playground.tools.cli as cli


def make_result(*, success: bool, stdout: str = "", stderr: str = "") -> ToolResult:
    return ToolResult(
        success=success,
        exit_code=0 if success else 1,
        stdout=stdout,
        stderr=stderr,
        operation_id=OperationId(namespace="tools", category="dev", command="noop"),
    )


def test_handle_tool_result_success(capsys: pytest.CaptureFixture[str]) -> None:
    cli._handle_tool_result(make_result(success=True, stdout="ok", stderr=""))
    out, err = capsys.readouterr()
    assert "ok" in out
    assert err == ""


def test_handle_tool_result_failure_raises_exit(
    capsys: pytest.CaptureFixture[str],
) -> None:
    with pytest.raises(ClickExit):
        cli._handle_tool_result(make_result(success=False, stderr="bad"))
    out, err = capsys.readouterr()
    assert "bad" in err


def test_invoke_tests_invalid_suite_exits(capsys: pytest.CaptureFixture[str]) -> None:
    ctx = None  # _invoke_tests doesn't access ctx
    with pytest.raises(ClickExit):
        cli._invoke_tests(ctx, "tests/unknown", None, [])  # type: ignore[arg-type]
    _out, err = capsys.readouterr()
    assert "Unsupported test suite" in err


def test_load_config_with_error_handling_exit(
    tmp_path, capsys: pytest.CaptureFixture[str]
) -> None:
    with pytest.raises(ClickExit):
        cli.load_config_with_error_handling(tmp_path)
    _out, err = capsys.readouterr()
    assert "Configuration error" in err


def test_tools_cli_getters_return_instances(tmp_path) -> None:
    # Initialize state using repository root config
    cli.main(learning_mode=False, verbosity=0, dry_run=False, project_root=None)

    q = cli._get_quality_tools()
    t = cli._get_testing_tools()
    e = cli._get_environment_tools()
    c = cli._get_ci_tools()

    from ml_playground.tools.categories.quality import QualityTools as QT
    from ml_playground.tools.categories.testing import TestingTools as TT
    from ml_playground.tools.categories.environment import EnvironmentTools as ET
    from ml_playground.tools.categories.ci import CITools as CT

    assert isinstance(q, QT)
    assert isinstance(t, TT)
    assert isinstance(e, ET)
    assert isinstance(c, CT)


@contextmanager
def swap_attr(target, name: str, value):
    original = getattr(target, name)
    setattr(target, name, value)
    try:
        yield
    finally:
        setattr(target, name, original)


def test_quality_commands_use_tool_getter_stubs() -> None:
    class StubQuality:
        def lint(self, *a, **k):
            return make_result(success=True, stdout="lint ok")

        def format(self, *a, **k):
            return make_result(success=True, stdout="format ok")

        def deadcode(self, *a, **k):
            return make_result(success=True, stdout="deadcode ok")

        def typecheck(self, *a, **k):
            return make_result(success=True, stdout="typecheck ok")

    with swap_attr(cli, "_get_quality_tools", lambda: StubQuality()):
        cli.quality_lint(None)
        cli.quality_format(None)
        cli.quality_deadcode(None)
        cli.quality_typecheck(None)


def test_env_and_ci_commands_use_tool_getter_stubs() -> None:
    class StubEnv:
        def sync(self, *a, **k):
            return make_result(success=True, stdout="sync ok")

    class StubCI:
        def quality_fast(self, *a, **k):
            return make_result(success=True, stdout="fast ok")

        def quality_ext(self, *a, **k):
            return make_result(success=True, stdout="ext ok")

    with swap_attr(cli, "_get_environment_tools", lambda: StubEnv()):
        cli.env_sync(groups=None, all_groups=False, frozen=False, args=None)
    with swap_attr(cli, "_get_ci_tools", lambda: StubCI()):
        cli.ci_quality_fast(None)
        cli.ci_quality_ext(None)


def test_testing_coverage_threshold_failure_raises() -> None:
    class StubTesting:
        def coverage_threshold(self, *a, **k):
            return make_result(success=False, stderr="threshold fail")

    with swap_attr(cli, "_get_testing_tools", lambda: StubTesting()):
        with pytest.raises(ClickExit):
            cli.test_coverage_threshold(0.0, 0.0, False, None)


def test_testing_command_dispatch_with_stubs() -> None:
    class StubTesting:
        def unit(self, args, *, learning_mode: bool, verbosity_level: int):
            return make_result(success=True, stdout="unit ok")

        def property_tests(self, args, *, learning_mode: bool, verbosity_level: int):
            return make_result(success=True, stdout="property ok")

        def regression(self, args, *, learning_mode: bool, verbosity_level: int):
            return make_result(success=True, stdout="regression ok")

        def all_tests(self, args, *, learning_mode: bool, verbosity_level: int):
            return make_result(success=True, stdout="all ok")

        def coverage_test(self, args, *, learning_mode: bool, verbosity_level: int):
            return make_result(success=True, stdout="cov test ok")

        def coverage_report(
            self, args, *, verbose: bool, learning_mode: bool, verbosity_level: int
        ):
            return make_result(success=True, stdout="cov report ok")

        def clean(self, args, *, learning_mode: bool, verbosity_level: int):
            return make_result(success=True, stdout="clean ok")

    with swap_attr(cli, "_get_testing_tools", lambda: StubTesting()):
        cli.test_unit(ctx=None, pattern=None, extra_args=None)
        cli.test_property(ctx=None, pattern=None, extra_args=None)
        cli.test_regression(ctx=None, pattern=None, extra_args=None)
        cli.test_all(None)
        cli.test_coverage_test(None)
        cli.test_coverage_report(verbose=False, args=None)
        cli.test_clean(None)


def test_ci_badge_command_with_stub() -> None:
    class StubCI:
        def coverage_badge(self, *a, **k):
            return make_result(success=True, stdout="badge ok")

    with swap_attr(cli, "_get_ci_tools", lambda: StubCI()):
        cli.ci_coverage_badge(None)
