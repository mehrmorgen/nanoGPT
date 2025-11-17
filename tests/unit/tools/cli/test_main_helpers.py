from __future__ import annotations

from pathlib import Path
from typing import Callable, Generator

import pytest

from ml_playground.tools.core.config import ToolsConfig
from ml_playground.tools.core.interfaces import OperationId, ToolResult
from click.exceptions import Exit as ClickExit
import ml_playground.tools.cli.main as cli


def make_result(*, success: bool, stdout: str = "", stderr: str = "") -> ToolResult:
    return ToolResult(
        success=success,
        exit_code=0 if success else 1,
        stdout=stdout,
        stderr=stderr,
        operation_id=OperationId(namespace="tools", category="dev", command="noop"),
    )


def _deps(
    *,
    load_config: Callable[[Path | None], ToolsConfig] | None = None,
    quality_factory: Callable[[ToolsConfig, Path], object] | None = None,
    testing_factory: Callable[[ToolsConfig, Path], object] | None = None,
    environment_factory: Callable[[ToolsConfig, Path], object] | None = None,
    ci_factory: Callable[[ToolsConfig, Path], object] | None = None,
    agentic_factory: Callable[[ToolsConfig, Path], object] | None = None,
    dev_factory: Callable[[ToolsConfig], object] | None = None,
    result_handler: Callable[[ToolResult], None] | None = None,
) -> cli.ToolsDependencies:
    """Create ToolsDependencies with test-friendly defaults."""
    base = cli.default_tools_dependencies()
    return cli.ToolsDependencies(
        load_config=load_config or (lambda _root: cli.ToolsConfig()),
        quality_factory=quality_factory or base.quality_factory,  # type: ignore[arg-type]
        testing_factory=testing_factory or base.testing_factory,  # type: ignore[arg-type]
        environment_factory=environment_factory or base.environment_factory,  # type: ignore[arg-type]
        ci_factory=ci_factory or base.ci_factory,  # type: ignore[arg-type]
        dev_factory=dev_factory or base.dev_factory,  # type: ignore[arg-type]
        result_handler=result_handler or cli.default_tool_result_handler,
    )


@pytest.fixture(autouse=True)
def reset_tools_state() -> Generator[None, None, None]:
    cli.state = cli.GlobalState()
    cli.reset_tools_dependencies()
    yield
    cli.state = cli.GlobalState()
    cli.reset_tools_dependencies()


def test_handle_tool_result_delegates_to_dependencies() -> None:
    captured: list[ToolResult] = []
    deps = _deps(result_handler=lambda result: captured.append(result))
    with cli.override_tools_dependencies(deps):
        cli.handle_tool_result(make_result(success=True, stdout="ok"))

    assert captured and captured[0].stdout == "ok"


def test_default_tool_result_handler_writes_output(
    capsys: pytest.CaptureFixture[str],
) -> None:
    cli.default_tool_result_handler(make_result(success=True, stdout="ok"))
    out, err = capsys.readouterr()
    assert "ok" in out
    assert err == ""


def test_default_tool_result_handler_failure(
    capsys: pytest.CaptureFixture[str],
) -> None:
    with pytest.raises(ClickExit):
        cli.default_tool_result_handler(make_result(success=False, stderr="bad"))
    _out, err = capsys.readouterr()
    assert "bad" in err


def test_load_config_with_error_handling_exit(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    def _raise_config(_root: Path | None = None) -> ToolsConfig:
        raise cli.ToolConfigurationError("bad", reason="oops", rationale="testing")

    deps = _deps(load_config=_raise_config)
    with cli.override_tools_dependencies(deps):
        with pytest.raises(ClickExit):
            cli.load_config_with_error_handling(tmp_path)
    _out, err = capsys.readouterr()
    assert "Configuration error" in err


def test_tools_cli_getters_return_instances(tmp_path: Path) -> None:
    cli.main(learning_mode=False, verbosity=0, dry_run=False, project_root=None)

    q = cli.get_quality_tools()
    t = cli.get_testing_tools()
    e = cli.get_environment_tools()
    c = cli.get_ci_tools()

    from ml_playground.tools.quality.quality import QualityTools as QT
    from ml_playground.tools.testing.testing import TestingTools as TT
    from ml_playground.tools.environment.environment import EnvironmentTools as ET
    from ml_playground.tools.ci.ci import CITools as CT

    assert isinstance(q, QT)
    assert isinstance(t, TT)
    assert isinstance(e, ET)
    assert isinstance(c, CT)


def _set_stub_config() -> None:
    cli.state.config = ToolsConfig()
    cli.state.project_root = Path.cwd()


def test_quality_commands_use_tool_getter_stubs() -> None:
    class StubQuality:
        def lint(self, *args: object, **kwargs: object) -> ToolResult:
            return make_result(success=True, stdout="lint ok")

        def format(self, *args: object, **kwargs: object) -> ToolResult:
            return make_result(success=True, stdout="format ok")

        def deadcode(self, *args: object, **kwargs: object) -> ToolResult:
            return make_result(success=True, stdout="deadcode ok")

        def typecheck(self, *args: object, **kwargs: object) -> ToolResult:
            return make_result(success=True, stdout="typecheck ok")

    deps = _deps(quality_factory=lambda _cfg, _root: StubQuality())

    with cli.override_tools_dependencies(deps):
        _set_stub_config()
        cli.quality_lint(None)
        cli.quality_format(None)
        cli.quality_deadcode(None)
        cli.quality_typecheck(None)


def test_env_and_ci_commands_use_tool_getter_stubs() -> None:
    class StubEnv:
        def sync(self, *args: object, **kwargs: object) -> ToolResult:
            return make_result(success=True, stdout="sync ok")

    class StubCI:
        def quality_fast(self, *args: object, **kwargs: object) -> ToolResult:
            return make_result(success=True, stdout="fast ok")

        def quality_ext(self, *args: object, **kwargs: object) -> ToolResult:
            return make_result(success=True, stdout="ext ok")

    deps = _deps(
        environment_factory=lambda _cfg, _root: StubEnv(),
        ci_factory=lambda _cfg, _root: StubCI(),
    )

    with cli.override_tools_dependencies(deps):
        _set_stub_config()
        cli.env_sync(groups=None, all_groups=False, frozen=False, args=None)
        cli.ci_quality_fast(None)
        cli.ci_quality_ext(None)


def test_testing_coverage_threshold_failure_raises() -> None:
    class StubTesting:
        def coverage(self, *args: object, **kwargs: object) -> ToolResult:
            return make_result(success=False, stderr="threshold fail")

    deps = _deps(testing_factory=lambda _cfg, _root: StubTesting())

    with cli.override_tools_dependencies(deps):
        _set_stub_config()
        with pytest.raises(ClickExit):
            cli.test_coverage(
                line_threshold=0.0,
                branch_threshold=0.0,
                force_regen=False,
                verbose=False,
                args=None,
            )


def test_testing_command_dispatch_with_stubs() -> None:
    class StubTesting:
        def unit(
            self, args: object, *, learning_mode: bool, verbosity_level: int
        ) -> ToolResult:
            return make_result(success=True, stdout="unit ok")

        def property_tests(
            self, args: object, *, learning_mode: bool, verbosity_level: int
        ) -> ToolResult:
            return make_result(success=True, stdout="property ok")

        def regression(
            self, args: object, *, learning_mode: bool, verbosity_level: int
        ) -> ToolResult:
            return make_result(success=True, stdout="regression ok")

        def all_tests(
            self, args: object, *, learning_mode: bool, verbosity_level: int
        ) -> ToolResult:
            return make_result(success=True, stdout="all ok")

        def coverage(
            self,
            args: object,
            *,
            line_threshold: float | None = None,
            branch_threshold: float | None = None,
            verbose: bool = False,
            learning_mode: bool,
            verbosity_level: int,
            force_regen: bool = False,
        ) -> ToolResult:
            return make_result(success=True, stdout="coverage ok")

        def clean(
            self, args: object, *, learning_mode: bool, verbosity_level: int
        ) -> ToolResult:
            return make_result(success=True, stdout="clean ok")

    deps = _deps(testing_factory=lambda _cfg, _root: StubTesting())

    with cli.override_tools_dependencies(deps):
        _set_stub_config()
        cli.test_unit(ctx=None, pattern=None, extra_args=None)  # type: ignore[arg-type]
        cli.test_property(ctx=None, pattern=None, extra_args=None)  # type: ignore[arg-type]
        cli.test_regression(ctx=None, pattern=None, extra_args=None)  # type: ignore[arg-type]
        cli.test_all(None)
        cli.test_coverage(
            line_threshold=0.0,
            branch_threshold=0.0,
            force_regen=False,
            verbose=False,
            args=None,
        )
        cli.test_clean(None)


def test_ci_badge_command_with_stub() -> None:
    class StubCI:
        def coverage_badge(self, *args: object, **kwargs: object) -> ToolResult:
            return make_result(success=True, stdout="badge ok")

    deps = _deps(ci_factory=lambda _cfg, _root: StubCI())

    with cli.override_tools_dependencies(deps):
        _set_stub_config()
        cli.ci_coverage_badge(None)
