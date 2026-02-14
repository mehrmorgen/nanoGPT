from __future__ import annotations

from contextlib import contextmanager
from types import SimpleNamespace
from typing import Any, Callable, Iterator, List

import pytest
import typer

import ml_playground.tools.cli.commands.testing as testing_commands
from ml_playground.tools.cli.state import state
from ml_playground.tools.core.errors import ToolConfigurationError, ToolExecutionError
from ml_playground.tools.core.interfaces import ToolResult


@contextmanager
def override_attr(obj: object, name: str, value: Any) -> Iterator[None]:
    original = getattr(obj, name)
    object.__setattr__(obj, name, value)
    try:
        yield
    finally:
        object.__setattr__(obj, name, original)


@contextmanager
def override_state(**overrides: object) -> Iterator[None]:
    originals = {key: getattr(state, key) for key in overrides}
    for key, value in overrides.items():
        object.__setattr__(state, key, value)
    try:
        yield
    finally:
        for key, value in originals.items():
            object.__setattr__(state, key, value)


def _tool_result(
    command: str, *, stdout: str = "ok", success: bool = True, stderr: str = ""
) -> ToolResult:
    return ToolResult.create(
        success=success,
        exit_code=0 if success else 1,
        namespace="tools",
        category="test",
        command=command,
        stdout=stdout,
        stderr=stderr,
    )


def _make_context() -> typer.Context:
    return SimpleNamespace()  # type: ignore[return-value]


class TestCoverageCommand:
    def test_coverage_runs_with_thresholds(self) -> None:
        captured: list[ToolResult] = []

        class StubTestingTools:
            def coverage(
                self,
                args: List[str],
                *,
                line_threshold: float,
                branch_threshold: float,
                verbose: bool,
                learning_mode: bool,
                verbosity_level: int,
                force_regen: bool,
            ) -> ToolResult:
                self.args = args
                self.line_threshold = line_threshold
                self.branch_threshold = branch_threshold
                self.verbose = verbose
                self.learning_mode = learning_mode
                self.verbosity_level = verbosity_level
                self.force_regen = force_regen
                return _tool_result("coverage", stdout="ran")

        stub = StubTestingTools()

        def _capture_run_tool_command(
            command_func: Callable[..., ToolResult], *args: Any, **kwargs: Any
        ) -> None:
            result = command_func(*args, **kwargs)
            captured.append(result)

        with override_state(learning_mode=True, verbosity=2):
            with override_attr(testing_commands, "get_testing_tools", lambda: stub):
                with override_attr(
                    testing_commands,
                    "run_tool_command",
                    _capture_run_tool_command,
                ):
                    testing_commands.test_coverage(
                        line_threshold=75.0,
                        branch_threshold=60.0,
                        force_regen=True,
                        verbose=True,
                        args=["--fast"],
                    )

        assert stub.args == ["--fast"]
        assert stub.line_threshold == 75.0
        assert stub.branch_threshold == 60.0
        assert stub.verbose is True
        assert stub.learning_mode is True
        assert stub.verbosity_level == 2
        assert stub.force_regen is True
        assert captured and captured[0].stdout == "ran"

    def test_coverage_handles_tool_error(self) -> None:
        captured: list[ToolResult] = []

        class FailureTools:
            def coverage(self, *args: object, **kwargs: object) -> ToolResult:  # noqa: ANN401
                return _tool_result(
                    "coverage",
                    success=False,
                    stderr="coverage failed",
                )

        def _capture_run_tool_command(
            command_func: Callable[..., ToolResult], *args: Any, **kwargs: Any
        ) -> None:
            result = command_func(*args, **kwargs)
            captured.append(result)

        with override_attr(
            testing_commands, "get_testing_tools", lambda: FailureTools()
        ):
            with override_attr(
                testing_commands,
                "run_tool_command",
                _capture_run_tool_command,
            ):
                testing_commands.test_coverage()

        assert captured and captured[0].success is False
        assert (captured[0].stderr or "") == "coverage failed"


class TestSuiteCommands:
    @pytest.mark.parametrize(
        ("command", "test_dir"),
        [
            (testing_commands.test_unit, "tests/unit"),
            (testing_commands.test_property, "tests/property"),
            (testing_commands.test_regression, "tests/regression"),
        ],
    )
    def test_suite_commands_delegate_to_invoke(
        self, command: Callable[..., None], test_dir: str
    ) -> None:
        recorded: list[tuple[Any, str, str | None, list[str]]] = []

        def _fake_invoke(
            ctx: typer.Context,
            invoke_dir: str,
            pattern: str | None,
            extra_args: list[str],
        ) -> None:
            recorded.append((ctx, invoke_dir, pattern, extra_args))

        ctx = _make_context()
        with override_attr(testing_commands, "_invoke_tests", _fake_invoke):
            command(ctx, pattern="slow", extra_args=["-k", "slow"])

        assert recorded
        _, invoke_dir, pattern, extra_args = recorded[0]
        assert invoke_dir == test_dir
        assert pattern == "slow"
        assert extra_args == ["-k", "slow"]


class TestInvokeTests:
    def test_invoke_tests_calls_suite_with_pattern_and_state(self) -> None:
        captured: list[ToolResult] = []

        class StubTestingTools:
            def __init__(self) -> None:
                self.calls: list[tuple[list[str], bool, int]] = []

            def unit(
                self,
                args: list[str],
                *,
                learning_mode: bool,
                verbosity_level: int,
            ) -> ToolResult:
                self.calls.append((args, learning_mode, verbosity_level))
                return _tool_result("unit")

        stub = StubTestingTools()

        def _capture_run_tool_command(
            command_func: Callable[..., ToolResult], *args: Any, **kwargs: Any
        ) -> None:
            result = command_func(*args, **kwargs)
            captured.append(result)

        with override_state(learning_mode=True, verbosity=2):
            with override_attr(testing_commands, "get_testing_tools", lambda: stub):
                with override_attr(
                    testing_commands,
                    "run_tool_command",
                    _capture_run_tool_command,
                ):
                    ctx = _make_context()
                    testing_commands._invoke_tests(
                        ctx,
                        "tests/unit",
                        pattern="fast",
                        extra_args=["-q"],
                    )

        assert stub.calls == [(["-q", "-k", "fast"], True, 2)]
        assert captured and captured[0].operation_id.command == "unit"

    def test_invoke_tests_handles_tool_execution_error(self) -> None:
        captured: list[ToolResult] = []

        class FailingTools:
            def unit(self, *args: object, **kwargs: object) -> ToolResult:  # noqa: ANN401
                raise ToolExecutionError("boom", reason="r", rationale="x")

        def _capture_run_tool_command(
            command_func: Callable[..., ToolResult], *args: Any, **kwargs: Any
        ) -> None:
            try:
                result = command_func(*args, **kwargs)
            except ToolExecutionError as e:  # type: ignore[unused-ignore]
                # Simulate the behavior of run_tool_command by synthesizing
                # a generic failure ToolResult while letting tests assert on
                # the stderr content instead of the exact command name.
                result = ToolResult.create(
                    success=False,
                    exit_code=1,
                    namespace="tools",
                    category="test",
                    command="generic-error",
                    stderr=f"Error running tests in tests/unit: {e}",
                )
            captured.append(result)

        with override_attr(
            testing_commands, "get_testing_tools", lambda: FailingTools()
        ):
            with override_attr(
                testing_commands,
                "run_tool_command",
                _capture_run_tool_command,
            ):
                ctx = _make_context()
                testing_commands._invoke_tests(
                    ctx, "tests/unit", pattern=None, extra_args=[]
                )

        assert captured and captured[0].success is False
        assert captured[0].operation_id.category == "test"
        assert captured[0].operation_id.command == "generic-error"
        assert "Error running tests in tests/unit" in (captured[0].stderr or "")

    def test_invoke_tests_raises_on_unknown_suite(self) -> None:
        ctx = _make_context()
        with pytest.raises(Exception):
            testing_commands._invoke_tests(ctx, "tests/unknown", None, [])


class TestAllAndCleanCommands:
    def test_test_all_delegates_to_tools(self) -> None:
        captured: list[ToolResult] = []

        class StubTestingTools:
            def all_tests(
                self,
                args: List[str],
                *,
                learning_mode: bool,
                verbosity_level: int,
            ) -> ToolResult:
                self.args = args
                self.learning_mode = learning_mode
                self.verbosity_level = verbosity_level
                return _tool_result("all", stdout="all done")

        stub = StubTestingTools()

        def _capture_run_tool_command(
            command_func: Callable[..., ToolResult], *args: Any, **kwargs: Any
        ) -> None:
            result = command_func(*args, **kwargs)
            captured.append(result)

        with override_state(learning_mode=False, verbosity=1):
            with override_attr(testing_commands, "get_testing_tools", lambda: stub):
                with override_attr(
                    testing_commands,
                    "run_tool_command",
                    _capture_run_tool_command,
                ):
                    testing_commands.test_all(args=["-n", "auto"])

        assert stub.args == ["-n", "auto"]
        assert stub.learning_mode is False
        assert stub.verbosity_level == 1
        assert captured and captured[0].stdout == "all done"

    def test_test_all_handles_configuration_error(self) -> None:
        captured: list[ToolResult] = []

        class FailingTools:
            def all_tests(self, *args: object, **kwargs: object) -> ToolResult:  # noqa: ANN401
                raise ToolConfigurationError("bad config", reason="a", rationale="b")

        def _capture_run_tool_command(
            command_func: Callable[..., ToolResult], *args: Any, **kwargs: Any
        ) -> None:
            try:
                result = command_func(*args, **kwargs)
            except ToolConfigurationError as e:  # type: ignore[unused-ignore]
                result = ToolResult.create(
                    success=False,
                    exit_code=1,
                    namespace="tools",
                    category="test",
                    command="generic-error",
                    stderr=f"Error running all tests: {e}",
                )
            captured.append(result)

        with override_attr(
            testing_commands, "get_testing_tools", lambda: FailingTools()
        ):
            with override_attr(
                testing_commands,
                "run_tool_command",
                _capture_run_tool_command,
            ):
                testing_commands.test_all(args=[])

        assert captured and captured[0].success is False
        assert captured[0].operation_id.category == "test"
        assert captured[0].operation_id.command == "generic-error"
        assert "Error running all tests" in (captured[0].stderr or "")

    def test_test_clean_delegates(self) -> None:
        captured: list[ToolResult] = []

        class StubTestingTools:
            def clean(
                self,
                args: List[str],
                *,
                learning_mode: bool,
                verbosity_level: int,
            ) -> ToolResult:
                self.args = args
                self.learning_mode = learning_mode
                self.verbosity_level = verbosity_level
                return _tool_result("clean")

        stub = StubTestingTools()

        def _capture_run_tool_command(
            command_func: Callable[..., ToolResult], *args: Any, **kwargs: Any
        ) -> None:
            result = command_func(*args, **kwargs)
            captured.append(result)

        with override_state(learning_mode=True, verbosity=0):
            with override_attr(testing_commands, "get_testing_tools", lambda: stub):
                with override_attr(
                    testing_commands,
                    "run_tool_command",
                    _capture_run_tool_command,
                ):
                    testing_commands.test_clean(args=["--purge"])

        assert stub.args == ["--purge"]
        assert stub.learning_mode is True
        assert stub.verbosity_level == 0
        assert captured and captured[0].operation_id.command == "clean"

    def test_test_clean_handles_tool_execution_error(self) -> None:
        captured: list[ToolResult] = []

        class FailingTools:
            def clean(self, *args: object, **kwargs: object) -> ToolResult:  # noqa: ANN401
                raise ToolExecutionError("clean failed", reason="c", rationale="d")

        def _capture_run_tool_command(
            command_func: Callable[..., ToolResult], *args: Any, **kwargs: Any
        ) -> None:
            try:
                result = command_func(*args, **kwargs)
            except ToolExecutionError as e:  # type: ignore[unused-ignore]
                result = ToolResult.create(
                    success=False,
                    exit_code=1,
                    namespace="tools",
                    category="test",
                    command="generic-error",
                    stderr=f"Error cleaning test artifacts: {e}",
                )
            captured.append(result)

        with override_attr(
            testing_commands, "get_testing_tools", lambda: FailingTools()
        ):
            with override_attr(
                testing_commands,
                "run_tool_command",
                _capture_run_tool_command,
            ):
                testing_commands.test_clean()

        assert captured and captured[0].success is False
        assert captured[0].operation_id.category == "test"
        assert captured[0].operation_id.command == "generic-error"
        assert "Error cleaning test artifacts" in (captured[0].stderr or "")
