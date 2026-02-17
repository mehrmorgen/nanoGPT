from __future__ import annotations

from contextlib import contextmanager
from typing import Any, Callable, Iterator, List

import pytest
import typer

import ml_playground.tools.cli.commands.quality as quality_commands
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


def _tool_result(command: str, *, stdout: str = "ok") -> ToolResult:
    return ToolResult.create(
        success=True,
        exit_code=0,
        namespace="tools",
        category="quality",
        command=command,
        stdout=stdout,
    )


class TestQualityLint:
    def test_quality_lint_delegates_to_tools(self) -> None:
        captured: list[ToolResult] = []

        class StubQualityTools:
            def lint(
                self,
                args: List[str],
                *,
                learning_mode: bool,
                verbosity_level: int,
            ) -> ToolResult:
                self.args = args
                self.learning_mode = learning_mode
                self.verbosity_level = verbosity_level
                return _tool_result("lint", stdout="lint ok")

        stub = StubQualityTools()

        def _capture_run_tool_command(
            command_func: Callable[..., ToolResult], *args: Any, **kwargs: Any
        ) -> None:
            result = command_func(*args, **kwargs)
            captured.append(result)

        with override_attr(quality_commands, "get_quality_tools", lambda: stub):
            with override_attr(
                quality_commands,
                "run_tool_command",
                _capture_run_tool_command,
            ):
                quality_commands.quality_lint(args=["--fix"])

        assert stub.args == ["--fix"]
        assert stub.learning_mode is quality_commands.state.learning_mode
        assert stub.verbosity_level == quality_commands.state.verbosity
        assert captured and captured[0].stdout == "lint ok"

    def test_quality_lint_handles_tool_error(self) -> None:
        captured: list[ToolResult] = []

        class FailingTools:
            def lint(self, *args: object, **kwargs: object) -> ToolResult:  # noqa: ANN401
                raise ToolExecutionError("lint failed", reason="x", rationale="y")

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
                    category="quality",
                    command="generic-error",
                    stderr=str(e),
                )
            captured.append(result)

        with override_attr(
            quality_commands, "get_quality_tools", lambda: FailingTools()
        ):
            with override_attr(
                quality_commands,
                "run_tool_command",
                _capture_run_tool_command,
            ):
                quality_commands.quality_lint()

        assert captured and captured[0].success is False
        assert captured[0].operation_id.category == "quality"
        assert captured[0].operation_id.command == "generic-error"
        assert "lint failed" in (captured[0].stderr or "")


class TestQualityFormat:
    def test_quality_format_re_raises_typer_exit(self) -> None:
        class ExitTools:
            def format(self, *args: object, **kwargs: object) -> ToolResult:  # noqa: ANN401
                raise typer.Exit(5)

        with override_attr(quality_commands, "get_quality_tools", lambda: ExitTools()):
            with pytest.raises(typer.Exit) as exc:
                quality_commands.quality_format()
        assert exc.value.exit_code == 5

    def test_quality_format_handles_tool_error(self) -> None:
        captured: list[ToolResult] = []

        class FailingTools:
            def format(self, *args: object, **kwargs: object) -> ToolResult:  # noqa: ANN401
                raise ToolConfigurationError("bad format", reason="r", rationale="z")

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
                    category="quality",
                    command="generic-error",
                    stderr=str(e),
                )
            captured.append(result)

        with override_attr(
            quality_commands, "get_quality_tools", lambda: FailingTools()
        ):
            with override_attr(
                quality_commands,
                "run_tool_command",
                _capture_run_tool_command,
            ):
                quality_commands.quality_format()

        assert captured and captured[0].success is False
        assert captured[0].operation_id.category == "quality"
        assert captured[0].operation_id.command == "generic-error"
        assert "bad format" in (captured[0].stderr or "")


class TestQualityLintCheckAndDeadcode:
    def test_quality_lint_check_handles_generic_exception(self) -> None:
        captured: list[ToolResult] = []

        class FailingTools:
            def lint_check(self, *args: object, **kwargs: object) -> ToolResult:  # noqa: ANN401
                raise ToolExecutionError("boom", reason="fail", rationale="test")

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
                    category="quality",
                    command="generic-error",
                    stderr=str(e),
                )
            captured.append(result)

        with override_attr(
            quality_commands, "get_quality_tools", lambda: FailingTools()
        ):
            with override_attr(
                quality_commands,
                "run_tool_command",
                _capture_run_tool_command,
            ):
                quality_commands.quality_lint_check()

        assert captured and captured[0].success is False
        assert captured[0].operation_id.category == "quality"
        assert captured[0].operation_id.command == "generic-error"
        assert "boom" in (captured[0].stderr or "")

    def test_quality_deadcode_delegates(self) -> None:
        captured: list[ToolResult] = []

        class StubQualityTools:
            def deadcode(
                self,
                args: List[str],
                *,
                learning_mode: bool,
                verbosity_level: int,
            ) -> ToolResult:
                self.args = args
                self.learning_mode = learning_mode
                self.verbosity_level = verbosity_level
                return _tool_result("deadcode", stdout="dead ok")

        stub = StubQualityTools()

        def _capture_run_tool_command(
            command_func: Callable[..., ToolResult], *args: Any, **kwargs: Any
        ) -> None:
            result = command_func(*args, **kwargs)
            captured.append(result)

        with override_attr(quality_commands, "get_quality_tools", lambda: stub):
            with override_attr(
                quality_commands,
                "run_tool_command",
                _capture_run_tool_command,
            ):
                quality_commands.quality_deadcode(args=["--min-confidence", "80"])

        assert stub.args == ["--min-confidence", "80"]
        assert captured and captured[0].stdout == "dead ok"


class TestQualityTypecheckers:
    def test_typecheck_command_handles_errors(self) -> None:
        captured: list[ToolResult] = []

        class FailingTools:
            def typecheck(self, *args: object, **kwargs: object) -> ToolResult:  # noqa: ANN401
                raise ToolExecutionError("typecheck failed", reason="a", rationale="b")

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
                    category="quality",
                    command="generic-error",
                    stderr=str(e),
                )
            captured.append(result)

        with override_attr(
            quality_commands, "get_quality_tools", lambda: FailingTools()
        ):
            with override_attr(
                quality_commands,
                "run_tool_command",
                _capture_run_tool_command,
            ):
                quality_commands.quality_typecheck(None)

        assert captured and captured[0].success is False
        assert captured[0].operation_id.category == "quality"
        assert captured[0].operation_id.command == "generic-error"
        assert "typecheck failed" in (captured[0].stderr or "")
