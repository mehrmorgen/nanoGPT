from __future__ import annotations

from contextlib import contextmanager
from typing import Any, Iterator, List

import ml_playground.tools.cli.commands.ci as ci_commands
from ml_playground.tools.core.errors import ToolConfigurationError, ToolExecutionError
from ml_playground.tools.core.interfaces import ToolResult


@contextmanager
def override_attr(obj: object, name: str, value: Any) -> Iterator[None]:
    original = getattr(obj, name)
    setattr(obj, name, value)
    try:
        yield
    finally:
        setattr(obj, name, original)


def _tool_result(command: str, *, stdout: str = "ok") -> ToolResult:
    return ToolResult.create(
        success=True,
        exit_code=0,
        namespace="tools",
        category="ci",
        command=command,
        stdout=stdout,
    )


class TestQualityGate:
    def test_quality_gate_delegates_to_ci_tools(self) -> None:
        captured: list[ToolResult] = []

        class StubCiTools:
            def __init__(self) -> None:
                self.args: list[str] = []

            def quality_gate(self, args: List[str]) -> ToolResult:
                self.args = args
                return _tool_result("quality-gate", stdout="gate ok")

        stub = StubCiTools()
        with override_attr(ci_commands, "get_ci_tools", lambda: stub):
            with override_attr(ci_commands, "handle_tool_result", captured.append):
                ci_commands.ci_quality_gate(args=["--all"])

        assert stub.args == ["--all"]
        assert captured and captured[0].stdout == "gate ok"

    def test_quality_gate_handles_tool_execution_error(self) -> None:
        captured: list[ToolResult] = []

        class FailingTools:
            def quality_gate(self, *_: object) -> ToolResult:
                raise ToolExecutionError("gate broke", reason="r", rationale="z")

        with override_attr(ci_commands, "get_ci_tools", lambda: FailingTools()):
            with override_attr(ci_commands, "handle_tool_result", captured.append):
                ci_commands.ci_quality_gate()

        assert captured and captured[0].success is False
        assert "gate broke" in (captured[0].stderr or "")


class TestQualityFastAndExt:
    def test_quality_fast_handles_configuration_error(self) -> None:
        captured: list[ToolResult] = []

        class FailingTools:
            def quality_fast(self, *_: object) -> ToolResult:
                raise ToolConfigurationError("fast broke", reason="a", rationale="b")

        with override_attr(ci_commands, "get_ci_tools", lambda: FailingTools()):
            with override_attr(ci_commands, "handle_tool_result", captured.append):
                ci_commands.ci_quality_fast(args=None)

        assert captured and captured[0].success is False
        assert "Error running quality fast" in (captured[0].stderr or "")

    def test_quality_ext_delegates(self) -> None:
        captured: list[ToolResult] = []

        class StubCiTools:
            def quality_ext(self, args: List[str]) -> ToolResult:
                self.args = args
                return _tool_result("quality-ext", stdout="ext ok")

        stub = StubCiTools()
        with override_attr(ci_commands, "get_ci_tools", lambda: stub):
            with override_attr(ci_commands, "handle_tool_result", captured.append):
                ci_commands.ci_quality_ext(args=["--verbose"])

        assert stub.args == ["--verbose"]
        assert captured and captured[0].stdout == "ext ok"


class TestQualityCiLocal:
    def test_quality_ci_local_passes_flags(self) -> None:
        captured: list[ToolResult] = []

        class StubCiTools:
            def quality_ci_local(self, *, bind_caches: bool, args: List[str]) -> ToolResult:
                self.bind_caches = bind_caches
                self.args = args
                return _tool_result("quality-ci-local", stdout="ci local ok")

        stub = StubCiTools()
        with override_attr(ci_commands, "get_ci_tools", lambda: stub):
            with override_attr(ci_commands, "handle_tool_result", captured.append):
                ci_commands.ci_quality_ci_local(bind_caches=False, args=["--jobs", "2"])

        assert stub.bind_caches is False
        assert stub.args == ["--jobs", "2"]
        assert captured and captured[0].stdout == "ci local ok"

    def test_quality_ci_local_handles_errors(self) -> None:
        captured: list[ToolResult] = []

        class FailingTools:
            def quality_ci_local(self, *args: object, **kwargs: object) -> ToolResult:
                raise ToolExecutionError("ci local fail", reason="r", rationale="z")

        with override_attr(ci_commands, "get_ci_tools", lambda: FailingTools()):
            with override_attr(ci_commands, "handle_tool_result", captured.append):
                ci_commands.ci_quality_ci_local()

        assert captured and captured[0].success is False
        assert "Error running quality ci local" in (captured[0].stderr or "")


class TestCoverageBadge:
    def test_coverage_badge_handles_errors(self) -> None:
        captured: list[ToolResult] = []

        class FailingTools:
            def coverage_badge(self, *_: object) -> ToolResult:
                raise ToolExecutionError("badge fail", reason="r", rationale="z")

        with override_attr(ci_commands, "get_ci_tools", lambda: FailingTools()):
            with override_attr(ci_commands, "handle_tool_result", captured.append):
                ci_commands.ci_coverage_badge()

        assert captured and captured[0].success is False
        assert "Error generating coverage badge" in (captured[0].stderr or "")
