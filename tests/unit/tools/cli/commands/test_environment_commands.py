from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator, List

import pytest
import typer

import ml_playground.tools.cli.commands.environment as env_commands
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
        category="env",
        command=command,
        stdout=stdout,
    )


class TestEnvSetupCommand:
    def test_env_setup_delegates_to_tools(self) -> None:
        captured: list[ToolResult] = []

        class StubEnvTools:
            def setup(self, args: List[str], clear: bool = False) -> ToolResult:
                self.args = args
                self.clear = clear
                return _tool_result("setup", stdout="done")

        stub = StubEnvTools()
        with override_attr(env_commands, "get_environment_tools", lambda: stub):
            with override_attr(env_commands, "handle_tool_result", captured.append):
                env_commands.env_setup(clear=True, args=["--verbose"])

        assert stub.args == ["--verbose"]
        assert stub.clear is True
        assert captured and captured[0].stdout == "done"

    def test_env_setup_handles_errors(self) -> None:
        captured: list[ToolResult] = []

        class FailingEnvTools:
            def setup(self, args: List[str], clear: bool = False) -> ToolResult:
                raise ToolExecutionError("setup failed", reason="x", rationale="y")

        with override_attr(
            env_commands, "get_environment_tools", lambda: FailingEnvTools()
        ):
            with override_attr(env_commands, "handle_tool_result", captured.append):
                env_commands.env_setup(args=[])

        assert captured and captured[0].success is False
        assert "Error setting up environment" in (captured[0].stderr or "")


class TestEnvSyncCommand:
    def test_env_sync_passes_flags(self) -> None:
        captured: list[ToolResult] = []

        class StubEnvTools:
            def sync(
                self,
                args: List[str],
                *,
                groups: List[str] | None,
                all_groups: bool,
                frozen: bool,
            ) -> ToolResult:
                self.args = args
                self.groups = groups
                self.all_groups = all_groups
                self.frozen = frozen
                return _tool_result("sync")

        stub = StubEnvTools()
        with override_attr(env_commands, "get_environment_tools", lambda: stub):
            with override_attr(env_commands, "handle_tool_result", captured.append):
                env_commands.env_sync(
                    groups=["dev"], all_groups=False, frozen=True, args=["--upgrade"]
                )

        assert stub.args == ["--upgrade"]
        assert stub.groups == ["dev"]
        assert stub.all_groups is False
        assert stub.frozen is True
        assert captured


class TestEnvVerifyAndInfoCommands:
    def test_env_verify_delegates(self) -> None:
        captured: list[ToolResult] = []

        class StubEnvTools:
            def verify(self, args: List[str]) -> ToolResult:
                self.args = args
                return _tool_result("verify", stdout="verified")

        stub = StubEnvTools()
        with override_attr(env_commands, "get_environment_tools", lambda: stub):
            with override_attr(env_commands, "handle_tool_result", captured.append):
                env_commands.env_verify(args=["--quiet"])

        assert stub.args == ["--quiet"]
        assert captured and captured[0].stdout == "verified"

    def test_env_info_delegates(self) -> None:
        captured: list[ToolResult] = []

        class StubEnvTools:
            def info(self, args: List[str]) -> ToolResult:
                self.args = args
                return _tool_result("info", stdout="info")

        stub = StubEnvTools()
        with override_attr(env_commands, "get_environment_tools", lambda: stub):
            with override_attr(env_commands, "handle_tool_result", captured.append):
                env_commands.env_info(args=[])

        assert stub.args == []
        assert captured and captured[0].stdout == "info"


class TestEnvCleanCommand:
    def test_env_clean_handles_configuration_error(self) -> None:
        captured: list[ToolResult] = []

        class FailingEnvTools:
            def clean(self, args: List[str]) -> ToolResult:
                raise ToolConfigurationError("bad config", reason="r", rationale="z")

        with override_attr(
            env_commands, "get_environment_tools", lambda: FailingEnvTools()
        ):
            with override_attr(env_commands, "handle_tool_result", captured.append):
                env_commands.env_clean()

        assert captured and captured[0].success is False
        assert "Error cleaning environment" in (captured[0].stderr or "")


class TestEnvAiGuidelinesCommand:
    def test_env_ai_guidelines_passes_args(self) -> None:
        captured: list[ToolResult] = []

        class StubEnvTools:
            def ai_guidelines(
                self, args: List[str], *, tool: str, dry_run: bool
            ) -> ToolResult:
                self.args = args
                self.tool = tool
                self.dry_run = dry_run
                return _tool_result("ai-guidelines")

        stub = StubEnvTools()
        with override_attr(env_commands, "get_environment_tools", lambda: stub):
            with override_attr(env_commands, "handle_tool_result", captured.append):
                env_commands.env_ai_guidelines(
                    "cursor", dry_run=True, args=["--verbose"]
                )

        assert stub.args == ["--verbose"]
        assert stub.tool == "cursor"
        assert stub.dry_run is True
        assert captured


class TestEnvTensorboardCommand:
    def test_env_tensorboard_forwards_options(self, tmp_path: Path) -> None:
        captured: list[ToolResult] = []

        class StubEnvTools:
            def tensorboard(
                self,
                args: List[str],
                *,
                logdir: Path,
                port: int,
                host: str,
            ) -> ToolResult:
                self.args = args
                self.logdir = logdir
                self.port = port
                self.host = host
                return _tool_result("tensorboard")

        stub = StubEnvTools()
        with override_attr(env_commands, "get_environment_tools", lambda: stub):
            with override_attr(env_commands, "handle_tool_result", captured.append):
                env_commands.env_tensorboard(
                    logdir=tmp_path,
                    port=7777,
                    host="0.0.0.0",
                    args=["--reload_interval=1"],
                )

        assert stub.args == ["--reload_interval=1"]
        assert stub.logdir == tmp_path
        assert stub.port == 7777
        assert stub.host == "0.0.0.0"
        assert captured

    def test_env_tensorboard_handles_tool_error(self) -> None:
        class FailingEnvTools:
            def tensorboard(
                self, args: List[str], *, logdir: Path, port: int, host: str
            ) -> ToolResult:
                raise ToolExecutionError(
                    "tensorboard failed", reason="x", rationale="y"
                )

        with override_attr(
            env_commands, "get_environment_tools", lambda: FailingEnvTools()
        ):
            with pytest.raises(typer.Exit):
                env_commands.env_tensorboard(logdir=Path("logs"))
