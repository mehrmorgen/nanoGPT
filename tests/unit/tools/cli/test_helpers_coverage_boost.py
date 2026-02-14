from __future__ import annotations
from pathlib import Path
from ml_playground.tools.cli import helpers as cli_helpers
from ml_playground.tools.cli.state import state, reset_state
from ml_playground.tools.core.config import ToolsConfig
from ml_playground.tools.core.interfaces import ToolResult


def test_get_agentic_tools_success(tmp_path: Path) -> None:
    reset_state()
    state.config = ToolsConfig()
    state.project_root = tmp_path
    from ml_playground.tools.agentic.agentic import AgenticTools

    tools = cli_helpers.get_agentic_tools()
    assert isinstance(tools, AgenticTools)


def test_get_dev_tools_success(tmp_path: Path) -> None:
    reset_state()
    state.config = ToolsConfig()
    state.project_root = tmp_path
    from ml_playground.tools.dev.dev import DevTools

    tools = cli_helpers.get_dev_tools()
    assert isinstance(tools, DevTools)


def test_mutation_run_exception_catch_all(tmp_path: Path) -> None:
    from ml_playground.tools.testing.testing import TestingTools
    from ml_playground.tools.utils.subprocess_utils import SubprocessRunner

    config = ToolsConfig()

    class StubRunner(SubprocessRunner):
        def run_uv_command(self, *args, **kwargs):
            return ToolResult.create(
                success=True,
                exit_code=0,
                namespace="tools",
                category="test",
                command="noop",
            )

        def run_pytest_command(self, *args, **kwargs):
            return ToolResult.create(
                success=True,
                exit_code=0,
                namespace="tools",
                category="test",
                command="noop",
            )

        def run_subprocess(self, *args, **kwargs):
            return ToolResult.create(
                success=True,
                exit_code=0,
                namespace="tools",
                category="test",
                command="noop",
            )

    runner = StubRunner()
    # We want mutation_run to fail at some step with an unexpected exception
    # mutation_run calls mutation_init, mutation_exec, mutation_report

    tools = TestingTools(config, tmp_path, subprocess_runner=runner)

    def raise_error(*args, **kwargs):
        raise RuntimeError("Unexpected failure")

    # Patch one of the steps to raise
    tools.mutation_init = raise_error

    result = tools.mutation_run([])

    assert result.success is False
    assert result.exit_code == 1
    assert "Mutation init failed: Unexpected failure" in result.stderr
