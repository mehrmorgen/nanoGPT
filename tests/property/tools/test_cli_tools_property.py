from __future__ import annotations

from contextlib import contextmanager
from datetime import timedelta
from importlib import import_module
from pathlib import Path
from typing import Iterator, List, Sequence

import hypothesis.strategies as st
from hypothesis import example, given, settings
from ml_playground.tools.core import runtime as tools_runtime
from ml_playground.tools.core.config import ToolsConfig, load_tools_config
from ml_playground.tools.core.interfaces import OperationId, ToolResult
from ml_playground.tools.utils import subprocess_utils
from typer.testing import CliRunner, Result

import ml_playground.tools.cli as tools_cli

# Modules whose `_default_runner` attribute should mirror the deterministic runner during tests
TARGET_RUNNER_MODULES = [
    # Shims that re-export classes and expose _default_runner
    "ml_playground.tools.quality",
    "ml_playground.tools.testing",
    "ml_playground.tools.dev",
    # Implementation modules where _default_runner is bound at import time
    "ml_playground.tools.quality.quality",
    "ml_playground.tools.testing.testing",
    "ml_playground.tools.dev.dev",
]

CLI_RUNNER = CliRunner()
PROJECT_ROOT = Path(__file__).resolve().parents[3]
PRELOADED_CONFIG: ToolsConfig = load_tools_config(PROJECT_ROOT)


def _load_tools_config_stub(_project_root: Path | None = None) -> ToolsConfig:
    return PRELOADED_CONFIG


@contextmanager
def _stubbed_tools_config() -> Iterator[None]:
    original_loader = tools_runtime.load_tools_config
    try:
        tools_runtime.load_tools_config = _load_tools_config_stub
        yield
    finally:
        tools_runtime.load_tools_config = original_loader


def _collect_command_paths(
    app: tools_cli.typer.Typer,
) -> tuple[list[tuple[str, ...]], list[tuple[str, ...]]]:
    """Return (group_paths, command_paths) for the provided Typer app."""

    group_paths: list[tuple[str, ...]] = []
    command_paths: list[tuple[str, ...]] = []

    def _walk(prefix: tuple[str, ...], typer_app: tools_cli.typer.Typer) -> None:
        for command_info in getattr(typer_app, "registered_commands", ()):  # type: ignore[attr-defined]
            name = command_info.name
            if name:
                command_paths.append((*prefix, name))

        for group_info in getattr(typer_app, "registered_groups", ()):  # type: ignore[attr-defined]
            name = group_info.name
            if not name:
                continue

            path = (*prefix, name)
            group_paths.append(path)
            _walk(path, group_info.typer_instance)

    _walk((), app)
    return group_paths, command_paths


GROUP_PATHS, COMMAND_PATHS = _collect_command_paths(tools_cli.app)
ALL_VALID_TOKENS = {token for path in (*GROUP_PATHS, *COMMAND_PATHS) for token in path}
GROUP_PREFIXES: list[tuple[str, ...]] = [()] + GROUP_PATHS

INVALID_TOKEN_POOL = [f"invalid-token-{index}" for index in range(200)]
INVALID_TOKENS = [
    token for token in INVALID_TOKEN_POOL if token not in ALL_VALID_TOKENS
]


def _reset_cli_state() -> None:
    tools_runtime.reset_state()
    tools_runtime.set_config(PRELOADED_CONFIG, PROJECT_ROOT)


def _invoke_cli(raw_args: Sequence[str]) -> Result:
    with _stubbed_tools_config():
        _reset_cli_state()
        result = CLI_RUNNER.invoke(tools_cli.app, list(raw_args))
        _reset_cli_state()
    return result


# --- Deterministic subprocess runner for CLI command execution tests ---


class DeterministicRunner(subprocess_utils.SubprocessRunner):
    """Fake runner returning canned results for tools category commands."""

    def __init__(self) -> None:
        self.calls: list[dict[str, object]] = []

    def run_subprocess(self, *args, **kwargs) -> ToolResult:  # type: ignore[override]
        return self._create_success(kwargs.get("operation_id"))

    def run_uv_command(  # type: ignore[override]
        self,
        args: List[str],
        *,
        cwd: Path | None = None,
        env: dict[str, str] | None = None,
        timeout: int | None = None,
        operation_id: OperationId,
        python: str | None = None,
        no_project: bool = False,
    ) -> ToolResult:
        self.calls.append({"args": args, "operation_id": operation_id})
        return self._create_success(operation_id, stdout="executed")

    def run_pytest_command(self, *args, **kwargs) -> ToolResult:  # type: ignore[override]
        return self._create_success(kwargs.get("operation_id"), stdout="pytest passed")

    @staticmethod
    def _create_success(
        operation_id: OperationId | None, stdout: str = ""
    ) -> ToolResult:
        return ToolResult(
            success=True,
            exit_code=0,
            stderr="",
            operation_id=operation_id
            or OperationId(namespace="tools", category="cli", command="deterministic"),
        )


@contextmanager
def _install_deterministic_runner() -> Iterator[DeterministicRunner]:
    """Install a deterministic subprocess runner for the duration of the context."""
    original_runner = subprocess_utils._default_runner
    deterministic = DeterministicRunner()

    modules = [import_module(name) for name in TARGET_RUNNER_MODULES]
    original_module_runners = {
        module: getattr(module, "_default_runner", None) for module in modules
    }

    subprocess_utils._default_runner = deterministic
    for module in modules:
        if hasattr(module, "_default_runner"):
            setattr(module, "_default_runner", deterministic)

    try:
        yield deterministic
    finally:
        subprocess_utils._default_runner = original_runner
        for module, runner in original_module_runners.items():
            if runner is not None:
                setattr(module, "_default_runner", runner)


def _build_flags(
    learning_flag: str | None,
    verbosity: int | None,
    dry_run: bool,
    include_project_root: bool,
) -> List[str]:
    flags: List[str] = []
    if learning_flag:
        flags.append(learning_flag)
    if verbosity is not None:
        flags.extend(["--verbosity", str(verbosity)])
    if dry_run:
        flags.append("--dry-run")
    if include_project_root:
        flags.extend(["--project-root", str(PROJECT_ROOT)])
    return flags


GLOBAL_FLAGS_STRATEGY = st.builds(
    _build_flags,
    learning_flag=st.none()
    | st.sampled_from(["--learning-mode", "--no-learning-mode"]),
    verbosity=st.none() | st.integers(min_value=0, max_value=2),
    dry_run=st.booleans(),
    include_project_root=st.booleans(),
)

GROUP_PATH_STRATEGY = st.sampled_from(GROUP_PATHS) if GROUP_PATHS else st.just(())
GROUP_PREFIX_STRATEGY = st.sampled_from(GROUP_PREFIXES)
INVALID_TOKEN_STRATEGY = (
    st.sampled_from(INVALID_TOKENS) if INVALID_TOKENS else st.just("invalid-token")
)


@given(flags=GLOBAL_FLAGS_STRATEGY)
@example(flags=[])
@settings(max_examples=50, deadline=None, derandomize=True)
def test_cli_without_subcommand_shows_guidance(flags: List[str]) -> None:
    result = _invoke_cli(flags)
    assert result.exit_code == 2


@given(flags=GLOBAL_FLAGS_STRATEGY, group_path=GROUP_PREFIX_STRATEGY)
@example(flags=[], group_path=("quality",))
@example(flags=["--dry-run"], group_path=("test", "mutation"))
@settings(max_examples=50, deadline=None, derandomize=True)
def test_cli_groups_render_help(flags: List[str], group_path: tuple[str, ...]) -> None:
    args = [*flags, *group_path]
    result = _invoke_cli(args)
    assert result.exit_code == 2
    output = result.stdout or result.stderr
    assert "Usage:" in output
    if group_path:
        assert group_path[-1] in output


@given(
    flags=GLOBAL_FLAGS_STRATEGY,
    group_path=GROUP_PREFIX_STRATEGY,
    invalid=INVALID_TOKEN_STRATEGY,
)
@example(flags=[], group_path=("quality",), invalid="totally-unknown")
@example(flags=["--no-learning-mode"], group_path=("env",), invalid="bogus")
@settings(max_examples=50, deadline=timedelta(milliseconds=200), derandomize=True)
def test_cli_reports_unknown_commands(
    flags: List[str], group_path: tuple[str, ...], invalid: str
) -> None:
    args = [*flags, *group_path, invalid]
    result = _invoke_cli(args)
    assert result.exit_code != 0
    error_stream = result.stderr or result.stdout
    lowered = error_stream.lower()
    assert "no such command" in lowered or "unknown command" in lowered


@given(flags=GLOBAL_FLAGS_STRATEGY)
@example(flags=[])
@example(flags=["--learning-mode", "--verbosity", "2"])
@settings(max_examples=50, deadline=timedelta(milliseconds=120), derandomize=True)
def test_version_command_reports_metadata(flags: List[str]) -> None:
    args = [*flags, "version"]
    result = _invoke_cli(args)
    assert result.exit_code == 0
    assert "ML Playground Tools v" in result.stdout


@given(flags=GLOBAL_FLAGS_STRATEGY)
@example(flags=[])
@example(flags=["--project-root", str(PROJECT_ROOT)])
@settings(max_examples=50, deadline=timedelta(milliseconds=120), derandomize=True)
def test_config_command_shows_categories(flags: List[str]) -> None:
    args = [*flags, "config"]
    result = _invoke_cli(args)
    assert result.exit_code == 0
    assert "Current tools configuration:" in result.stdout
    assert "Quality tools:" in result.stdout


@given(
    flags=st.builds(
        _build_flags,
        learning_flag=st.none(),
        verbosity=st.none(),
        dry_run=st.booleans(),
        include_project_root=st.booleans(),
    ),
    invalid_value=st.integers(min_value=-5, max_value=5).filter(
        lambda value: value < 0 or value > 2
    ),
)
@example(flags=[], invalid_value=3)
@settings(max_examples=50, deadline=None, derandomize=True)
def test_invalid_verbosity_is_rejected(flags: List[str], invalid_value: int) -> None:
    args = [*flags, "--verbosity", str(invalid_value)]
    result = _invoke_cli(args)
    assert result.exit_code != 0
    error_stream = result.stderr or result.stdout
    assert "Invalid value for '--verbosity'" in error_stream or "Usage:" in error_stream


# --- Command execution coverage ------------------------------------------------


EXECUTION_COMPATIBLE_COMMANDS: list[tuple[str, ...]] = []
SKIPPED_COMMANDS: list[tuple[str, ...]] = []

ALLOWED_ROOTS = {"quality"}
ALLOWED_TOP_LEVEL = {("version",), ("config",)}
ALLOWED_TEST_SUBCOMMANDS = {("test", "unit")}

for command in COMMAND_PATHS:
    if tuple(command) in ALLOWED_TOP_LEVEL:
        EXECUTION_COMPATIBLE_COMMANDS.append(command)
        continue

    root = command[0] if command else ""
    if root in ALLOWED_ROOTS:
        EXECUTION_COMPATIBLE_COMMANDS.append(command)
        continue

    if tuple(command[:2]) in ALLOWED_TEST_SUBCOMMANDS:
        EXECUTION_COMPATIBLE_COMMANDS.append(command)
        continue

    SKIPPED_COMMANDS.append(command)

COMMAND_EXECUTION_STRATEGY = (
    st.sampled_from(EXECUTION_COMPATIBLE_COMMANDS)
    if EXECUTION_COMPATIBLE_COMMANDS
    else st.just(("quality", "lint"))
)


def _invoke_with_deterministic_runner(
    command: tuple[str, ...],
    *,
    global_flags: list[str] | None = None,
    extra: list[str] | None = None,
) -> Result:
    with _install_deterministic_runner():
        args: list[str] = []
        if global_flags:
            args.extend(global_flags)
        args.extend(command)
        if extra:
            args.extend(extra)
        return _invoke_cli(args)


@given(command=COMMAND_EXECUTION_STRATEGY, flags=GLOBAL_FLAGS_STRATEGY)
@example(command=("quality", "lint"), flags=[])
@example(command=("test", "unit"), flags=["--no-learning-mode"])
@settings(max_examples=40, deadline=None, derandomize=True)
def test_commands_execute_with_deterministic_runner(
    command: tuple[str, ...], flags: List[str]
) -> None:
    result = _invoke_with_deterministic_runner(command, global_flags=flags)
    assert result.exit_code == 0


SINGLE_VALUE_COMMANDS = {
    ("quality", "lint"): ["--", "--format"],
    ("quality", "deadcode"): ["--", "--min-confidence", "80"],
    ("test", "unit"): ["--", "-k", "sample"],
}


@given(command=st.sampled_from(list(SINGLE_VALUE_COMMANDS.keys())))
@settings(max_examples=20, deadline=None, derandomize=True)
def test_commands_accept_sample_arguments(command: tuple[str, ...]) -> None:
    extra = SINGLE_VALUE_COMMANDS[command]
    result = _invoke_with_deterministic_runner(command, extra=extra)
    assert result.exit_code == 0


def test_skipped_commands_documented() -> None:
    # Ensure skipped commands are outside the explicitly allowed set.
    allowed = set(EXECUTION_COMPATIBLE_COMMANDS)
    assert all(tuple(path) not in allowed for path in SKIPPED_COMMANDS)
