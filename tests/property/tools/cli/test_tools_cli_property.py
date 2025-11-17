from __future__ import annotations

from contextlib import ExitStack, contextmanager
from pathlib import Path
from typing import Iterator, Sequence, cast

import hypothesis.strategies as st
from hypothesis import example, given, settings
import pytest
import ml_playground.tools.core.runtime as tools_runtime
from ml_playground.tools.core.config import ToolsConfig, load_tools_config
from ml_playground.tools.core.errors import ToolConfigurationError
from click import Command
from click.testing import CliRunner, Result
from typer.main import get_command

import ml_playground.tools.cli.main as tools_cli
from tests.property.tools._helpers import override_tools_with_deterministic_runner

_MISSING = object()


def _stack_override_attr(
    stack: ExitStack, obj: object, name: str, value: object
) -> None:
    original = getattr(obj, name, _MISSING)
    setattr(obj, name, value)

    def _restore() -> None:
        if original is _MISSING:
            delattr(obj, name)
        else:
            setattr(obj, name, original)

    stack.callback(_restore)


CLI_RUNNER = CliRunner()
PROJECT_ROOT = Path(__file__).resolve().parents[4]


def _load_preconfigured_tools_config() -> ToolsConfig:
    try:
        return load_tools_config(PROJECT_ROOT)
    except ToolConfigurationError:
        return ToolsConfig()


PRELOADED_CONFIG: ToolsConfig = _load_preconfigured_tools_config()
CLICK_APP = cast(Command, get_command(tools_cli.app))


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
        result = CLI_RUNNER.invoke(CLICK_APP, list(raw_args))
        _reset_cli_state()
    return result


def _build_flags(
    learning_flag: str | None,
    verbosity: int | None,
    dry_run: bool,
    include_project_root: bool,
) -> list[str]:
    flags: list[str] = []
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
def test_cli_without_subcommand_shows_guidance(flags: list[str]) -> None:
    result = _invoke_cli(flags)
    assert result.exit_code == 2


@given(flags=GLOBAL_FLAGS_STRATEGY, group_path=GROUP_PREFIX_STRATEGY)
@example(flags=[], group_path=("quality",))
@example(flags=["--dry-run"], group_path=("test", "mutation"))
@settings(max_examples=50, deadline=None, derandomize=True)
def test_cli_groups_render_help(flags: list[str], group_path: tuple[str, ...]) -> None:
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
@settings(max_examples=50, deadline=None, derandomize=True)
def test_cli_reports_unknown_commands(
    flags: list[str], group_path: tuple[str, ...], invalid: str
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
@settings(max_examples=50, deadline=None, derandomize=True)
def test_version_command_reports_metadata(flags: list[str]) -> None:
    args = [*flags, "version"]
    result = _invoke_cli(args)
    assert result.exit_code == 0
    assert "ML Playground Tools v" in result.stdout


@given(flags=GLOBAL_FLAGS_STRATEGY)
@example(flags=[])
@example(flags=["--project-root", str(PROJECT_ROOT)])
@settings(max_examples=50, deadline=None, derandomize=True)
def test_config_command_shows_categories(flags: list[str]) -> None:
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
def test_invalid_verbosity_is_rejected(flags: list[str], invalid_value: int) -> None:
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
    with override_tools_with_deterministic_runner():
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
    command: tuple[str, ...], flags: list[str]
) -> None:
    result = _invoke_with_deterministic_runner(command, global_flags=flags)
    assert result.exit_code == 0


SINGLE_VALUE_COMMANDS: dict[tuple[str, ...], list[str]] = {
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


@given(st.just(None))
@settings(max_examples=1, deadline=None, derandomize=True)
def test_main_entry_invokes_cli_app(_: None) -> None:
    called: list[str] = []

    def fake_app() -> None:
        called.append("run")

    with ExitStack() as stack:
        _stack_override_attr(stack, tools_cli, "app", fake_app)

        tools_cli.main_entry()

    assert called == ["run"]


@given(st.just(None))
@settings(max_examples=1, deadline=None, derandomize=True)
def test_main_entry_handles_keyboard_interrupt(_: None) -> None:
    def fake_app() -> None:
        raise KeyboardInterrupt

    echoed: list[tuple[str, bool]] = []

    def fake_echo(message: str, *, err: bool = False) -> None:
        echoed.append((message, err))

    with ExitStack() as stack:
        _stack_override_attr(stack, tools_cli, "app", fake_app)
        _stack_override_attr(stack, tools_cli.typer, "echo", fake_echo)

        with pytest.raises(SystemExit) as exc:
            tools_cli.main_entry()

    assert exc.value.code == 1
    assert echoed and echoed[-1][1] is True
    assert "Operation cancelled" in echoed[-1][0]


@given(reason=st.text(min_size=1, max_size=25))
@settings(max_examples=10, deadline=None, derandomize=True)
def test_main_entry_handles_generic_exception(reason: str) -> None:
    class Boom(Exception):
        pass

    def fake_app() -> None:
        raise Boom(reason)

    echoed: list[str] = []

    def fake_echo(message: str, *, err: bool = False) -> None:
        if err:
            echoed.append(message)

    with ExitStack() as stack:
        _stack_override_attr(stack, tools_cli, "app", fake_app)
        _stack_override_attr(stack, tools_cli.typer, "echo", fake_echo)

        with pytest.raises(SystemExit) as exc:
            tools_cli.main_entry()

    assert exc.value.code == 1
    assert any(reason in msg for msg in echoed)
