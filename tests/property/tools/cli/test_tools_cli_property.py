from __future__ import annotations

from contextlib import ExitStack, contextmanager
from pathlib import Path
from typing import Iterator, Sequence

import hypothesis.strategies as st
from hypothesis import assume, example, given, settings
import typer
from ml_playground.tools.core.config import ToolsConfig, load_tools_config
from ml_playground.tools.core.errors import ToolConfigurationError
from click.testing import CliRunner, Result
from typer.main import get_command

import ml_playground.tools.cli.main as tools_cli
from ml_playground.tools.cli.dependencies import (
    ToolsDependencies,
    default_tools_dependencies,
    override_tools_dependencies,
)
from ml_playground.tools.cli.state import state as cli_state
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


def _output_text(result: Result) -> str:
    return (result.stdout or "") + (result.stderr or "") + (result.output or "")


def _assert_traceback_free(text: str) -> None:
    assert "traceback" not in text.lower()


def _assert_tools_cli_error(result: Result, *needles: str) -> None:
    text = _output_text(result)
    _assert_traceback_free(text)
    lowered = text.lower()
    assert any(needle.lower() in lowered for needle in needles) or "usage:" in lowered


def _load_preconfigured_tools_config() -> ToolsConfig:
    try:
        return load_tools_config(PROJECT_ROOT)
    except ToolConfigurationError:
        return ToolsConfig()


PRELOADED_CONFIG: ToolsConfig = _load_preconfigured_tools_config()
CLICK_APP = get_command(tools_cli.app)


def _load_tools_config_stub(_project_root: Path | None = None) -> ToolsConfig:
    return PRELOADED_CONFIG


@contextmanager
def _stubbed_tools_dependencies() -> Iterator[None]:
    base_deps = default_tools_dependencies()
    overridden = ToolsDependencies(
        load_config=_load_tools_config_stub,
        quality_factory=base_deps.quality_factory,
        testing_factory=base_deps.testing_factory,
        environment_factory=base_deps.environment_factory,
        ci_factory=base_deps.ci_factory,
        dev_factory=base_deps.dev_factory,
        result_handler=base_deps.result_handler,
    )
    with override_tools_dependencies(overridden):
        yield


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

COMMAND_INFO = tools_cli.get_command_info()
CATEGORIES = sorted(COMMAND_INFO.keys())
CATEGORY_STRATEGY = st.sampled_from(CATEGORIES) if CATEGORIES else st.just("quality")
INVALID_CATEGORIES = [
    token for token in INVALID_TOKEN_POOL if token not in COMMAND_INFO
] or ["unknown-category"]
INVALID_CATEGORY_STRATEGY = st.sampled_from(INVALID_CATEGORIES)
CATEGORY_COMMAND_MAP = {
    category: sorted(info["commands"].keys()) for category, info in COMMAND_INFO.items()
}
VALID_EXPLAIN_TARGETS = [
    f"{category}.{command}"
    for category, commands in CATEGORY_COMMAND_MAP.items()
    for command in commands
]
VALID_EXPLAIN_STRATEGY = (
    st.sampled_from(VALID_EXPLAIN_TARGETS)
    if VALID_EXPLAIN_TARGETS
    else st.just("quality.lint")
)


def _reset_cli_state() -> None:
    cli_state.reset()
    cli_state.config = PRELOADED_CONFIG
    cli_state.project_root = PROJECT_ROOT


def _invoke_cli_unisolated(raw_args: Sequence[str]) -> Result:
    _reset_cli_state()
    result = CLI_RUNNER.invoke(CLICK_APP, list(raw_args))
    _reset_cli_state()
    return result


def _invoke_cli(raw_args: Sequence[str]) -> Result:
    with _stubbed_tools_dependencies():
        return _invoke_cli_unisolated(raw_args)


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


_NON_INT_TEXT = st.text(
    alphabet=st.characters(
        whitelist_categories=("Ll", "Lu"), whitelist_characters="_-"
    ),
    min_size=1,
    max_size=12,
)


_NON_BOOL_TEXT = st.text(
    alphabet=st.characters(
        whitelist_categories=("Ll", "Lu"), whitelist_characters="_-"
    ),
    min_size=1,
    max_size=12,
).filter(lambda value: value.lower() not in {"true", "false", "1", "0"})


_REL_NAME_TEXT = st.text(
    alphabet=st.characters(
        whitelist_categories=("Ll", "Lu", "Nd"), whitelist_characters="_-"
    ),
    min_size=1,
    max_size=40,
).filter(lambda value: value not in {".", ".."})


@given(flags=GLOBAL_FLAGS_STRATEGY)
@example(flags=[])
@settings(max_examples=50, deadline=None, derandomize=True)
def test_cli_without_subcommand_shows_guidance(flags: list[str]) -> None:
    result = _invoke_cli(flags)
    assert result.exit_code == 2
    output = _output_text(result)
    lowered = output.lower()
    assert "welcome to ml playground tools cli" in lowered
    assert "no tools command was provided" in lowered
    assert "try `uv run tools test`" in output or "try" in lowered
    _assert_traceback_free(output)


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


@given(flags=GLOBAL_FLAGS_STRATEGY)
@example(flags=[])
@settings(max_examples=30, deadline=None, derandomize=True)
def test_cli_help_always_succeeds(flags: list[str]) -> None:
    args = [*flags, "--help"]
    result = _invoke_cli(args)
    assert result.exit_code == 0
    output = _output_text(result)
    assert "Usage:" in output
    _assert_traceback_free(output)


@given(
    flags=GLOBAL_FLAGS_STRATEGY,
    subcommand=GROUP_PREFIX_STRATEGY,
)
@example(flags=[], subcommand=("quality",))
@example(flags=["--learning-mode"], subcommand=("analysis",))
@settings(max_examples=30, deadline=None, derandomize=True)
def test_cli_subcommand_help_always_succeeds(
    flags: list[str], subcommand: tuple[str, ...]
) -> None:
    args = [*flags, *subcommand, "--help"]
    result = _invoke_cli(args)
    assert result.exit_code == 0
    output = _output_text(result)
    assert "Usage:" in output
    _assert_traceback_free(output)


@given(
    flags=GLOBAL_FLAGS_STRATEGY,
    whitespace=st.lists(
        st.sampled_from([" ", "  ", "\t", "\n"]),
        min_size=1,
        max_size=3,
    ),
)
@example(flags=[], whitespace=[" "])
@settings(max_examples=30, deadline=None, derandomize=True)
def test_cli_whitespace_args_never_crash(
    flags: list[str], whitespace: list[str]
) -> None:
    args = [*flags, *whitespace]
    result = _invoke_cli(args)
    if result.exception is not None:
        assert isinstance(result.exception, SystemExit)
    output = _output_text(result)
    _assert_traceback_free(output)
    assert "Usage:" in output or "No such command" in output


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
    error_stream = _output_text(result)
    lowered = error_stream.lower()
    assert "no such command" in lowered or "unknown command" in lowered
    _assert_traceback_free(error_stream)


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
    _assert_tools_cli_error(result, "invalid value")


@given(
    flags=GLOBAL_FLAGS_STRATEGY, opt=st.sampled_from(["--verbosity", "--project-root"])
)
@settings(max_examples=30, deadline=None, derandomize=True)
def test_global_options_require_values(flags: list[str], opt: str) -> None:
    args = [*flags, opt]
    result = _invoke_cli(args)
    assert result.exit_code != 0
    _assert_tools_cli_error(result, "requires an argument", "missing")


@given(flags=GLOBAL_FLAGS_STRATEGY, bad_bool=_NON_BOOL_TEXT)
@settings(max_examples=30, deadline=None, derandomize=True)
def test_learning_mode_flag_rejects_value_form(flags: list[str], bad_bool: str) -> None:
    args = [*flags, f"--learning-mode={bad_bool}"]
    result = _invoke_cli(args)
    assert result.exit_code != 0
    _assert_tools_cli_error(result, "does not take a value")


@given(flags=GLOBAL_FLAGS_STRATEGY, missing_name=_REL_NAME_TEXT)
@settings(max_examples=30, deadline=None, derandomize=True)
def test_project_root_missing_path_is_rejected(
    flags: list[str],
    missing_name: str,
) -> None:
    runner = CliRunner()
    with runner.isolated_filesystem():
        missing_path = Path(".pbt-missing") / missing_name
        assume(not missing_path.exists())
        args = [*flags, "--project-root", str(missing_path), "version"]
        result = runner.invoke(CLICK_APP, args)

    assert result.exit_code != 0
    _assert_tools_cli_error(result, "project root not found")


@given(flags=GLOBAL_FLAGS_STRATEGY, filename=_REL_NAME_TEXT)
@settings(max_examples=30, deadline=None, derandomize=True)
def test_project_root_file_path_is_rejected(
    flags: list[str],
    filename: str,
) -> None:
    runner = CliRunner()
    with runner.isolated_filesystem():
        file_path = Path(f"{filename}.txt")
        file_path.write_text("x", encoding="utf-8")
        args = [*flags, "--project-root", str(file_path), "version"]
        result = runner.invoke(CLICK_APP, args)

    assert result.exit_code != 0
    _assert_tools_cli_error(result, "project root is not a directory")


@given(flags=GLOBAL_FLAGS_STRATEGY, bad_port=_NON_INT_TEXT)
@example(flags=[], bad_port="not-a-number")
@settings(max_examples=40, deadline=None, derandomize=True)
def test_analysis_lit_rejects_non_int_port(flags: list[str], bad_port: str) -> None:
    args = [*flags, "analysis", "lit", "--port", bad_port]
    result = _invoke_cli(args)
    assert result.exit_code != 0
    _assert_tools_cli_error(result, "invalid value", "usage:")


@given(
    flags=GLOBAL_FLAGS_STRATEGY,
    opt=st.sampled_from(["--port", "--host"]),
)
@example(flags=[], opt="--port")
@example(flags=["--dry-run"], opt="--host")
@settings(max_examples=40, deadline=None, derandomize=True)
def test_analysis_lit_requires_option_values(flags: list[str], opt: str) -> None:
    args = [*flags, "analysis", "lit", opt]
    result = _invoke_cli(args)
    assert result.exit_code != 0
    _assert_tools_cli_error(result, "requires an argument", "missing", "usage:")


@given(flags=GLOBAL_FLAGS_STRATEGY, bad_port=_NON_INT_TEXT)
@example(flags=[], bad_port="not-a-number")
@settings(max_examples=40, deadline=None, derandomize=True)
def test_analysis_lit_rejects_non_int_port_equals_form(
    flags: list[str], bad_port: str
) -> None:
    args = [*flags, "analysis", "lit", f"--port={bad_port}"]
    result = _invoke_cli(args)
    assert result.exit_code != 0
    _assert_tools_cli_error(result, "invalid value", "usage:")


@given(flags=GLOBAL_FLAGS_STRATEGY, bad_bool=_NON_BOOL_TEXT)
@example(flags=[], bad_bool="maybe")
@settings(max_examples=40, deadline=None, derandomize=True)
def test_analysis_lit_rejects_invalid_open_browser_value(
    flags: list[str], bad_bool: str
) -> None:
    args = [*flags, "analysis", "lit", f"--open-browser={bad_bool}"]
    result = _invoke_cli(args)
    assert result.exit_code != 0
    _assert_tools_cli_error(result, "invalid value", "does not take a value", "usage:")


@given(flags=GLOBAL_FLAGS_STRATEGY)
@settings(max_examples=40, deadline=None, derandomize=True)
def test_analysis_sample_quality_requires_file_argument(flags: list[str]) -> None:
    args = [*flags, "analysis", "sample-quality"]
    result = _invoke_cli(args)
    assert result.exit_code != 0
    _assert_tools_cli_error(result, "missing argument")


@given(flags=GLOBAL_FLAGS_STRATEGY, missing_name=st.text(min_size=1, max_size=50))
@example(flags=[], missing_name="definitely-missing-sample.txt")
@settings(max_examples=40, deadline=None, derandomize=True)
def test_analysis_sample_quality_missing_file_has_stable_error(
    flags: list[str],
    missing_name: str,
) -> None:
    missing_path = Path(".pbt-missing") / missing_name
    assume(not missing_path.exists())

    args = [*flags, "analysis", "sample-quality", str(missing_path)]
    result = _invoke_cli(args)

    assert result.exit_code != 0
    _assert_tools_cli_error(result, "sample file not found")


@given(
    flags=GLOBAL_FLAGS_STRATEGY,
    category=CATEGORY_STRATEGY,
    detailed=st.booleans(),
)
@example(flags=[], category="quality", detailed=False)
@example(flags=["--dry-run"], category="test", detailed=True)
@settings(max_examples=30, deadline=None, derandomize=True)
def test_learn_commands_accept_valid_categories(
    flags: list[str], category: str, detailed: bool
) -> None:
    args = [*flags, "learn", "commands", "--category", category]
    if detailed:
        args.append("--detailed")
    result = _invoke_cli(args)
    assert result.exit_code == 0
    output = result.stdout
    assert category.lower() in output.lower()


@given(flags=GLOBAL_FLAGS_STRATEGY, category=INVALID_CATEGORY_STRATEGY)
@example(flags=[], category="totally-unknown")
@settings(max_examples=30, deadline=None, derandomize=True)
def test_learn_commands_reject_unknown_categories(
    flags: list[str], category: str
) -> None:
    args = [*flags, "learn", "commands", "--category", category]
    result = _invoke_cli(args)
    assert result.exit_code == 1
    _assert_tools_cli_error(result, "unknown category")


@given(flags=GLOBAL_FLAGS_STRATEGY)
@settings(max_examples=20, deadline=None, derandomize=True)
def test_learn_commands_requires_category_value(flags: list[str]) -> None:
    args = [*flags, "learn", "commands", "--category"]
    result = _invoke_cli(args)
    assert result.exit_code != 0
    _assert_tools_cli_error(result, "requires an argument", "missing")


@given(flags=GLOBAL_FLAGS_STRATEGY, detailed=st.booleans())
@example(flags=[], detailed=False)
@settings(max_examples=20, deadline=None, derandomize=True)
def test_learn_commands_overview(flags: list[str], detailed: bool) -> None:
    args = [*flags, "learn", "commands"]
    if detailed:
        args.append("--detailed")
    result = _invoke_cli(args)
    assert result.exit_code == 0
    text = result.stdout
    assert "ML Playground Tools" in text


@given(flags=GLOBAL_FLAGS_STRATEGY, category=CATEGORY_STRATEGY)
@example(flags=[], category="env")
@settings(max_examples=30, deadline=None, derandomize=True)
def test_learn_best_practices_valid_category(flags: list[str], category: str) -> None:
    args = [*flags, "learn", "best-practices", "--category", category]
    result = _invoke_cli(args)
    assert result.exit_code == 0
    output = result.stdout
    assert COMMAND_INFO[category]["name"] in output


@given(flags=GLOBAL_FLAGS_STRATEGY, category=INVALID_CATEGORY_STRATEGY)
@example(flags=["--dry-run"], category="missing")
@settings(max_examples=30, deadline=None, derandomize=True)
def test_learn_best_practices_rejects_unknown_category(
    flags: list[str], category: str
) -> None:
    args = [*flags, "learn", "best-practices", "--category", category]
    result = _invoke_cli(args)
    assert result.exit_code == 1
    _assert_tools_cli_error(result, "unknown category")


@given(flags=GLOBAL_FLAGS_STRATEGY)
@settings(max_examples=20, deadline=None, derandomize=True)
def test_learn_best_practices_requires_category_value(flags: list[str]) -> None:
    args = [*flags, "learn", "best-practices", "--category"]
    result = _invoke_cli(args)
    assert result.exit_code != 0
    _assert_tools_cli_error(result, "requires an argument", "missing")


@given(flags=GLOBAL_FLAGS_STRATEGY)
@example(flags=[])
@settings(max_examples=20, deadline=None, derandomize=True)
def test_learn_best_practices_overview(flags: list[str]) -> None:
    args = [*flags, "learn", "best-practices"]
    result = _invoke_cli(args)
    assert result.exit_code == 0
    text = result.stdout
    assert "Best Practices" in text


@given(flags=GLOBAL_FLAGS_STRATEGY, target=VALID_EXPLAIN_STRATEGY)
@example(flags=[], target="quality.lint")
@settings(max_examples=30, deadline=None, derandomize=True)
def test_learn_explain_known_command(flags: list[str], target: str) -> None:
    args = [*flags, "learn", "explain", target]
    result = _invoke_cli(args)
    assert result.exit_code == 0
    output = result.stdout
    assert target.split(".")[0] in output
    assert target.split(".")[1] in output


@given(flags=GLOBAL_FLAGS_STRATEGY, bad_value=st.text(min_size=1, max_size=10))
@example(flags=[], bad_value="invalid-format")
@settings(max_examples=30, deadline=None, derandomize=True)
def test_learn_explain_rejects_invalid_format(flags: list[str], bad_value: str) -> None:
    assume_value = bad_value if "." not in bad_value else bad_value.replace(".", "-")
    args = [*flags, "learn", "explain", assume_value]
    result = _invoke_cli(args)
    assert result.exit_code == 1
    _assert_tools_cli_error(result, "command must be in format")


@given(
    flags=GLOBAL_FLAGS_STRATEGY,
    unknown_category=st.text(
        alphabet=st.characters(
            whitelist_categories=("Ll", "Lu", "Nd"), whitelist_characters="_-"
        ),
        min_size=1,
        max_size=12,
    ).filter(lambda value: value not in COMMAND_INFO),
    unknown_command=st.text(
        alphabet=st.characters(
            whitelist_categories=("Ll", "Lu", "Nd"), whitelist_characters="_-"
        ),
        min_size=1,
        max_size=12,
    ),
)
@settings(max_examples=30, deadline=None, derandomize=True)
def test_learn_explain_rejects_unknown_target(
    flags: list[str],
    unknown_category: str,
    unknown_command: str,
) -> None:
    args = [*flags, "learn", "explain", f"{unknown_category}.{unknown_command}"]
    result = _invoke_cli(args)
    assert result.exit_code == 1
    _assert_tools_cli_error(result, "unknown", "not found")


@given(flags=GLOBAL_FLAGS_STRATEGY, bad_value=_NON_INT_TEXT)
@example(flags=[], bad_value="not-a-number")
@settings(max_examples=30, deadline=None, derandomize=True)
def test_dev_gha_rejects_non_int_limit(flags: list[str], bad_value: str) -> None:
    args = [*flags, "dev", "gha", "--limit", bad_value]
    result = _invoke_cli(args)
    assert result.exit_code != 0
    _assert_tools_cli_error(result, "invalid value")


@given(flags=GLOBAL_FLAGS_STRATEGY, bad_value=_NON_INT_TEXT)
@example(flags=[], bad_value="not-a-number")
@settings(max_examples=30, deadline=None, derandomize=True)
def test_dev_gha_rejects_non_int_run_id(flags: list[str], bad_value: str) -> None:
    args = [*flags, "dev", "gha", "--run-id", bad_value]
    result = _invoke_cli(args)
    assert result.exit_code != 0
    _assert_tools_cli_error(result, "invalid value")


@given(
    flags=GLOBAL_FLAGS_STRATEGY,
    opt=st.sampled_from(["--limit", "--run-id"]),
)
@settings(max_examples=20, deadline=None, derandomize=True)
def test_dev_gha_requires_option_values(flags: list[str], opt: str) -> None:
    args = [*flags, "dev", "gha", opt]
    result = _invoke_cli(args)
    assert result.exit_code != 0
    _assert_tools_cli_error(result, "requires an argument", "missing")


@given(flags=GLOBAL_FLAGS_STRATEGY, bad_value=_NON_INT_TEXT)
@example(flags=[], bad_value="not-a-number")
@settings(max_examples=30, deadline=None, derandomize=True)
def test_dev_kill_port_rejects_non_int_port(flags: list[str], bad_value: str) -> None:
    args = [*flags, "dev", "kill-port", bad_value]
    result = _invoke_cli(args)
    assert result.exit_code != 0
    _assert_tools_cli_error(result, "invalid value")


@given(flags=GLOBAL_FLAGS_STRATEGY)
@settings(max_examples=20, deadline=None, derandomize=True)
def test_dev_kill_port_requires_port_argument(flags: list[str]) -> None:
    args = [*flags, "dev", "kill-port"]
    result = _invoke_cli(args)
    assert result.exit_code != 0
    _assert_tools_cli_error(result, "missing argument", "usage:")


@given(flags=GLOBAL_FLAGS_STRATEGY, bad_value=_NON_INT_TEXT)
@example(flags=[], bad_value="not-a-number")
@settings(max_examples=30, deadline=None, derandomize=True)
def test_dev_review_bulk_reply_rejects_non_int_pr_number(
    flags: list[str],
    bad_value: str,
) -> None:
    args = [*flags, "dev", "review-bulk-reply", bad_value, "--replies", "replies.json"]
    result = _invoke_cli(args)
    assert result.exit_code != 0
    _assert_tools_cli_error(result, "invalid value")


@given(flags=GLOBAL_FLAGS_STRATEGY)
@settings(max_examples=20, deadline=None, derandomize=True)
def test_dev_review_bulk_reply_requires_replies_value(flags: list[str]) -> None:
    args = [*flags, "dev", "review-bulk-reply", "1", "--replies"]
    result = _invoke_cli(args)
    assert result.exit_code != 0
    _assert_tools_cli_error(result, "requires an argument", "missing")


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
    with override_tools_with_deterministic_runner(load_config=_load_tools_config_stub):
        args: list[str] = []
        if global_flags:
            args.extend(global_flags)
        args.extend(command)
        if extra:
            args.extend(extra)
        return _invoke_cli_unisolated(args)


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

        exit_code: int | None = None
        try:
            tools_cli.main_entry()
        except typer.Exit as exc:
            exit_code = exc.exit_code

    assert exit_code == 1
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

        exit_code: int | None = None
        try:
            tools_cli.main_entry()
        except typer.Exit as exc:
            exit_code = exc.exit_code

    assert exit_code == 1
    assert any(reason in msg for msg in echoed)
