"""Runtime CLI property tests aligned with the current architecture."""

from __future__ import annotations

import logging
from collections.abc import Callable
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
from typing import Any, ContextManager, cast

import hypothesis.strategies as st
from hypothesis import HealthCheck, assume, example, given, settings
import pytest
import typer
from typer.testing import CliRunner

# Import pytest typing for fixtures
try:
    from _pytest.logging import LogCaptureFixture
except ImportError:
    # Fallback for older pytest versions
    from typing import Any as LogCaptureFixture

from ml_playground.runtime.cli.main import (
    app,
    CLIDependencies,
    extract_exp_config,
    global_device_setup,
    global_options,
    log_command_status,
    log_directory,
    override_cli_dependencies,
    run_or_exit,
)
from ml_playground.configuration.models import (
    DataConfig,
    ExperimentConfig,
    LRSchedule,
    ModelConfig,
    OptimConfig,
    PreparerConfig,
    RuntimeConfig,
    SampleConfig,
    SamplerConfig,
    SharedConfig,
    TrainerConfig,
)
from ml_playground.runtime.core.results import (
    LearningModeEngine,
    ToolResult,
    VerbosityLevel,
)


class LoggerProbe:
    """Minimal logger fake that satisfies the LoggerLike protocol."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, str]] = []

    def _record(self, level: str, msg: str, *args: Any, **kwargs: Any) -> None:
        if args:
            try:
                msg = msg % args
            except Exception:
                msg = " ".join([msg, *map(str, args)])
        self.calls.append((level, str(msg)))

    def debug(
        self, msg: str, *args: Any, **kwargs: Any
    ) -> None:  # pragma: no cover - unused
        self._record("debug", msg, *args)

    def info(self, msg: str, *args: Any, **kwargs: Any) -> None:
        self._record("info", msg, *args)

    def warning(
        self, msg: str, *args: Any, **kwargs: Any
    ) -> None:  # pragma: no cover - unused
        self._record("warning", msg, *args)

    def error(
        self, msg: str, *args: Any, **kwargs: Any
    ) -> None:  # pragma: no cover - unused
        self._record("error", msg, *args)

    @property
    def infos(self) -> list[str]:
        return [message for level, message in self.calls if level == "info"]


_EXCEPTIONS = st.sampled_from([FileNotFoundError, ValueError, RuntimeError])
_MESSAGES = st.text(min_size=1, max_size=32)
_EXPERIMENT_NAMES = st.text(
    alphabet="abcdefghijklmnopqrstuvwxyz0123456789_", min_size=1, max_size=8
)


_KNOWN_TOP_LEVEL = {
    "prepare",
    "train",
    "sample",
    "analyze",
    "--help",
    "-h",
}


_KNOWN_SUBCOMMANDS = ("prepare", "train", "sample", "analyze")


_PATH_TOKEN = st.text(
    alphabet=st.characters(
        whitelist_categories=("Ll", "Lu", "Nd"), whitelist_characters="_-"
    ),
    min_size=1,
    max_size=20,
)


_REL_SEGMENT = st.text(
    alphabet=st.characters(
        whitelist_categories=("Ll", "Lu", "Nd"), whitelist_characters="_-"
    ),
    min_size=1,
    max_size=12,
)


_NON_INT_TEXT = st.text(
    alphabet=st.characters(
        whitelist_categories=("Ll", "Lu"), whitelist_characters="_-"
    ),
    min_size=1,
    max_size=12,
)


_INVALID_TOKEN_POOL = [f"invalid-token-{index}" for index in range(200)]
_UNKNOWN_COMMAND_LIST = [
    token for token in _INVALID_TOKEN_POOL if token not in _KNOWN_TOP_LEVEL
]
_UNKNOWN_COMMANDS = (
    st.sampled_from(_UNKNOWN_COMMAND_LIST)
    if _UNKNOWN_COMMAND_LIST
    else st.just("unknown-command")
)


def test_run_or_exit_keyboard_interrupt_logs_message(
    caplog: LogCaptureFixture,
) -> None:
    """KeyboardInterrupt should log the provided message and exit cleanly."""

    with caplog.at_level(logging.INFO, logger="ml_playground.runtime.cli"):

        def _raise_keyboard_interrupt() -> None:
            raise KeyboardInterrupt

        run_or_exit(_raise_keyboard_interrupt, keyboard_interrupt_msg="Interrupted")

    assert "Interrupted" in caplog.messages


@given(command=_UNKNOWN_COMMANDS)
@settings(max_examples=25, deadline=None, derandomize=True)
def test_runtime_cli_reports_unknown_commands(command: str) -> None:
    runner = CliRunner()
    result = runner.invoke(app, [command])
    assert result.exit_code != 0
    stream = (result.stderr or result.stdout).lower()
    assert (
        "no such command" in stream or "unknown command" in stream or "usage:" in stream
    )
    assert "traceback" not in stream


def test_runtime_cli_help_always_succeeds() -> None:
    runner = CliRunner()
    result = runner.invoke(app, ["--help"])
    assert result.exit_code == 0
    output = (result.stdout or "") + (result.stderr or "")
    lowered = output.lower()
    assert "usage:" in lowered
    assert "traceback" not in lowered


def test_runtime_cli_short_help_flag_never_shows_traceback() -> None:
    runner = CliRunner()
    result = runner.invoke(app, ["-h"])
    output = (result.stdout or "") + (result.stderr or "")
    lowered = output.lower()
    assert "traceback" not in lowered
    assert result.exit_code != 0 or "usage:" in lowered


@given(subcommand=st.sampled_from(_KNOWN_SUBCOMMANDS))
@settings(max_examples=10, deadline=None, derandomize=True)
def test_runtime_cli_subcommand_help_always_succeeds(subcommand: str) -> None:
    runner = CliRunner()
    result = runner.invoke(app, [subcommand, "--help"])
    assert result.exit_code == 0
    output = (result.stdout or "") + (result.stderr or "")
    lowered = output.lower()
    assert "usage:" in lowered
    assert "traceback" not in lowered


@given(
    whitespace=st.lists(
        st.sampled_from([" ", "  ", "\t", "\n"]),
        min_size=1,
        max_size=3,
    )
)
@example(whitespace=[" "])
@settings(max_examples=20, deadline=None, derandomize=True)
def test_runtime_cli_whitespace_args_never_show_traceback(
    whitespace: list[str],
) -> None:
    runner = CliRunner()
    result = runner.invoke(app, whitespace)
    if result.exception is not None:
        assert isinstance(result.exception, SystemExit)
    output = (result.stdout or "") + (result.stderr or "")
    lowered = output.lower()
    assert "traceback" not in lowered
    assert "usage:" in lowered or "no such command" in lowered


@given(
    bad_value=st.integers(min_value=-5, max_value=5).filter(lambda v: v < 0 or v > 2)
)
@settings(max_examples=20, deadline=None, derandomize=True)
def test_runtime_cli_rejects_invalid_verbosity_range(bad_value: int) -> None:
    runner = CliRunner()
    result = runner.invoke(app, ["--verbosity", str(bad_value)])
    assert result.exit_code != 0
    stream = ((result.stderr or result.stdout) or "").lower()
    assert "traceback" not in stream
    assert "invalid value" in stream or "usage:" in stream


@given(bad_value=_NON_INT_TEXT)
@settings(max_examples=20, deadline=None, derandomize=True)
def test_runtime_cli_rejects_non_int_verbosity(bad_value: str) -> None:
    runner = CliRunner()
    result = runner.invoke(app, ["--verbosity", bad_value])
    assert result.exit_code != 0
    stream = ((result.stderr or result.stdout) or "").lower()
    assert "traceback" not in stream
    assert "invalid value" in stream or "usage:" in stream


@given(name=_PATH_TOKEN)
@settings(max_examples=25, deadline=None, derandomize=True)
def test_runtime_cli_missing_exp_config_exits_with_stable_error(name: str) -> None:
    runner = CliRunner()
    with TemporaryDirectory() as tmpdir:
        missing = Path(tmpdir) / f"{name}.toml"
        result = runner.invoke(
            app,
            ["--exp-config", str(missing), "prepare", "nonexistent"],
        )

    assert result.exit_code == 2
    error_stream = (result.stderr or result.stdout) or ""
    assert "Config file not found" in error_stream
    assert "Traceback" not in error_stream


@given(name=_PATH_TOKEN)
@settings(max_examples=25, deadline=None, derandomize=True)
def test_runtime_cli_existing_exp_config_does_not_trigger_missing_error(
    name: str,
) -> None:
    runner = CliRunner()
    with TemporaryDirectory() as tmpdir:
        config_path = Path(tmpdir) / f"{name}.toml"
        config_path.write_text("\n", encoding="utf-8")
        result = runner.invoke(
            app,
            ["--exp-config", str(config_path), "prepare", "nonexistent"],
        )

    error_stream = (result.stderr or result.stdout) or ""
    assert "Config file not found" not in error_stream


@given(filename=_REL_SEGMENT)
@settings(max_examples=25, deadline=None, derandomize=True)
def test_runtime_cli_missing_exp_config_relative_path_is_stable(
    filename: str,
) -> None:
    runner = CliRunner()
    with runner.isolated_filesystem():
        rel_path = Path(f"{filename}.toml")
        result = runner.invoke(
            app,
            ["--exp-config", str(rel_path), "prepare", "nonexistent"],
        )

    assert result.exit_code == 2
    error_stream = (result.stderr or result.stdout) or ""
    assert "Config file not found" in error_stream
    assert "Traceback" not in error_stream


@given(dirname=_REL_SEGMENT, filename=_REL_SEGMENT)
@settings(max_examples=25, deadline=None, derandomize=True)
def test_runtime_cli_exp_config_normalizes_dotdot_paths(
    dirname: str,
    filename: str,
) -> None:
    runner = CliRunner()
    with runner.isolated_filesystem():
        base = Path(dirname)
        base.mkdir(parents=True, exist_ok=True)
        config_path = base / f"{filename}.toml"
        config_path.write_text("\n", encoding="utf-8")

        rel_with_dotdot = Path(dirname) / ".." / dirname / f"{filename}.toml"
        result = runner.invoke(
            app,
            ["--exp-config", str(rel_with_dotdot), "prepare", "nonexistent"],
        )

    error_stream = (result.stderr or result.stdout) or ""
    assert "Config file not found" not in error_stream


def test_extract_exp_config_handles_missing_and_present_context() -> None:
    """extract_exp_config should read the experiment path when available."""

    ctx = cast(typer.Context, cast(Any, SimpleNamespace(obj=None)))
    assert extract_exp_config(ctx) is None

    ctx.obj = {"exp_config": Path("/tmp/demo.toml")}
    assert extract_exp_config(ctx) == Path("/tmp/demo.toml")


def test_log_directory_reports_states(tmp_path: Path) -> None:
    logger = LoggerProbe()

    log_directory("tag", "unset", None, logger)
    missing = tmp_path / "missing"
    log_directory("tag", "missing", missing, logger)

    existing = tmp_path / "existing"
    existing.mkdir()
    (existing / "file.txt").write_text("data", encoding="utf-8")
    log_directory("tag", "existing", existing, logger)

    info_text = "\n".join(logger.infos)
    assert "<not set>" in info_text
    assert "missing" in info_text
    assert "Contents" in info_text


def test_log_command_status_handles_missing_paths(
    tmp_path: Path, shared_config_factory: Callable[[Path], SharedConfig]
) -> None:
    logger = LoggerProbe()
    shared = shared_config_factory(tmp_path)

    log_command_status("tag", shared, None, logger)
    assert any("<not set>" in message for message in logger.infos)


def test_log_command_status_swallows_log_directory_errors(
    tmp_path: Path,
    shared_config_factory: Callable[[Path], SharedConfig],
    override_attr: Callable[[object, str, object], ContextManager[None]],
) -> None:
    logger = LoggerProbe()
    shared = shared_config_factory(tmp_path)

    def boom(*_args: object, **_kwargs: object) -> None:
        raise OSError("boom")

    import ml_playground.runtime.cli.runners as runners_module

    with override_attr(runners_module, "log_directory", boom):
        log_command_status("tag", shared, shared.dataset_dir, logger)

    assert logger.infos == []


def test_global_device_setup_handles_runtime_error() -> None:
    class BadTorch:
        def manual_seed(self, seed: int) -> None:  # pragma: no cover - invoked
            raise RuntimeError("fail")

    with pytest.raises(RuntimeError, match="fail"):
        global_device_setup("cpu", "float32", 123, torch_module=BadTorch())


def test_global_device_setup_sets_cuda_state() -> None:
    seed_calls: list[tuple[str, int]] = []

    def _record_cpu_seed(seed: int) -> None:
        seed_calls.append(("cpu", seed))

    def _record_cuda_seed(seed: int) -> None:
        seed_calls.append(("cuda", seed))

    def _cuda_available() -> bool:
        return True

    fake_torch = SimpleNamespace(
        manual_seed=_record_cpu_seed,
        cuda=SimpleNamespace(
            manual_seed=_record_cuda_seed,
            is_available=_cuda_available,
        ),
        backends=SimpleNamespace(
            cuda=SimpleNamespace(matmul=SimpleNamespace(allow_tf32=False)),
            cudnn=SimpleNamespace(allow_tf32=False),
        ),
    )

    global_device_setup(
        "cuda", "float16", 7, torch_module=fake_torch, cuda_is_available=_cuda_available
    )

    assert ("cpu", 7) in seed_calls
    assert ("cuda", 7) in seed_calls
    assert fake_torch.backends.cuda.matmul.allow_tf32 is True
    assert fake_torch.backends.cudnn.allow_tf32 is True


@given(exc_type=_EXCEPTIONS, message=_MESSAGES, exit_code=st.integers(1, 32))
@example(exc_type=FileNotFoundError, message="missing.txt", exit_code=1)
@settings(max_examples=20, deadline=None, derandomize=True)
def test_run_or_exit_maps_known_exceptions_to_exit(
    exc_type: type[Exception], message: str, exit_code: int
) -> None:
    def _raise() -> None:
        raise exc_type(message)

    with pytest.raises(typer.Exit) as excinfo:
        run_or_exit(_raise, exception_exit_code=exit_code)

    assert excinfo.value.exit_code == exit_code  # pyright: ignore[reportAttributeAccessIssue,reportUnknownMemberType]


@settings(
    max_examples=40,
    deadline=None,
    derandomize=True,
    suppress_health_check=[HealthCheck.function_scoped_fixture],
)
@given(
    learning_mode=st.booleans(),
    verbosity=st.integers(min_value=0, max_value=2),
    use_exp_path=st.booleans(),
    path_exists=st.booleans(),
)
def test_global_options_sets_context_flags(
    learning_mode: bool,
    verbosity: int,
    use_exp_path: bool,
    path_exists: bool,
    tmp_path: Path,
) -> None:
    ctx = typer.Context(typer.main.get_command(app))
    exp_path = tmp_path / "exp.toml" if use_exp_path else None
    if exp_path is not None and path_exists:
        exp_path.write_text("{}", encoding="utf-8")

    if exp_path is not None and not path_exists:
        assume(False)

    global_options(
        ctx,
        exp_config=exp_path,
        learning_mode=learning_mode,
        verbosity=verbosity,
    )

    assert ctx.obj["exp_config"] == exp_path
    if learning_mode:
        assert ctx.obj.get("learning_mode") is True
    else:
        assert "learning_mode" not in ctx.obj

    verbosity_value = ctx.obj.get("verbosity")
    expected_level = VerbosityLevel(verbosity)
    if expected_level != VerbosityLevel.STANDARD:
        assert verbosity_value == expected_level
    else:
        assert verbosity_value in (None, VerbosityLevel.STANDARD)


@given(experiment=_EXPERIMENT_NAMES)
@example(experiment="alpha")
@settings(max_examples=10, deadline=None, derandomize=True)
def test_prepare_command_invokes_override(experiment: str) -> None:
    """The CLI prepare command should delegate to the injected dependency."""

    with TemporaryDirectory() as tmpdir:
        base = Path(tmpdir)
        dataset_dir = base / "dataset"
        dataset_dir.mkdir()
        train_dir = base / "train"
        train_dir.mkdir()
        sample_dir = base / "sample"
        sample_dir.mkdir()

        config_path = dataset_dir / f"{experiment}.toml"
        config_path.write_text("{}", encoding="utf-8")

        shared = SharedConfig(
            experiment=experiment,
            config_path=config_path,
            project_home=base,
            dataset_dir=dataset_dir,
            train_out_dir=train_dir,
            sample_out_dir=sample_dir,
        )

        exp = ExperimentConfig(
            prepare=PreparerConfig(),
            train=TrainerConfig(
                model=ModelConfig(),
                data=DataConfig(),
                optim=OptimConfig(),
                schedule=LRSchedule(),
                runtime=RuntimeConfig(out_dir=train_dir),
            ),
            sample=SamplerConfig(
                runtime=RuntimeConfig(out_dir=sample_dir),
                sample=SampleConfig(),
            ),
            shared=shared,
        )

        calls: dict[str, int] = {"prepare": 0}

        def _load_experiment(name: str, exp_config: Path | None) -> ExperimentConfig:
            assert name == experiment
            assert exp_config is None
            return exp

        def _run_prepare(
            name: str,
            prepare_cfg: PreparerConfig,
            config_path_arg: Path,
            shared_cfg: SharedConfig,
            learning_mode_engine: LearningModeEngine | None = None,
        ) -> ToolResult:
            calls["prepare"] += 1
            assert name == experiment
            assert prepare_cfg is exp.prepare
            assert shared_cfg is shared
            return ToolResult.create(
                success=True,
                exit_code=0,
                namespace="ml",
                category="prepare",
                command=name,
                stdout="ok",
            )

        def _noop_train(
            name: str,
            train_cfg: TrainerConfig,
            config_path_arg: Path,
            shared_cfg: SharedConfig,
            learning_mode_engine: LearningModeEngine | None = None,
        ) -> ToolResult:
            return ToolResult.create(
                success=True,
                exit_code=0,
                namespace="ml",
                category="train",
                command=name,
                stdout="ok",
            )

        def _noop_sample(
            name: str,
            sample_cfg: SamplerConfig,
            config_path_arg: Path,
            shared_cfg: SharedConfig,
            learning_mode_engine: LearningModeEngine | None = None,
        ) -> ToolResult:
            return ToolResult.create(
                success=True,
                exit_code=0,
                namespace="ml",
                category="sample",
                command=name,
                stdout="ok",
            )

        def _noop_train_prereqs(exp_cfg: ExperimentConfig) -> None:
            return None

        def _noop_sample_prereqs(exp_cfg: ExperimentConfig) -> None:
            return None

        deps = CLIDependencies(
            load_experiment=_load_experiment,
            ensure_train_prerequisites=_noop_train_prereqs,
            ensure_sample_prerequisites=_noop_sample_prereqs,
            run_prepare=_run_prepare,
            run_train=_noop_train,
            run_sample=_noop_sample,
        )

        runner = CliRunner()
        with override_cli_dependencies(deps):
            result = runner.invoke(app, ["prepare", experiment])

        assert result.exit_code == 0
        assert calls["prepare"] == 1


@given(experiment=_EXPERIMENT_NAMES)
@example(experiment="train_test")
@settings(max_examples=10, deadline=None, derandomize=True)
def test_train_command_invokes_override(experiment: str) -> None:
    """The CLI train command should delegate to the injected dependency."""

    with TemporaryDirectory() as tmpdir:
        base = Path(tmpdir)
        dataset_dir = base / "dataset"
        dataset_dir.mkdir()
        train_dir = base / "train"
        train_dir.mkdir()

        config_path = dataset_dir / f"{experiment}.toml"
        config_path.write_text("{}", encoding="utf-8")

        shared = SharedConfig(
            experiment=experiment,
            config_path=config_path,
            project_home=base,
            dataset_dir=dataset_dir,
            train_out_dir=train_dir,
            sample_out_dir=base / "sample",
        )

        exp = ExperimentConfig(
            prepare=PreparerConfig(),
            train=TrainerConfig(
                model=ModelConfig(),
                data=DataConfig(),
                optim=OptimConfig(),
                schedule=LRSchedule(),
                runtime=RuntimeConfig(out_dir=train_dir),
            ),
            sample=SamplerConfig(
                runtime=RuntimeConfig(out_dir=base / "sample"),
                sample=SampleConfig(),
            ),
            shared=shared,
        )

        calls: dict[str, int] = {"train": 0}

        def _load_experiment(name: str, exp_config: Path | None) -> ExperimentConfig:
            assert name == experiment
            assert exp_config is None
            return exp

        def _run_train(
            name: str,
            _cfg: TrainerConfig,
            _config_path: Path,
            _shared: SharedConfig,
            _engine: LearningModeEngine | None,
        ) -> ToolResult:
            calls["train"] += 1
            assert name == experiment
            assert _cfg is exp.train
            assert _shared is shared
            return ToolResult.create(
                success=True,
                exit_code=0,
                namespace="ml",
                category="train",
                command=name,
                stdout="train ok",
            )

        def _noop_prepare(
            name: str,
            prepare_cfg: PreparerConfig,
            config_path_arg: Path,
            shared_cfg: SharedConfig,
            learning_mode_engine: LearningModeEngine | None = None,
        ) -> ToolResult:
            return ToolResult.create(
                success=True,
                exit_code=0,
                namespace="ml",
                category="prepare",
                command=name,
                stdout="prepare ok",
            )

        def _noop_sample(
            name: str,
            sample_cfg: SamplerConfig,
            config_path_arg: Path,
            shared_cfg: SharedConfig,
            learning_mode_engine: LearningModeEngine | None = None,
        ) -> ToolResult:
            return ToolResult.create(
                success=True,
                exit_code=0,
                namespace="ml",
                category="sample",
                command=name,
                stdout="sample ok",
            )

        def _noop_train_prereqs(exp_cfg: ExperimentConfig) -> None:
            return None

        deps = CLIDependencies(
            load_experiment=_load_experiment,
            ensure_train_prerequisites=_noop_train_prereqs,
            ensure_sample_prerequisites=lambda exp: None,
            run_prepare=_noop_prepare,
            run_train=_run_train,
            run_sample=_noop_sample,
        )

        runner = CliRunner()
        with override_cli_dependencies(deps):
            result = runner.invoke(app, ["train", experiment])

        assert result.exit_code == 0
        assert calls["train"] == 1


@given(experiment=_EXPERIMENT_NAMES)
@example(experiment="sample_test")
@settings(max_examples=10, deadline=None, derandomize=True)
def test_sample_command_invokes_override(experiment: str) -> None:
    """The CLI sample command should delegate to the injected dependency."""

    with TemporaryDirectory() as tmpdir:
        base = Path(tmpdir)
        dataset_dir = base / "dataset"
        dataset_dir.mkdir()
        sample_dir = base / "sample"
        sample_dir.mkdir()

        config_path = dataset_dir / f"{experiment}.toml"
        config_path.write_text("{}", encoding="utf-8")

        shared = SharedConfig(
            experiment=experiment,
            config_path=config_path,
            project_home=base,
            dataset_dir=dataset_dir,
            train_out_dir=base / "train",
            sample_out_dir=sample_dir,
        )

        exp = ExperimentConfig(
            prepare=PreparerConfig(),
            train=TrainerConfig(
                model=ModelConfig(),
                data=DataConfig(),
                optim=OptimConfig(),
                schedule=LRSchedule(),
                runtime=RuntimeConfig(out_dir=base / "train"),
            ),
            sample=SamplerConfig(
                runtime=RuntimeConfig(out_dir=sample_dir),
                sample=SampleConfig(),
            ),
            shared=shared,
        )

        calls: dict[str, int] = {"sample": 0}

        def _load_experiment(name: str, exp_config: Path | None) -> ExperimentConfig:
            assert name == experiment
            assert exp_config is None
            return exp

        def _run_sample(
            name: str,
            _cfg: SamplerConfig,
            _config_path: Path,
            _shared: SharedConfig,
            _engine: LearningModeEngine | None,
        ) -> ToolResult:
            calls["sample"] += 1
            assert name == experiment
            assert _cfg is exp.sample
            assert _shared is shared
            return ToolResult.create(
                success=True,
                exit_code=0,
                namespace="ml",
                category="sample",
                command=name,
                stdout="sample ok",
            )

        def _noop_prepare(
            name: str,
            prepare_cfg: PreparerConfig,
            config_path_arg: Path,
            shared_cfg: SharedConfig,
            learning_mode_engine: LearningModeEngine | None = None,
        ) -> ToolResult:
            return ToolResult.create(
                success=True,
                exit_code=0,
                namespace="ml",
                category="prepare",
                command=name,
                stdout="prepare ok",
            )

        def _noop_train(
            name: str,
            train_cfg: TrainerConfig,
            config_path_arg: Path,
            shared_cfg: SharedConfig,
            learning_mode_engine: LearningModeEngine | None = None,
        ) -> ToolResult:
            return ToolResult.create(
                success=True,
                exit_code=0,
                namespace="ml",
                category="train",
                command=name,
                stdout="train ok",
            )

        def _noop_sample_prereqs(exp_cfg: ExperimentConfig) -> None:
            return None

        deps = CLIDependencies(
            load_experiment=_load_experiment,
            ensure_train_prerequisites=lambda exp: None,
            ensure_sample_prerequisites=_noop_sample_prereqs,
            run_prepare=_noop_prepare,
            run_train=_noop_train,
            run_sample=_run_sample,
        )

        runner = CliRunner()
        with override_cli_dependencies(deps):
            result = runner.invoke(app, ["sample", experiment])

        assert result.exit_code == 0
        assert calls["sample"] == 1


@given(
    experiment=_EXPERIMENT_NAMES,
    host=st.ip_addresses(v=4).map(str),
    port=st.integers(min_value=1024, max_value=65535),
    open_browser=st.booleans(),
    override_test_attr=st.just(None),
)
@example(
    experiment="analyze_test",
    host="127.0.0.1",
    port=8050,
    open_browser=True,
    override_test_attr=None,
)
@settings(
    max_examples=5,
    deadline=None,
    derandomize=True,
    suppress_health_check=[HealthCheck.function_scoped_fixture],
)
def test_analyze_command_invokes_override(
    experiment: str,
    host: str,
    port: int,
    open_browser: bool,
    override_test_attr: Any,
    override_attr: Callable[[object, str, object], ContextManager[None]],
) -> None:
    """The CLI analyze command should delegate to the injected analysis runner."""

    calls: dict[str, list[tuple[str, str, int, bool]]] = {"analyze": []}

    def _run_analyze(
        name: str,
        analysis_host: str,
        analysis_port: int,
        should_open_browser: bool,
        learning_engine: LearningModeEngine | None,
    ) -> ToolResult:
        calls["analyze"].append(
            (name, analysis_host, analysis_port, should_open_browser)
        )
        assert name == experiment
        assert analysis_host == host
        assert analysis_port == port
        assert should_open_browser == open_browser
        return ToolResult.create(
            success=True,
            exit_code=0,
            namespace="ml",
            category="analyze",
            command=name,
            stdout="analyze ok",
        )

    # Mock the analysis runner
    import ml_playground.runtime.cli.runners as runners_module

    with override_attr(runners_module, "run_analyze", _run_analyze):
        runner = CliRunner()
        result = runner.invoke(
            app,
            [
                "analyze",
                experiment,
                "--host",
                host,
                "--port",
                str(port),
                "--open-browser" if open_browser else "--no-open-browser",
            ],
        )

    assert result.exit_code == 0
    assert calls["analyze"] == [(experiment, host, port, open_browser)]
