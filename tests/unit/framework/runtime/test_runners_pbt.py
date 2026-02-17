from __future__ import annotations

from pathlib import Path
from types import TracebackType
from typing import Any, Mapping, cast
from hypothesis import given, settings, strategies as st

from ml_playground.framework.runtime import runners
from ml_playground.framework.runtime.protocols import (
    PrepareConfigLike,
    SampleConfigLike,
    TrainConfigLike,
)
from ml_playground.framework.core.logging_protocol import LoggerLike

# --- Mocks & Strategies ---

_ExcInfoType = (
    bool
    | BaseException
    | tuple[type[BaseException], BaseException, TracebackType | None]
    | None
)


class MockLogger(LoggerLike):
    def debug(
        self,
        msg: object,
        *args: object,
        exc_info: _ExcInfoType = None,
        stack_info: bool = False,
        stacklevel: int = 1,
        extra: Mapping[str, object] | None = None,
    ) -> None:
        del msg, args, exc_info, stack_info, stacklevel, extra

    def info(
        self,
        msg: object,
        *args: object,
        exc_info: _ExcInfoType = None,
        stack_info: bool = False,
        stacklevel: int = 1,
        extra: Mapping[str, object] | None = None,
    ) -> None:
        del msg, args, exc_info, stack_info, stacklevel, extra

    def warning(
        self,
        msg: object,
        *args: object,
        exc_info: _ExcInfoType = None,
        stack_info: bool = False,
        stacklevel: int = 1,
        extra: Mapping[str, object] | None = None,
    ) -> None:
        del msg, args, exc_info, stack_info, stacklevel, extra

    def error(
        self,
        msg: object,
        *args: object,
        exc_info: _ExcInfoType = None,
        stack_info: bool = False,
        stacklevel: int = 1,
        extra: Mapping[str, object] | None = None,
    ) -> None:
        del msg, args, exc_info, stack_info, stacklevel, extra


@st.composite
def prepare_configs(draw: st.DrawFn) -> PrepareConfigLike:
    class MockPrepareConfig:
        logger = cast(LoggerLike, MockLogger())
        # Ensure we use draw to satisfy hypothesis
        _dummy = draw(st.booleans())

    return MockPrepareConfig()


@st.composite
def train_configs(draw: st.DrawFn) -> TrainConfigLike:
    class MockRuntime:
        seed = draw(st.integers())
        device = "cpu"
        dtype = "float32"
        out_dir = Path("/tmp")

    class MockTrainConfig:
        logger = cast(LoggerLike, MockLogger())
        runtime = MockRuntime() if draw(st.booleans()) else None
        data = None
        model = None
        optim = None
        schedule = None

    return MockTrainConfig()


@st.composite
def sample_configs(draw: st.DrawFn) -> SampleConfigLike:
    class MockRuntime:
        seed = draw(st.integers())
        device = "cpu"
        dtype = "float32"
        out_dir = Path("/tmp")

    class MockSampleConfig:
        logger = cast(LoggerLike, MockLogger())
        runtime = MockRuntime() if draw(st.booleans()) else None

    return MockSampleConfig()


def _noop(*args: Any, **kwargs: Any) -> None:
    pass


def _failing_factory(*args: Any, **kwargs: Any) -> Any:
    raise RuntimeError("Factory failed")


def _broken_runner_factory(*args: Any, **kwargs: Any) -> Any:
    class NoRun:
        pass

    return NoRun()


class MockLearningEngine:
    def explain_command(self, **kwargs: Any) -> dict[str, str]:
        return {"explanation": "mock"}


# --- Tests ---


@given(config=prepare_configs())
@settings(max_examples=12, deadline=None, derandomize=True)
def test_run_prepare_handles_success(config: PrepareConfigLike) -> None:
    hooks = runners.RuntimeRunHooks(
        pipeline_factory=lambda *_: type("Runner", (), {"run": lambda: None}),
        trainer_factory=_noop,
        sampler_factory=_noop,
        device_setup=_noop,
        log_status=_noop,
    )
    result = runners.run_prepare_impl("exp", config, Path("cfg"), {}, hooks=hooks)
    assert result.success
    assert result.exit_code == 0


@given(config=prepare_configs())
@settings(max_examples=12, deadline=None, derandomize=True)
def test_run_prepare_with_learning_mode(config: PrepareConfigLike) -> None:
    hooks = runners.RuntimeRunHooks(
        pipeline_factory=lambda *_: type("Runner", (), {"run": lambda: None}),
        trainer_factory=_noop,
        sampler_factory=_noop,
        device_setup=_noop,
        log_status=_noop,
    )
    engine: Any = MockLearningEngine()
    result = runners.run_prepare_impl(
        "exp", config, Path("cfg"), {}, hooks=hooks, learning_mode_engine=engine
    )
    assert result.success
    assert result.learning_info == {"explanation": "mock"}


@given(config=prepare_configs())
@settings(max_examples=12, deadline=None, derandomize=True)
def test_run_prepare_failure_with_learning_mode(config: PrepareConfigLike) -> None:
    hooks = runners.RuntimeRunHooks(
        pipeline_factory=_failing_factory,
        trainer_factory=_noop,
        sampler_factory=_noop,
        device_setup=_noop,
        log_status=_noop,
    )
    engine: Any = MockLearningEngine()
    result = runners.run_prepare_impl(
        "exp", config, Path("cfg"), {}, hooks=hooks, learning_mode_engine=engine
    )
    assert not result.success
    assert result.learning_info == {"explanation": "mock"}
    hooks = runners.RuntimeRunHooks(
        pipeline_factory=_failing_factory,
        trainer_factory=_noop,
        sampler_factory=_noop,
        device_setup=_noop,
        log_status=_noop,
    )
    result = runners.run_prepare_impl("exp", config, Path("cfg"), {}, hooks=hooks)
    assert not result.success
    assert result.exit_code == 1
    assert "Pipeline preparation failed" in (result.stderr or "")


@given(config=prepare_configs())
@settings(max_examples=12, deadline=None, derandomize=True)
def test_run_prepare_handles_broken_runner(config: PrepareConfigLike) -> None:
    hooks = runners.RuntimeRunHooks(
        pipeline_factory=_broken_runner_factory,
        trainer_factory=_noop,
        sampler_factory=_noop,
        device_setup=_noop,
        log_status=_noop,
    )
    result = runners.run_prepare_impl("exp", config, Path("cfg"), {}, hooks=hooks)
    assert not result.success
    assert "without a run() method" in (result.stderr or "")


@given(config=train_configs())
@settings(max_examples=12, deadline=None, derandomize=True)
def test_run_train_handles_scenarios(config: TrainConfigLike) -> None:
    hooks = runners.RuntimeRunHooks(
        pipeline_factory=_noop,
        trainer_factory=lambda *_: type("Runner", (), {"run": lambda: None}),
        sampler_factory=_noop,
        device_setup=_noop,
        log_status=_noop,
    )

    result = runners.run_train_impl("exp", config, Path("cfg"), {}, hooks=hooks)

    if config.runtime is None:
        assert not result.success
        assert "Runtime configuration is missing" in (result.stderr or "")
    else:
        assert result.success
        assert result.exit_code == 0


@given(config=train_configs())
@settings(max_examples=12, deadline=None, derandomize=True)
def test_run_train_handles_factory_failure(config: TrainConfigLike) -> None:
    hooks = runners.RuntimeRunHooks(
        pipeline_factory=_noop,
        trainer_factory=_failing_factory,
        sampler_factory=_noop,
        device_setup=_noop,
        log_status=_noop,
    )

    result = runners.run_train_impl("exp", config, Path("cfg"), {}, hooks=hooks)

    if config.runtime is None:
        assert not result.success
        assert "Runtime configuration is missing" in (result.stderr or "")
    else:
        assert not result.success
        assert "Training failed" in (result.stderr or "")


@given(config=train_configs())
@settings(max_examples=12, deadline=None, derandomize=True)
def test_run_train_handles_broken_runner(config: TrainConfigLike) -> None:
    hooks = runners.RuntimeRunHooks(
        pipeline_factory=_noop,
        trainer_factory=_broken_runner_factory,
        sampler_factory=_noop,
        device_setup=_noop,
        log_status=_noop,
    )
    result = runners.run_train_impl("exp", config, Path("cfg"), {}, hooks=hooks)
    if config.runtime:
        assert not result.success
        assert "without a run() method" in (result.stderr or "")


@given(config=train_configs())
@settings(max_examples=12, deadline=None, derandomize=True)
def test_run_train_with_learning_mode(config: TrainConfigLike) -> None:
    hooks = runners.RuntimeRunHooks(
        pipeline_factory=_noop,
        trainer_factory=lambda *_: type("Runner", (), {"run": lambda: None}),
        sampler_factory=_noop,
        device_setup=_noop,
        log_status=_noop,
    )
    # Cast to Any to satisfy typing
    engine: Any = MockLearningEngine()
    result = runners.run_train_impl(
        "exp", config, Path("cfg"), {}, hooks=hooks, learning_mode_engine=engine
    )
    if config.runtime:
        assert result.success
        assert result.learning_info == {"explanation": "mock"}


@given(config=sample_configs())
@settings(max_examples=12, deadline=None, derandomize=True)
def test_run_sample_handles_factory_failure(config: SampleConfigLike) -> None:
    hooks = runners.RuntimeRunHooks(
        pipeline_factory=_noop,
        trainer_factory=_noop,
        sampler_factory=_failing_factory,
        device_setup=_noop,
        log_status=_noop,
    )
    result = runners.run_sample_impl("exp", config, Path("cfg"), {}, hooks=hooks)
    if config.runtime:
        assert not result.success
        assert "Sampling failed" in (result.stderr or "")


@given(config=sample_configs())
@settings(max_examples=12, deadline=None, derandomize=True)
def test_run_sample_handles_broken_runner(config: SampleConfigLike) -> None:
    hooks = runners.RuntimeRunHooks(
        pipeline_factory=_noop,
        trainer_factory=_noop,
        sampler_factory=_broken_runner_factory,
        device_setup=_noop,
        log_status=_noop,
    )
    result = runners.run_sample_impl("exp", config, Path("cfg"), {}, hooks=hooks)
    if config.runtime:
        assert not result.success
        assert "without a run() method" in (result.stderr or "")


@given(config=sample_configs())
@settings(max_examples=12, deadline=None, derandomize=True)
def test_run_sample_with_learning_mode(config: SampleConfigLike) -> None:
    hooks = runners.RuntimeRunHooks(
        pipeline_factory=_noop,
        trainer_factory=_noop,
        sampler_factory=lambda *_: type("Runner", (), {"run": lambda: None}),
        device_setup=_noop,
        log_status=_noop,
    )
    engine: Any = MockLearningEngine()
    result = runners.run_sample_impl(
        "exp", config, Path("cfg"), {}, hooks=hooks, learning_mode_engine=engine
    )
    if config.runtime:
        assert result.success
        assert result.learning_info == {"explanation": "mock"}
    hooks = runners.RuntimeRunHooks(
        pipeline_factory=_noop,
        trainer_factory=_noop,
        sampler_factory=lambda *_: type("Runner", (), {"run": lambda: None}),
        device_setup=_noop,
        log_status=_noop,
    )

    result = runners.run_sample_impl("exp", config, Path("cfg"), {}, hooks=hooks)

    if config.runtime is None:
        assert not result.success
        assert "Runtime configuration is missing" in (result.stderr or "")
    else:
        assert result.success
        assert result.exit_code == 0


@given(experiment=st.text(min_size=1))
@settings(max_examples=12, deadline=None, derandomize=True)
def test_run_analyze_validation(experiment: str) -> None:
    def _noop_runner(
        _host: str | None, _port: int, _open_browser: bool, _logger: Any
    ) -> None:
        return None

    engine: Any = MockLearningEngine()
    result = runners.run_analyze(
        experiment,
        "host",
        8000,
        True,
        learning_mode_engine=engine,
        analyze_runner=_noop_runner,
    )
    assert result.success
    assert result.learning_info == {"explanation": "mock"}


def test_run_analyze_handles_exception() -> None:
    def _failing_runner(
        _host: str | None, _port: int, _open_browser: bool, _logger: Any
    ) -> None:
        raise RuntimeError("analyze failed")

    result = runners.run_analyze(
        "bundestag_char", "host", 8000, True, analyze_runner=_failing_runner
    )
    assert not result.success
    assert "Analysis failed: analyze failed" in (result.stderr or "")


def test_run_analyze_success_with_learning_mode() -> None:
    def _noop_runner(
        _host: str | None, _port: int, _open_browser: bool, _logger: Any
    ) -> None:
        return None

    engine: Any = MockLearningEngine()
    result = runners.run_analyze(
        "bundestag_char",
        "host",
        8000,
        True,
        learning_mode_engine=engine,
        analyze_runner=_noop_runner,
    )
    assert result.success
    assert result.learning_info == {"explanation": "mock"}


@given(config=train_configs())
@settings(max_examples=12, deadline=None, derandomize=True)
def test_run_train_exception_with_learning_mode(config: TrainConfigLike) -> None:
    hooks = runners.RuntimeRunHooks(
        pipeline_factory=_noop,
        trainer_factory=_failing_factory,
        sampler_factory=_noop,
        device_setup=_noop,
        log_status=_noop,
    )
    engine: Any = MockLearningEngine()
    result = runners.run_train_impl(
        "exp", config, Path("cfg"), {}, hooks=hooks, learning_mode_engine=engine
    )
    if config.runtime:
        assert not result.success
        assert result.learning_info == {"explanation": "mock"}


@given(config=sample_configs())
@settings(max_examples=12, deadline=None, derandomize=True)
def test_run_sample_exception_with_learning_mode(config: SampleConfigLike) -> None:
    hooks = runners.RuntimeRunHooks(
        pipeline_factory=_noop,
        trainer_factory=_noop,
        sampler_factory=_failing_factory,
        device_setup=_noop,
        log_status=_noop,
    )
    engine: Any = MockLearningEngine()
    result = runners.run_sample_impl(
        "exp", config, Path("cfg"), {}, hooks=hooks, learning_mode_engine=engine
    )
    if config.runtime:
        assert not result.success
        assert result.learning_info == {"explanation": "mock"}


@given(config=train_configs())
@settings(max_examples=12, deadline=None, derandomize=True)
def test_run_train_seed_resolution(config: TrainConfigLike) -> None:
    seeds: list[int] = []

    def _device_setup(device: str, dtype: str, seed: int, **_kwargs: Any) -> None:
        seeds.append(seed)

    def _resolve_seed(_phase: str, _metadata: object, seed: int) -> int:
        return 999

    hooks = runners.RuntimeRunHooks(
        pipeline_factory=_noop,
        trainer_factory=lambda *_: type("Runner", (), {"run": lambda: None}),
        sampler_factory=_noop,
        device_setup=_device_setup,
        log_status=_noop,
        resolve_seed=_resolve_seed,
    )

    result = runners.run_train_impl("exp", config, Path("cfg"), {}, hooks=hooks)

    if config.runtime:
        assert result.success
        assert seeds == [999]


@given(config=sample_configs())
@settings(max_examples=12, deadline=None, derandomize=True)
def test_run_sample_seed_resolution(config: SampleConfigLike) -> None:
    seeds: list[int] = []

    def _device_setup(device: str, dtype: str, seed: int, **_kwargs: Any) -> None:
        seeds.append(seed)

    def _resolve_seed(_phase: str, _metadata: object, seed: int) -> int:
        return 777

    hooks = runners.RuntimeRunHooks(
        pipeline_factory=_noop,
        trainer_factory=_noop,
        sampler_factory=lambda *_: type("Runner", (), {"run": lambda: None}),
        device_setup=_device_setup,
        log_status=_noop,
        resolve_seed=_resolve_seed,
    )

    result = runners.run_sample_impl("exp", config, Path("cfg"), {}, hooks=hooks)

    if config.runtime:
        assert result.success
        assert seeds == [777]


@given(config=train_configs())
@settings(max_examples=12, deadline=None, derandomize=True)
def test_run_train_seed_resolution_none(config: TrainConfigLike) -> None:
    seeds: list[int] = []

    def _device_setup(device: str, dtype: str, seed: int, **_kwargs: Any) -> None:
        seeds.append(seed)

    def _resolve_seed(_phase: str, _metadata: object, seed: int) -> int | None:
        return None

    hooks = runners.RuntimeRunHooks(
        pipeline_factory=_noop,
        trainer_factory=lambda *_: type("Runner", (), {"run": lambda: None}),
        sampler_factory=_noop,
        device_setup=_device_setup,
        log_status=_noop,
        resolve_seed=_resolve_seed,
    )

    result = runners.run_train_impl("exp", config, Path("cfg"), {}, hooks=hooks)

    if config.runtime:
        assert result.success
        # Should use original seed
        assert seeds == [config.runtime.seed]


@given(config=sample_configs())
@settings(max_examples=12, deadline=None, derandomize=True)
def test_run_sample_seed_resolution_none(config: SampleConfigLike) -> None:
    seeds: list[int] = []

    def _device_setup(device: str, dtype: str, seed: int, **_kwargs: Any) -> None:
        seeds.append(seed)

    def _resolve_seed(_phase: str, _metadata: object, seed: int) -> int | None:
        return None

    hooks = runners.RuntimeRunHooks(
        pipeline_factory=_noop,
        trainer_factory=_noop,
        sampler_factory=lambda *_: type("Runner", (), {"run": lambda: None}),
        device_setup=_device_setup,
        log_status=_noop,
        resolve_seed=_resolve_seed,
    )

    result = runners.run_sample_impl("exp", config, Path("cfg"), {}, hooks=hooks)

    if config.runtime:
        assert result.success
        assert seeds == [config.runtime.seed]
