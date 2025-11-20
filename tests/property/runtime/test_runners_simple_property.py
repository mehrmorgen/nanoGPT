"""Property-based tests for runtime/runners module.

Tests function availability and basic behavior without complex execution.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Callable, Optional

from hypothesis import given, settings
from hypothesis import strategies as st

from ml_playground.core.logging_protocol import LoggerLike
from ml_playground.runtime import runners as runtime_runners


@st.composite
def valid_strings(draw: st.DrawFn) -> str:
    """Generate valid non-empty strings."""
    return draw(st.text(min_size=1, max_size=20, alphabet="abcdefghijklmnopqrstuvwxyz"))


class _StubLogger(LoggerLike):
    """Simple logger implementation satisfying LoggerLike for tests."""

    def debug(self, msg: str, *args: object, **kwargs: object) -> None:  # noqa: D401
        """Log debug message (no-op)."""

    def info(self, msg: str, *args: object, **kwargs: object) -> None:  # noqa: D401
        """Log info message (no-op)."""

    def warning(self, msg: str, *args: object, **kwargs: object) -> None:  # noqa: D401
        """Log warning message (no-op)."""

    def error(self, msg: str, *args: object, **kwargs: object) -> None:  # noqa: D401
        """Log error message (no-op)."""


@dataclass
class _StubRuntimeCfg:
    device: str
    dtype: str
    seed: int


@dataclass
class _FakePreparerData:
    block_size: int


@dataclass
class _FakeModel:
    n_layer: int
    n_head: int
    n_embd: int
    vocab_size: int
    context_size: int


@dataclass
class _FakeOptimizer:
    learning_rate: float


@dataclass
class _FakeTrainerState:
    max_steps: int
    eval_interval: int
    save_interval: int


@dataclass
class _FakeSamplerState:
    num_samples: int
    temperature: float
    top_k: int | None


@dataclass
class _StubPreparerCfg:
    runtime: _StubRuntimeCfg
    data: _FakePreparerData
    logger: LoggerLike


@dataclass
class _StubTrainerCfg:
    runtime: Optional[_StubRuntimeCfg]
    model: Optional[_FakeModel]
    optimizer: Optional[_FakeOptimizer]
    trainer: Optional[_FakeTrainerState]
    logger: LoggerLike


@dataclass
class _StubSamplerCfg:
    runtime: _StubRuntimeCfg
    model: Optional[_FakeModel]
    sampler: Optional[_FakeSamplerState]
    logger: LoggerLike


@dataclass
class _StubSharedConfig:
    """Stub shared config used for log_command_status tests.

    It matches the SharedConfigLike protocol by exposing ``dataset_dir``.
    """

    dataset_dir: Any


class _NoopTrainer:
    def __init__(self, *args: Any, **kwargs: Any) -> None:  # noqa: ARG002
        pass

    def run(self) -> None:  # noqa: D401
        return None


class _NoopSampler:
    def __init__(self, *args: Any, **kwargs: Any) -> None:  # noqa: ARG002
        pass

    def run(self) -> None:  # noqa: D401
        return None


def _noop_trainer_factory(*args: Any, **kwargs: Any) -> _NoopTrainer:  # noqa: ARG002
    return _NoopTrainer()


def _noop_sampler_factory(*args: Any, **kwargs: Any) -> _NoopSampler:  # noqa: ARG002
    return _NoopSampler()


def _make_preparer_cfg() -> _StubPreparerCfg:
    """Construct a minimal PreparerConfig suitable for runtime.runners tests."""

    cfg = _StubPreparerCfg(
        runtime=_StubRuntimeCfg(device="cpu", dtype="float32", seed=42),
        data=_FakePreparerData(block_size=128),
        logger=_StubLogger(),
    )
    return cfg


def _make_trainer_cfg() -> _StubTrainerCfg:
    """Construct a minimal TrainerConfig suitable for runtime.runners tests."""

    cfg = _StubTrainerCfg(
        runtime=_StubRuntimeCfg(device="cpu", dtype="float32", seed=42),
        model=_FakeModel(
            n_layer=6,
            n_head=6,
            n_embd=384,
            vocab_size=1000,
            context_size=128,
        ),
        optimizer=_FakeOptimizer(learning_rate=0.001),
        trainer=_FakeTrainerState(max_steps=100, eval_interval=50, save_interval=100),
        logger=_StubLogger(),
    )
    return cfg


def _make_sampler_cfg() -> _StubSamplerCfg:
    """Construct a minimal SamplerConfig suitable for runtime.runners tests."""

    cfg = _StubSamplerCfg(
        runtime=_StubRuntimeCfg(device="cpu", dtype="float32", seed=42),
        model=_FakeModel(
            n_layer=6,
            n_head=6,
            n_embd=384,
            vocab_size=1000,
            context_size=128,
        ),
        sampler=_FakeSamplerState(num_samples=5, temperature=1.0, top_k=None),
        logger=_StubLogger(),
    )
    return cfg


@given(
    device=st.sampled_from(["cpu", "cuda", "mps"]),
    dtype=st.sampled_from(["float32", "float16", "bfloat16"]),
    seed=st.integers(min_value=0, max_value=2**31 - 1),
)
@settings(max_examples=10, deadline=None, derandomize=True)
def test_global_device_setup_signature(
    device: str, dtype: str, seed: int
) -> None:
    """Test global_device_setup accepts different parameters."""
    # Just verify the function exists and is callable
    assert callable(runtime_runners.global_device_setup)
    
    # Test with minimal parameters - will set global device state
    runtime_runners.global_device_setup(device, dtype, seed)


@given(
    tag=valid_strings(),
    experiment=valid_strings(),
    config_path=valid_strings(),
)
@settings(max_examples=10, deadline=None, derandomize=True)
def test_run_prepare_impl_signature(
    tag: str, experiment: str, config_path: str
) -> None:
    """Test run_prepare_impl function signature."""
    assert callable(runtime_runners.run_prepare_impl)
    
    # Create minimal fake config
    prepare_cfg = _make_preparer_cfg()
    
    shared = SimpleNamespace()
    shared.project_home = Path("/tmp")
    shared.data_dir = Path("/tmp/data")
    shared.out_dir = Path("/tmp/out")
    
    # Patch external dependencies to avoid actual execution
    import ml_playground.runtime.runners as runners_module
    original_prepare = getattr(runners_module, 'prepare_data', None)
    
    def fake_prepare_data(*args: Any, **kwargs: Any) -> Any:
        result = SimpleNamespace()
        result.train_data = SimpleNamespace()
        result.val_data = SimpleNamespace()
        result.meta = SimpleNamespace()
        result.meta.train_tokens = 1000
        result.meta.val_tokens = 100
        return result
    
    runners_module.prepare_data = fake_prepare_data
    
    try:
        # This should return a ToolResult
        result = runtime_runners.run_prepare_impl(
            experiment, prepare_cfg, Path(config_path), shared
        )
        
        assert hasattr(result, 'success')
        assert hasattr(result, 'exit_code')
        assert hasattr(result, 'operation_id')
    finally:
        if original_prepare is not None:
            runners_module.prepare_data = original_prepare


@given(
    tag=valid_strings(),
    experiment=valid_strings(),
    config_path=valid_strings(),
)
@settings(max_examples=10, deadline=None, derandomize=True)
def test_run_train_impl_signature(
    tag: str, experiment: str, config_path: str
) -> None:
    """Test run_train_impl function signature."""
    assert callable(runtime_runners.run_train_impl)
    
    # Create minimal fake config
    train_cfg = _make_trainer_cfg()
    
    shared = SimpleNamespace()
    shared.project_home = Path("/tmp")
    shared.data_dir = Path("/tmp/data")
    shared.out_dir = Path("/tmp/out")
    
    # Patch external dependencies
    import ml_playground.runtime.runners as runners_module
    original_trainer = getattr(runners_module, 'CoreTrainer', None)
    original_optimizer = getattr(runners_module, 'create_optimizer', None)
    original_gpt = getattr(runners_module, 'GPT', None)
    
    class FakeTrainer:
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            self.step = 0
        
        def run(self) -> None:
            return SimpleNamespace(best_val_loss=0.5)
    
    class FakeGPT:
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            self.train = lambda: None
            self.eval = lambda: None
            self.parameters = lambda: []
    
    def fake_create_optimizer(*args: Any, **kwargs: Any) -> Any:
        return SimpleNamespace(step=lambda: None, param_groups=[])
    
    runners_module.CoreTrainer = FakeTrainer
    runners_module.create_optimizer = fake_create_optimizer
    runners_module.GPT = FakeGPT
    
    try:
        result = runtime_runners.run_train_impl(
            experiment, train_cfg, Path(config_path), shared
        )
        
        assert hasattr(result, 'success')
        assert hasattr(result, 'exit_code')
        assert hasattr(result, 'operation_id')
    finally:
        if original_trainer is not None:
            runners_module.CoreTrainer = original_trainer
        if original_optimizer is not None:
            runners_module.create_optimizer = original_optimizer
        if original_gpt is not None:
            runners_module.GPT = original_gpt


@given(
    tag=valid_strings(),
    experiment=valid_strings(),
    config_path=valid_strings(),
)
@settings(max_examples=10, deadline=None, derandomize=True)
def test_run_sample_impl_signature(
    tag: str, experiment: str, config_path: str
) -> None:
    """Test run_sample_impl function signature."""
    assert callable(runtime_runners.run_sample_impl)
    
    # Create minimal fake config
    sample_cfg = _make_sampler_cfg()
    
    shared = SimpleNamespace()
    shared.project_home = Path("/tmp")
    shared.data_dir = Path("/tmp/data")
    shared.out_dir = Path("/tmp/out")
    
    # Patch external dependencies
    import ml_playground.runtime.runners as runners_module
    original_sampler = getattr(runners_module, 'Sampler', None)
    original_load = getattr(runners_module, 'load_checkpoint', None)
    original_gpt = getattr(runners_module, 'GPT', None)
    
    class FakeSampler:
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            self.samples = ["sample"] * 5
        
        def run(self) -> None:
            return self.samples
    
    class FakeGPT:
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            self.train = lambda: None
            self.eval = lambda: None
            self.parameters = lambda: []
    
    def fake_load_checkpoint(*args: Any, **kwargs: Any) -> Any:
        return SimpleNamespace(model=FakeGPT())
    
    runners_module.Sampler = FakeSampler
    runners_module.load_checkpoint = fake_load_checkpoint
    runners_module.GPT = FakeGPT
    
    try:
        result = runtime_runners.run_sample_impl(
            experiment, sample_cfg, Path(config_path), shared
        )
        
        assert hasattr(result, 'success')
        assert hasattr(result, 'exit_code')
        assert hasattr(result, 'operation_id')
    finally:
        if original_sampler is not None:
            runners_module.Sampler = original_sampler
        if original_load is not None:
            runners_module.load_checkpoint = original_load
        if original_gpt is not None:
            runners_module.GPT = original_gpt


@given(
    experiment=valid_strings(),
    host=valid_strings(),
    port=st.integers(min_value=1024, max_value=65535),
    open_browser=st.booleans(),
)
@settings(max_examples=10, deadline=None, derandomize=True)
def test_run_analyze_signature(
    experiment: str, host: str, port: int, open_browser: bool
) -> None:
    """Test run_analyze function signature."""
    assert callable(runtime_runners.run_analyze)
    
    # Patch external dependencies
    import ml_playground.runtime.runners as runners_module
    original_analyze = getattr(runners_module, 'analyze_checkpoint', None)
    
    def fake_analyze_checkpoint(*args: Any, **kwargs: Any) -> Any:
        return SimpleNamespace(report="Analysis report")
    
    runners_module.analyze_checkpoint = fake_analyze_checkpoint
    
    try:
        result = runtime_runners.run_analyze(
            experiment, host, port, open_browser
        )
        
        assert hasattr(result, 'success')
        assert hasattr(result, 'exit_code')
        assert hasattr(result, 'operation_id')
    finally:
        if original_analyze is not None:
            runners_module.analyze_checkpoint = original_analyze


def test_all_runtime_functions_exist() -> None:
    """Test that all expected runtime functions are available."""
    expected_functions = [
        "create_pipeline",
        "global_device_setup",
        "log_command_status",
        "run_analyze",
        "run_prepare_impl",
        "run_sample_impl",
        "run_train_impl",
    ]
    
    for func_name in expected_functions:
        assert hasattr(runtime_runners, func_name)
        assert callable(getattr(runtime_runners, func_name))


@given(
    tag=valid_strings(),
    out_dir=valid_strings(),
    message=valid_strings(),
)
@settings(max_examples=10, deadline=None, derandomize=True)
def test_log_command_status_signature(
    tag: str, out_dir: str, message: str
) -> None:
    """Test log_command_status function signature."""
    assert callable(runtime_runners.log_command_status)
    
    shared = _StubSharedConfig(dataset_dir=Path("/tmp/dataset"))

    logger: LoggerLike = _StubLogger()
    
    # Should not raise an exception
    runtime_runners.log_command_status(tag, shared, Path(out_dir), logger)


@given(experiment=valid_strings())
@settings(max_examples=5, deadline=None, derandomize=True)
def test_run_train_impl_missing_runtime_returns_failure(experiment: str) -> None:
    """run_train_impl returns failure ToolResult when runtime config is missing."""
    train_cfg = _StubTrainerCfg(runtime=_StubRuntimeCfg("cpu", "float32", 42), model=None, optimizer=None, trainer=None, logger=_StubLogger())
    train_cfg.runtime = None

    shared = SimpleNamespace()
    result = runtime_runners.run_train_impl(
        experiment, train_cfg, Path("config.toml"), shared
    )

    assert result.success is False
    assert result.exit_code == 1
    assert result.operation_id.category == "train"


@given(experiment=valid_strings())
@settings(max_examples=5, deadline=None, derandomize=True)
def test_run_sample_impl_missing_runtime_returns_failure(experiment: str) -> None:
    """run_sample_impl returns failure ToolResult when runtime config is missing."""
    sample_cfg = _StubSamplerCfg(runtime=_StubRuntimeCfg("cpu", "float32", 42), model=None, sampler=None, logger=_StubLogger())
    sample_cfg.runtime = None

    shared = SimpleNamespace()
    result = runtime_runners.run_sample_impl(
        experiment, sample_cfg, Path("config.toml"), shared
    )

    assert result.success is False
    assert result.exit_code == 1
    assert result.operation_id.category == "sample"


@given(experiment=valid_strings())
@settings(max_examples=5, deadline=None, derandomize=True)
def test_run_prepare_impl_error_returns_failure(experiment: str) -> None:
    """run_prepare_impl returns failure ToolResult when pipeline raises."""
    prepare_cfg = _make_preparer_cfg()

    shared = SimpleNamespace()

    def bad_pipeline_factory(cfg: Any, shared_: Any) -> Any:  # noqa: ARG001
        raise RuntimeError("pipeline error")

    hooks = runtime_runners.RuntimeRunHooks(
        pipeline_factory=bad_pipeline_factory,
        trainer_factory=_noop_trainer_factory,
        sampler_factory=_noop_sampler_factory,
        device_setup=lambda d, t, s: None,  # type: ignore[unused-argument]
        log_status=lambda tag, shared_, out_dir, logger: None,  # type: ignore[unused-argument]
    )

    result = runtime_runners.run_prepare_impl(
        experiment, prepare_cfg, Path("config.toml"), shared, hooks=hooks
    )

    assert result.success is False
    assert result.exit_code == 1
    assert result.operation_id.category == "prepare"
    assert "failed" in result.stderr.lower()


@given(experiment=valid_strings())
@settings(max_examples=5, deadline=None, derandomize=True)
def test_run_train_impl_error_returns_failure(experiment: str) -> None:
    """run_train_impl returns failure ToolResult when trainer.run raises."""  
    train_cfg = _make_trainer_cfg()

    shared = SimpleNamespace()

    class BadTrainer:
        def __init__(self, *args: Any, **kwargs: Any) -> None:  # noqa: D401, ARG002
            """Initialise bad trainer."""

        def run(self) -> None:  # noqa: D401
            """Raise to simulate training error."""
            raise RuntimeError("training error")

    hooks = runtime_runners.RuntimeRunHooks(
        pipeline_factory=lambda cfg, shared_: None,  # type: ignore[unused-argument]
        trainer_factory=BadTrainer,
        sampler_factory=_noop_sampler_factory,
        device_setup=lambda d, t, s: None,  # type: ignore[unused-argument]
        log_status=lambda tag, shared_, out_dir, logger: None,  # type: ignore[unused-argument]
    )

    result = runtime_runners.run_train_impl(
        experiment, train_cfg, Path("config.toml"), shared, hooks=hooks
    )

    assert result.success is False
    assert result.exit_code == 1
    assert result.operation_id.category == "train"
    assert "failed" in result.stderr.lower()


@given(experiment=valid_strings())
@settings(max_examples=5, deadline=None, derandomize=True)
def test_run_sample_impl_error_returns_failure(experiment: str) -> None:
    """run_sample_impl returns failure ToolResult when sampler.run raises."""
    sample_cfg = _make_sampler_cfg()

    shared = SimpleNamespace()

    class BadSampler:
        def __init__(self, *args: Any, **kwargs: Any) -> None:  # noqa: D401, ARG002
            """Initialise bad sampler."""

        def run(self) -> None:  # noqa: D401
            """Raise to simulate sampling error."""
            raise RuntimeError("sampling error")

    hooks = runtime_runners.RuntimeRunHooks(
        pipeline_factory=lambda cfg, shared_: None,  # type: ignore[unused-argument]
        trainer_factory=_noop_trainer_factory,
        sampler_factory=BadSampler,
        device_setup=lambda d, t, s: None,  # type: ignore[unused-argument]
        log_status=lambda tag, shared_, out_dir, logger: None,  # type: ignore[unused-argument]
    )

    result = runtime_runners.run_sample_impl(
        experiment, sample_cfg, Path("config.toml"), shared, hooks=hooks
    )

    assert result.success is False
    assert result.exit_code == 1
    assert result.operation_id.category == "sample"
    assert "failed" in result.stderr.lower()


@given(experiment=valid_strings())
@settings(max_examples=5, deadline=None, derandomize=True)
def test_run_train_impl_seed_resolver_overrides_seed(experiment: str) -> None:
    """run_train_impl uses resolve_seed hook when provided and int is returned."""
    train_cfg = _make_trainer_cfg()

    shared = SimpleNamespace()

    class SeedTrackingDevice:
        def __init__(self) -> None:
            self.last_seed: int | None = None

        def __call__(self, device: str, dtype: str, seed: int) -> None:  # noqa: ARG002
            self.last_seed = seed

    device_tracker = SeedTrackingDevice()

    class NoopTrainer:
        def __init__(self, *args: Any, **kwargs: Any) -> None:  # noqa: D401, ARG002
            """Initialise trainer."""

        def run(self) -> None:  # noqa: D401
            """No-op run."""
            return None

    def resolve_seed(tag: str, shared_cfg: Any, seed: int) -> int | None:  # noqa: ARG001
        return seed + 1

    hooks = runtime_runners.RuntimeRunHooks(
        pipeline_factory=lambda cfg, shared_: None,  # type: ignore[unused-argument]
        trainer_factory=NoopTrainer,
        sampler_factory=SimpleNamespace,  # unused
        device_setup=device_tracker,
        log_status=lambda tag, shared_, out_dir, logger: None,  # type: ignore[unused-argument]
        resolve_seed=resolve_seed,
    )

    result = runtime_runners.run_train_impl(
        experiment, train_cfg, Path("config.toml"), shared, hooks=hooks
    )

    assert result.success is True
    assert device_tracker.last_seed == train_cfg.runtime.seed + 1


@given(experiment=valid_strings())
@settings(max_examples=5, deadline=None, derandomize=True)
def test_run_sample_impl_seed_resolver_overrides_seed(experiment: str) -> None:
    """run_sample_impl uses resolve_seed hook when provided and int is returned."""  
    sample_cfg = _make_sampler_cfg()

    shared = SimpleNamespace()

    class SeedTrackingDevice:
        def __init__(self) -> None:
            self.last_seed: int | None = None

        def __call__(self, device: str, dtype: str, seed: int) -> None:  # noqa: ARG002
            self.last_seed = seed

    device_tracker = SeedTrackingDevice()

    class NoopSampler:
        def __init__(self, *args: Any, **kwargs: Any) -> None:  # noqa: D401, ARG002
            """Initialise sampler."""

        def run(self) -> None:  # noqa: D401
            """No-op run."""
            return None

    def resolve_seed(tag: str, shared_cfg: Any, seed: int) -> int | None:  # noqa: ARG001
        return seed + 2

    hooks = runtime_runners.RuntimeRunHooks(
        pipeline_factory=lambda cfg, shared_: None,  # type: ignore[unused-argument]
        trainer_factory=SimpleNamespace,  # unused
        sampler_factory=NoopSampler,
        device_setup=device_tracker,
        log_status=lambda tag, shared_, out_dir, logger: None,  # type: ignore[unused-argument]
        resolve_seed=resolve_seed,
    )

    result = runtime_runners.run_sample_impl(
        experiment, sample_cfg, Path("config.toml"), shared, hooks=hooks
    )

    assert result.success is True
    assert device_tracker.last_seed == sample_cfg.runtime.seed + 2


@given(experiment=valid_strings())
@settings(max_examples=5, deadline=None, derandomize=True)
def test_run_train_impl_learning_mode_success(experiment: str) -> None:
    """run_train_impl attaches learning_info when learning_mode_engine is provided."""  
    train_cfg = _make_trainer_cfg()

    shared = SimpleNamespace()

    class NoopTrainer:
        def __init__(self, *args: Any, **kwargs: Any) -> None:  # noqa: D401, ARG002
            """Initialise trainer."""

        def run(self) -> None:  # noqa: D401
            """No-op run."""
            return None

    hooks = runtime_runners.RuntimeRunHooks(
        pipeline_factory=lambda cfg, shared_: None,  # type: ignore[unused-argument]
        trainer_factory=NoopTrainer,
        sampler_factory=_noop_sampler_factory,
        device_setup=lambda d, t, s: None,  # type: ignore[unused-argument]
        log_status=lambda tag, shared_, out_dir, logger: None,  # type: ignore[unused-argument]
    )

    # Use real LearningModeEngine from runtime.core.results
    from ml_playground.runtime.core.results import LearningModeEngine

    learning_engine = LearningModeEngine()

    result = runtime_runners.run_train_impl(
        experiment,
        train_cfg,
        Path("config.toml"),
        shared,
        learning_mode_engine=learning_engine,
        hooks=hooks,
    )

    assert result.success is True
    assert result.learning_info is not None
    assert result.learning_info.explanations


@given(experiment=valid_strings())
@settings(max_examples=5, deadline=None, derandomize=True)
def test_run_sample_impl_learning_mode_success(experiment: str) -> None:
    """run_sample_impl attaches learning_info when learning_mode_engine is provided."""  
    sample_cfg = _make_sampler_cfg()

    shared = SimpleNamespace()

    class NoopSampler:
        def __init__(self, *args: Any, **kwargs: Any) -> None:  # noqa: D401, ARG002
            """Initialise sampler."""

        def run(self) -> None:  # noqa: D401
            """No-op run."""
            return None

    hooks = runtime_runners.RuntimeRunHooks(
        pipeline_factory=lambda cfg, shared_: None,  # type: ignore[unused-argument]
        trainer_factory=_noop_trainer_factory,
        sampler_factory=NoopSampler,
        device_setup=lambda d, t, s: None,  # type: ignore[unused-argument]
        log_status=lambda tag, shared_, out_dir, logger: None,  # type: ignore[unused-argument]
    )

    from ml_playground.runtime.core.results import LearningModeEngine

    learning_engine = LearningModeEngine()

    result = runtime_runners.run_sample_impl(
        experiment,
        sample_cfg,
        Path("config.toml"),
        shared,
        learning_mode_engine=learning_engine,
        hooks=hooks,
    )

    assert result.success is True
    assert result.learning_info is not None
    assert result.learning_info.explanations


@given(host=valid_strings(), port=st.integers(min_value=1024, max_value=65535))
@settings(max_examples=5, deadline=None, derandomize=True)
def test_run_analyze_success_with_learning_mode(host: str, port: int) -> None:
    """run_analyze succeeds for bundestag_char and attaches learning_info when enabled."""  
    from ml_playground.runtime.core.results import LearningModeEngine

    learning_engine = LearningModeEngine()

    result = runtime_runners.run_analyze(
        experiment="bundestag_char",
        host=host,
        port=port,
        open_browser=False,
        learning_mode_engine=learning_engine,
    )

    assert result.success is True
    assert result.exit_code == 0
    assert result.operation_id.category == "analyze"
    assert result.learning_info is not None
    assert result.learning_info.explanations


@given(experiment=valid_strings().filter(lambda e: e != "bundestag_char"))
@settings(max_examples=5, deadline=None, derandomize=True)
def test_run_analyze_unsupported_experiment_returns_failure(experiment: str) -> None:
    """run_analyze returns failure for unsupported experiment names."""
    result = runtime_runners.run_analyze(
        experiment=experiment,
        host="localhost",
        port=8080,
        open_browser=False,
    )

    assert result.success is False
    assert result.exit_code == 1
    assert result.operation_id.category == "analyze"
    assert "bundestag_char" in result.stderr


def test_run_analyze_error_returns_failure() -> None:
    """run_analyze returns failure ToolResult when logger_factory raises."""

    def bad_logger_factory(name: str) -> Any:  # noqa: ARG001
        raise RuntimeError("logger factory error")

    result = runtime_runners.run_analyze(
        experiment="bundestag_char",
        host="localhost",
        port=8080,
        open_browser=False,
        logger_factory=bad_logger_factory,
    )

    assert result.success is False
    assert result.exit_code == 1
    assert result.operation_id.category == "analyze"
    assert "failed" in result.stderr.lower()
