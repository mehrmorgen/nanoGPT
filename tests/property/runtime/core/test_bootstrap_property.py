"""Property-based tests for runtime/core/bootstrap module.

Tests state management, context managers, and dependency injection patterns
using Hypothesis to discover edge cases and verify invariants.
"""

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from ml_playground.runtime.core import bootstrap


@st.composite
def dependency_tags(draw: st.DrawFn) -> str:
    """Generate dependency tags for testing."""
    return draw(
        st.text(
            min_size=1, max_size=10, alphabet="abcdefghijklmnopqrstuvwxyz0123456789"
        )
    )


@st.composite
def experiment_configs(draw: st.DrawFn) -> tuple[str, Path | None]:
    """Generate experiment name and config path combinations."""
    name = draw(
        st.text(
            min_size=1, max_size=10, alphabet="abcdefghijklmnopqrstuvwxyz0123456789"
        )
    )
    use_path = draw(st.booleans())
    config_path = (
        Path(f"/tmp/{draw(st.text(min_size=1, max_size=5, alphabet='abc'))}.toml")
        if use_path
        else None
    )
    return name, config_path


def _make_dependencies(tag: str) -> bootstrap.CLIDependencies:
    """Create dependencies with a tracking tag for verification."""

    def _load_experiment(experiment: str, exp_config: Path | None) -> SimpleNamespace:
        return SimpleNamespace(tag=tag, experiment=experiment, exp_config=exp_config)

    def _noop_prepare(
        experiment: str,
        cfg: object,
        config_path: Path,
        shared: object,
        engine: object | None,
    ) -> SimpleNamespace:
        return SimpleNamespace(tag=tag, experiment=experiment, cfg=cfg, engine=engine)

    def _noop_train(
        experiment: str,
        cfg: object,
        config_path: Path,
        shared: object,
        engine: object | None,
    ) -> SimpleNamespace:
        return SimpleNamespace(tag=tag, experiment=experiment, cfg=cfg, engine=engine)

    def _noop_sample(
        experiment: str,
        cfg: object,
        config_path: Path,
        shared: object,
        engine: object | None,
    ) -> SimpleNamespace:
        return SimpleNamespace(tag=tag, experiment=experiment, cfg=cfg, engine=engine)

    def _noop_prereq(_: object) -> None:
        return None

    return bootstrap.CLIDependencies(
        load_experiment=_load_experiment,
        ensure_train_prerequisites=_noop_prereq,
        ensure_sample_prerequisites=_noop_prereq,
        run_prepare=_noop_prepare,
        run_train=_noop_train,
        run_sample=_noop_sample,
    )


@given(tag=dependency_tags())
@settings(max_examples=20, deadline=None, derandomize=True)
def test_configure_and_get_dependencies_preserve_tag(tag: str) -> None:
    """Configure dependencies with various tags and verify they're preserved."""
    bootstrap.configure_runtime_cli_dependencies(lambda: _make_dependencies(tag))

    deps = bootstrap.get_runtime_cli_dependencies()
    result = deps.load_experiment("test_exp", None)

    assert result.tag == tag
    assert result.experiment == "test_exp"
    assert result.exp_config is None


@given(tag1=dependency_tags(), tag2=dependency_tags())
@settings(
    max_examples=15,
    deadline=None,
    derandomize=True,
    suppress_health_check=[HealthCheck.filter_too_much],
)
def test_nested_override_context_restores_correctly(tag1: str, tag2: str) -> None:
    """Test that nested context managers restore the correct dependencies."""
    if tag1 == tag2:
        return

    baseline = _make_dependencies("baseline")
    override1 = _make_dependencies(tag1)
    override2 = _make_dependencies(tag2)

    bootstrap.configure_runtime_cli_dependencies(lambda: baseline)

    with bootstrap.override_runtime_cli_dependencies(override1):
        assert bootstrap.get_runtime_cli_dependencies() is override1

        with bootstrap.override_runtime_cli_dependencies(override2):
            assert bootstrap.get_runtime_cli_dependencies() is override2

        assert bootstrap.get_runtime_cli_dependencies() is override1

    assert bootstrap.get_runtime_cli_dependencies() is baseline


@given(tag=dependency_tags(), experiment_config=experiment_configs())
@settings(max_examples=20, deadline=None, derandomize=True)
def test_dependency_functions_receive_correct_parameters(
    tag: str, experiment_config: tuple[str, Path | None]
) -> None:
    """Verify that dependency functions receive and preserve parameters correctly."""
    experiment_name, config_path = experiment_config

    deps = _make_dependencies(tag)
    bootstrap.configure_runtime_cli_dependencies(lambda: deps)

    result = deps.load_experiment(experiment_name, config_path)

    assert result.tag == tag
    assert result.experiment == experiment_name
    assert result.exp_config == config_path


@given(tag=dependency_tags())
@settings(max_examples=10, deadline=None, derandomize=True)
def test_reset_creates_new_instance_with_same_factory(tag: str) -> None:
    """Test that reset creates a fresh instance from the same factory."""
    call_count = 0

    def _factory() -> bootstrap.CLIDependencies:
        nonlocal call_count
        call_count += 1
        return _make_dependencies(tag)

    bootstrap.configure_runtime_cli_dependencies(_factory)

    first = bootstrap.get_runtime_cli_dependencies()
    first_count = call_count

    bootstrap.reset_runtime_cli_dependencies()
    second = bootstrap.get_runtime_cli_dependencies()
    second_count = call_count

    assert first is not second
    assert first.load_experiment("test", None).tag == tag
    assert second.load_experiment("test", None).tag == tag
    assert second_count > first_count


@given(tags=st.lists(dependency_tags(), min_size=1, max_size=5))
@settings(
    max_examples=10,
    deadline=None,
    derandomize=True,
    suppress_health_check=[HealthCheck.filter_too_much],
)
def test_multiple_configure_calls_use_latest_factory(tags: list[str]) -> None:
    """Test that multiple configure calls use the most recent factory."""
    if len(set(tags)) <= 1:
        return

    for i, tag in enumerate(tags):
        bootstrap.configure_runtime_cli_dependencies(lambda: _make_dependencies(tag))
        deps = bootstrap.get_runtime_cli_dependencies()
        result = deps.load_experiment(f"exp_{i}", None)
        assert result.tag == tag


def test_context_manager_exception_restores_dependencies() -> None:
    """Test that exceptions inside context manager still restore dependencies."""
    baseline = _make_dependencies("baseline")
    override = _make_dependencies("override")

    bootstrap.configure_runtime_cli_dependencies(lambda: baseline)

    try:
        with bootstrap.override_runtime_cli_dependencies(override):
            assert bootstrap.get_runtime_cli_dependencies() is override
            raise ValueError("Test exception")
    except ValueError:
        pass

    assert bootstrap.get_runtime_cli_dependencies() is baseline


@given(tag=dependency_tags())
@settings(max_examples=10, deadline=None, derandomize=True)
def test_all_dependency_functions_are_callable(tag: str) -> None:
    """Verify all dependency functions are callable and return expected types."""
    deps = _make_dependencies(tag)

    assert callable(deps.load_experiment)
    assert callable(deps.ensure_train_prerequisites)
    assert callable(deps.ensure_sample_prerequisites)
    assert callable(deps.run_prepare)
    assert callable(deps.run_train)
    assert callable(deps.run_sample)

    exp_result = deps.load_experiment("test", None)
    assert hasattr(exp_result, "tag")

    deps.ensure_train_prerequisites(None)
    deps.ensure_sample_prerequisites(None)

    prepare_result = deps.run_prepare("cmd", None, Path("/tmp"), None, None)
    assert hasattr(prepare_result, "tag")

    train_result = deps.run_train("cmd", None, Path("/tmp"), None, None)
    assert hasattr(train_result, "tag")

    sample_result = deps.run_sample("cmd", None, Path("/tmp"), None, None)
    assert hasattr(sample_result, "tag")
