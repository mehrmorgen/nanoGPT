from __future__ import annotations

from pathlib import Path

import hypothesis.strategies as st
from hypothesis import given, settings

from ml_playground.tools.core.config import ToolsConfig
from ml_playground.tools.core.runtime import ToolsCLIState, load_config_with_error_handling, reset_state, set_config


@settings(max_examples=50, deadline=None, derandomize=True)
@given(
    learning=st.booleans(),
    verbosity=st.integers(min_value=0, max_value=5),
    dry_run=st.booleans(),
)
def test_state_reset_clears_fields(learning: bool, verbosity: int, dry_run: bool) -> None:
    """reset_state should return the CLI state to default values."""
    state = ToolsCLIState()
    state.learning_mode = learning
    state.verbosity = verbosity
    state.dry_run = dry_run
    state.project_root = None
    state.config = ToolsConfig()
    if learning:
        state.mark_learning_mode_explicit(True)
    else:
        state.mark_learning_mode_default(True)

    state.reset()

    assert state.learning_mode is False
    assert state.verbosity == 1
    assert state.dry_run is False
    assert state.project_root is None
    assert state.config is None
    assert state.learning_mode_set is False


@settings(max_examples=25, deadline=None, derandomize=True)
@given(
    learning_flag=st.booleans(),
)
def test_set_and_reset_config_roundtrip(learning_flag: bool) -> None:
    """set_config followed by reset_state should clear configuration references."""
    cfg = ToolsConfig(learning_mode_default=learning_flag, default_verbosity=2)
    set_config(cfg)

    reset_state()

    from ml_playground.tools.core.runtime import state

    assert state.config is None
    assert state.project_root is None


@settings(max_examples=10, deadline=None, derandomize=True)
@given(st.just(None))
def test_load_config_injects_defaults(_: None) -> None:
    """load_config_with_error_handling should populate state with defaults from config."""
    import ml_playground.tools.core.runtime as runtime_mod

    reset_state()

    cfg = ToolsConfig(learning_mode_default=True, default_verbosity=2)
    original_loader = runtime_mod.load_tools_config

    def _patched_loader(_root: Path | None = None) -> ToolsConfig:
        return cfg

    runtime_mod.load_tools_config = _patched_loader

    try:
        load_config_with_error_handling(None)
        assert runtime_mod.state.config is cfg
        assert runtime_mod.state.learning_mode is True
        assert runtime_mod.state.learning_mode_set is True
        assert runtime_mod.state.verbosity == 2
    finally:
        runtime_mod.load_tools_config = original_loader
        reset_state()
