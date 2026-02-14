from __future__ import annotations

from pathlib import Path
from tempfile import TemporaryDirectory

import pytest
from hypothesis import HealthCheck, assume, given, settings, strategies as st
from pydantic import ValidationError

from ml_playground.framework.configuration.models import RuntimeConfig


@settings(
    max_examples=30,
    deadline=None,
    derandomize=True,
    suppress_health_check=[HealthCheck.function_scoped_fixture],
)
@given(  # type: ignore[reportAny]
    max_games=st.integers(min_value=0, max_value=10_000),
    eval_interval_games=st.integers(min_value=1, max_value=1_000),
    eval_games=st.integers(min_value=1, max_value=1_000),
    log_interval_games=st.integers(min_value=1, max_value=1_000),
)
def test_runtime_config_accepts_domain_counters_when_consistent(
    max_games: int,
    eval_interval_games: int,
    eval_games: int,
    log_interval_games: int,
) -> None:
    """Runtime config accepts domain counters with valid intervals."""
    assume(log_interval_games <= eval_interval_games)
    with TemporaryDirectory() as tmp_dir:
        runtime = RuntimeConfig(
            out_dir=Path(tmp_dir),
            max_games=max_games,
            eval_interval_games=eval_interval_games,
            eval_games=eval_games,
            log_interval_games=log_interval_games,
        )
        assert runtime.eval_interval_games == eval_interval_games
        assert runtime.eval_games == eval_games
        assert runtime.log_interval_games == log_interval_games
        assert runtime.total_eval_games == max_games // eval_interval_games


@settings(
    max_examples=30,
    deadline=None,
    derandomize=True,
    suppress_health_check=[HealthCheck.function_scoped_fixture],
)
@given(  # type: ignore[reportAny]
    eval_interval_games=st.integers(min_value=1, max_value=1_000),
    log_interval_games=st.integers(min_value=1, max_value=1_000),
)
def test_runtime_config_rejects_invalid_domain_intervals(
    eval_interval_games: int, log_interval_games: int
) -> None:
    """Runtime config rejects invalid domain intervals."""
    assume(log_interval_games > eval_interval_games)
    with TemporaryDirectory() as tmp_dir:
        with pytest.raises(ValidationError):
            RuntimeConfig(
                out_dir=Path(tmp_dir),
                eval_interval_games=eval_interval_games,
                log_interval_games=log_interval_games,
            )
