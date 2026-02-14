from __future__ import annotations

from hypothesis import given, settings, strategies as st

from ml_playground.framework.self_play.utilities import (
    Outcome,
    ReplayPolicy,
    should_replay,
)


@settings(max_examples=50, deadline=None, derandomize=True)
@given(  # type: ignore[reportAny]
    outcome=st.sampled_from(list(Outcome)),
    policy=st.sampled_from(list(ReplayPolicy)),
)
def test_should_replay_policy_invariants(
    outcome: Outcome, policy: ReplayPolicy
) -> None:
    """Replay policy invariants hold for every outcome/policy pair."""
    expected = (
        True
        if policy is ReplayPolicy.ALL
        else False
        if policy is ReplayPolicy.NONE
        else outcome is Outcome.LOSS
    )
    assert should_replay(outcome, policy) is expected
