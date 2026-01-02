from __future__ import annotations

from hypothesis import given, settings, strategies as st

from ml_playground.self_play.utilities import (
    Outcome,
    OutcomeFilter,
    allows_outcome,
    filter_outcomes,
)


@settings(max_examples=50, deadline=50, derandomize=True)
@given(
    outcomes=st.lists(st.sampled_from(list(Outcome)), max_size=30),
    outcome_filter=st.sampled_from(list(OutcomeFilter)),
)
def test_filter_outcomes_respects_filter(
    outcomes: list[Outcome], outcome_filter: OutcomeFilter
) -> None:
    """Outcome filters never allow disallowed outcomes."""
    filtered = filter_outcomes(outcomes, outcome_filter)
    assert all(allows_outcome(outcome, outcome_filter) for outcome in filtered)

    if outcome_filter is OutcomeFilter.ALL:
        assert filtered == tuple(outcomes)
    elif outcome_filter is OutcomeFilter.WINS_ONLY:
        assert all(outcome is Outcome.WIN for outcome in filtered)
    elif outcome_filter is OutcomeFilter.LOSSES_ONLY:
        assert all(outcome is Outcome.LOSS for outcome in filtered)
    elif outcome_filter is OutcomeFilter.NO_DRAWS:
        assert all(outcome is not Outcome.DRAW for outcome in filtered)
    else:
        assert all(outcome is not Outcome.LOSS for outcome in filtered)
