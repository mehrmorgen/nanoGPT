from __future__ import annotations

import string

import pytest
from hypothesis import given, settings, strategies as st

from ml_playground.framework.analysis.metrics_registry import (
    is_valid_metric_name,
    validate_metric_name,
)


def _segment_strategy() -> st.SearchStrategy[str]:
    alphabet = string.ascii_lowercase + string.digits + "_"
    return st.text(alphabet=alphabet, min_size=1, max_size=8).filter(
        lambda segment: segment[0] in string.ascii_lowercase
    )


def _valid_metric_name_strategy() -> st.SearchStrategy[str]:
    return st.lists(_segment_strategy(), min_size=1, max_size=4).map(".".join)


def _invalid_metric_name_strategy() -> st.SearchStrategy[str]:
    base = st.text(
        alphabet=string.ascii_lowercase + string.digits + "_",
        min_size=1,
        max_size=5,
    )
    return st.one_of(
        base.map(lambda s: f".{s}"),
        base.map(lambda s: f"{s}."),
        base.map(lambda s: f"{s}..{s}"),
        st.text(alphabet=string.digits, min_size=1, max_size=5),
        st.text(alphabet=string.ascii_uppercase, min_size=1, max_size=5),
        st.text(alphabet=string.ascii_lowercase, min_size=1, max_size=5).map(
            lambda s: f"{s}-metric"
        ),
    )


@settings(max_examples=50, deadline=None, derandomize=True)
@given(  # type: ignore[reportAny]
    name=_valid_metric_name_strategy()
)
def test_metric_names_accept_valid_convention(name: str) -> None:
    """Valid names meet the naming convention."""
    assert is_valid_metric_name(name)
    validate_metric_name(name)


@settings(max_examples=50, deadline=None, derandomize=True)
@given(  # type: ignore[reportAny]
    name=_invalid_metric_name_strategy()
)
def test_metric_names_reject_invalid_convention(name: str) -> None:
    """Invalid names fail validation."""
    assert not is_valid_metric_name(name)
    with pytest.raises(ValueError):
        validate_metric_name(name)
