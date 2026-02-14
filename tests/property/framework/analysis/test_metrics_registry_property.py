from __future__ import annotations

import string

import pytest
from hypothesis import given, settings, strategies as st

from ml_playground.framework.analysis.metrics_registry import (
    MetricKind,
    MetricSpec,
    MetricsRegistry,
    register_all,
)


def _metric_specs_strategy() -> st.SearchStrategy[list[MetricSpec]]:
    alphabet = string.ascii_lowercase + string.digits + "_"
    segment = st.text(alphabet=alphabet, min_size=1, max_size=8).filter(
        lambda value: value[0] in string.ascii_lowercase
    )
    name = st.lists(segment, min_size=1, max_size=3).map(".".join)
    description = st.text(
        alphabet=string.ascii_lowercase + " ", min_size=1, max_size=30
    )
    kind = st.sampled_from(list(MetricKind))
    unit = st.one_of(
        st.none(),
        st.text(alphabet=string.ascii_lowercase, min_size=1, max_size=8),
    )
    return st.lists(
        st.tuples(name, description, kind, unit),
        min_size=1,
        max_size=6,
        unique_by=lambda item: item[0],
    ).map(
        lambda rows: [
            MetricSpec(name=row[0], description=row[1], kind=row[2], unit=row[3])
            for row in rows
        ]
    )


@settings(max_examples=30, deadline=None, derandomize=True)
@given(  # type: ignore[reportAny]
    specs=_metric_specs_strategy()
)
def test_metrics_registry_round_trips_and_sorts(specs: list[MetricSpec]) -> None:
    """Registry returns stable, sorted registrations."""
    registry = MetricsRegistry()
    register_all(registry, specs)
    sorted_specs = tuple(sorted(specs, key=lambda spec: spec.name))
    assert registry.fetch_all_specs() == sorted_specs
    for spec in specs:
        assert registry.get(spec.name) == spec

    markdown = registry.to_markdown()
    for spec in specs:
        assert spec.name in markdown

    dashboard = registry.to_dashboard_spec()
    assert "metrics" in dashboard


@settings(max_examples=30, deadline=None, derandomize=True)
@given(  # type: ignore[reportAny]
    name=st.text(alphabet=string.ascii_lowercase, min_size=1, max_size=8),
    description=st.text(alphabet=string.ascii_lowercase + " ", min_size=1, max_size=20),
)
def test_metrics_registry_rejects_duplicates(name: str, description: str) -> None:
    """Registering duplicate names fails."""
    registry = MetricsRegistry()
    registry.register_metric(name=name, description=description)
    with pytest.raises(ValueError):
        registry.register_metric(name=name, description=description)
