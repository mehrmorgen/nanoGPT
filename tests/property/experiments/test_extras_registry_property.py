from __future__ import annotations

from hypothesis import given, settings
from hypothesis import strategies as st
from pydantic import BaseModel

from ml_playground.framework.experiment_registry import extras_registry


class _DummyModel(BaseModel):
    value: int = 0


@given(  # type: ignore[reportAny]
    experiment=st.text(min_size=1, max_size=12),
    section=st.text(min_size=1, max_size=12),
)
@settings(max_examples=20, deadline=None, derandomize=True)
def test_register_and_get_extras_model_round_trip(
    experiment: str, section: str
) -> None:
    experiment = f"pbt_{experiment}"
    extras_registry.EXTRAS_MODELS.pop(experiment, None)
    extras_registry.LOADED_EXPERIMENTS.discard(experiment)
    try:
        extras_registry.register_extras_model(experiment, section, _DummyModel)
        assert extras_registry.get_extras_model(experiment, section) is _DummyModel
    finally:
        extras_registry.EXTRAS_MODELS.pop(experiment, None)
        extras_registry.LOADED_EXPERIMENTS.discard(experiment)
