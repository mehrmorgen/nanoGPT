from __future__ import annotations

from pathlib import Path
from tempfile import TemporaryDirectory

import pytest
from hypothesis import given, settings, strategies as st

from ml_playground.configuration.models import TrainerConfig


@settings(max_examples=20, deadline=50, derandomize=True)
@given(
    base_dir=st.text(min_size=1, max_size=10),
    best=st.text(min_size=1, max_size=8),
    last=st.text(min_size=1, max_size=8),
    final=st.text(min_size=1, max_size=8),
)
def test_adapter_output_policy_when_unique_then_resolves_paths(
    base_dir: str,
    best: str,
    last: str,
    final: str,
) -> None:
    """Adapter output policy when unique then resolves paths."""
    if len({best, last, final}) != 3:
        return
    policy = TrainerConfig.PeftConfig.AdapterOutputPolicy(
        base_dir=base_dir,
        best_name=best,
        last_name=last,
        final_name=final,
    )
    with TemporaryDirectory() as tmp_dir:
        out_dir = Path(tmp_dir)
        paths = policy.resolve(out_dir)
        assert paths["best"] == out_dir / base_dir / best
        assert paths["last"] == out_dir / base_dir / last
        assert paths["final"] == out_dir / base_dir / final


def test_adapter_output_policy_when_duplicates_then_raises() -> None:
    """Adapter output policy when duplicates then raises."""
    with pytest.raises(ValueError):
        TrainerConfig.PeftConfig.AdapterOutputPolicy(
            best_name="same",
            last_name="same",
            final_name="final",
        )


def test_adapter_output_policy_when_base_dir_empty_then_raises() -> None:
    """Adapter output policy when base dir empty then raises."""
    with pytest.raises(ValueError):
        TrainerConfig.PeftConfig.AdapterOutputPolicy(base_dir="")


def test_adapter_output_policy_when_name_empty_then_raises() -> None:
    """Adapter output policy when name empty then raises."""
    with pytest.raises(ValueError):
        TrainerConfig.PeftConfig.AdapterOutputPolicy(best_name="")
