from __future__ import annotations

from pathlib import Path
from tempfile import TemporaryDirectory

import pytest
from hypothesis import HealthCheck, given, settings, strategies as st

from ml_playground.core.error_handling import DataError
from ml_playground.data_pipeline.transforms.io import (
    coerce_seed_policy,
    seed_text_file_with_policy,
)


@settings(
    max_examples=10,
    deadline=50,
    derandomize=True,
    suppress_health_check=[HealthCheck.function_scoped_fixture],
)
@given(payload=st.binary(min_size=1, max_size=32))
def test_seed_text_file_with_policy_when_auto_then_seeds(payload: bytes) -> None:
    """Seed text file with policy when auto then seeds."""
    with TemporaryDirectory() as tmp_dir:
        base = Path(tmp_dir)
        dst = base / "input.txt"
        src = base / "seed.txt"
        src.write_bytes(payload)

        seed_text_file_with_policy(dst, [src], policy="auto")

        assert dst.exists()
        assert dst.read_bytes() == payload


@settings(
    max_examples=10,
    deadline=50,
    derandomize=True,
    suppress_health_check=[HealthCheck.function_scoped_fixture],
)
@given(name=st.text(min_size=1, max_size=10))
def test_seed_text_file_with_policy_when_fail_fast_then_raises(name: str) -> None:
    """Seed text file with policy when fail fast then raises."""
    with TemporaryDirectory() as tmp_dir:
        base = Path(tmp_dir)
        dst = base / f"{name}.txt"
        with pytest.raises(FileNotFoundError):
            seed_text_file_with_policy(
                dst,
                [base / "missing.txt"],
                policy="fail_fast",
            )


@settings(
    max_examples=10,
    deadline=50,
    derandomize=True,
    suppress_health_check=[HealthCheck.function_scoped_fixture],
)
@given(policy=st.text(min_size=1, max_size=10))
def test_seed_text_file_with_policy_when_unknown_then_raises(policy: str) -> None:
    """Seed text file with policy when unknown then raises."""
    if policy in {"auto", "fail_fast"}:
        return
    with TemporaryDirectory() as tmp_dir:
        with pytest.raises(DataError):
            seed_text_file_with_policy(Path(tmp_dir) / "input.txt", [], policy=policy)


@settings(max_examples=10, deadline=50, derandomize=True)
@given(policy=st.sampled_from(["auto", "fail_fast"]))
def test_coerce_seed_policy_when_known_then_returns(policy: str) -> None:
    """Coerce seed policy when known then returns."""
    assert coerce_seed_policy(policy) == policy


def test_coerce_seed_policy_when_none_then_defaults() -> None:
    """Coerce seed policy when none then defaults."""
    assert coerce_seed_policy(None) == "auto"
