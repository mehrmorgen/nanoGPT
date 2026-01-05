from __future__ import annotations

from pathlib import Path

import pytest

from ml_playground.core.error_handling import DataError
from ml_playground.data_pipeline.transforms.io import (
    coerce_seed_policy,
    seed_text_file_with_policy,
)


def test_coerce_seed_policy_returns_auto_on_none() -> None:
    assert coerce_seed_policy(None) == "auto"


def test_coerce_seed_policy_raises_on_unknown_value() -> None:
    with pytest.raises(DataError, match="Unknown seed policy"):
        coerce_seed_policy("invalid")


def test_seed_text_file_with_policy_fail_fast(tmp_path: Path) -> None:
    """Fail-fast policy should raise when destination missing and candidates absent."""
    dst = tmp_path / "dst.txt"
    with pytest.raises(FileNotFoundError, match="required file missing"):
        seed_text_file_with_policy(dst, [tmp_path / "missing.txt"], policy="fail_fast")
