from __future__ import annotations

import pytest

from ml_playground.self_play.pool_size import derive_pool_size


def test_derive_pool_size_raises_on_negative_target() -> None:
    with pytest.raises(ValueError, match="target_labeled_positions must be >= 0"):
        derive_pool_size(target_labeled_positions=-1, avg_positions_per_game=10)


def test_derive_pool_size_raises_on_non_positive_avg_positions() -> None:
    with pytest.raises(ValueError, match="avg_positions_per_game must be > 0"):
        derive_pool_size(target_labeled_positions=100, avg_positions_per_game=0)


def test_derive_pool_size_raises_on_non_positive_oversample() -> None:
    with pytest.raises(ValueError, match="oversample_factor must be > 0"):
        derive_pool_size(
            target_labeled_positions=100,
            avg_positions_per_game=10,
            oversample_factor=0.0,
        )


def test_derive_pool_size_returns_zero_when_target_is_zero() -> None:
    assert derive_pool_size(target_labeled_positions=0, avg_positions_per_game=10) == 0
