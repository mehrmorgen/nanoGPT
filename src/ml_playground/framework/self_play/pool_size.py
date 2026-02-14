from __future__ import annotations

import math


def derive_pool_size(
    target_labeled_positions: int,
    avg_positions_per_game: int,
    oversample_factor: float = 1.0,
) -> int:
    if target_labeled_positions < 0:
        raise ValueError("target_labeled_positions must be >= 0")
    if avg_positions_per_game <= 0:
        raise ValueError("avg_positions_per_game must be > 0")
    if oversample_factor <= 0:
        raise ValueError("oversample_factor must be > 0")
    if target_labeled_positions == 0:
        return 0
    return int(
        math.ceil(
            (target_labeled_positions / avg_positions_per_game) * oversample_factor
        )
    )
