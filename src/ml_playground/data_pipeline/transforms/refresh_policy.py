from __future__ import annotations

from ml_playground.configuration.models import BinRefreshPolicy


def should_refresh_bins(
    *,
    previous_tokens: int,
    new_tokens: int,
    policy: BinRefreshPolicy,
) -> bool:
    if new_tokens < 0 or previous_tokens < 0:
        raise ValueError("token counts must be non-negative")
    if new_tokens < policy.min_new_tokens:
        return False
    if previous_tokens == 0:
        return True
    ratio = new_tokens / previous_tokens
    return ratio >= policy.min_new_ratio
