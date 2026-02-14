from __future__ import annotations

from dataclasses import dataclass
import math


def wilson_interval(
    successes: int, trials: int, z: float = 1.96
) -> tuple[float, float]:
    if trials <= 0:
        raise ValueError("trials must be positive")
    if successes < 0 or successes > trials:
        raise ValueError("successes must be within [0, trials]")
    if z <= 0:
        raise ValueError("z must be positive")

    p = successes / trials
    z2 = z * z
    denom = 1.0 + z2 / trials
    center = (p + z2 / (2 * trials)) / denom
    margin = (z / denom) * math.sqrt((p * (1 - p) + z2 / (4 * trials)) / trials)
    lower = max(0.0, center - margin)
    upper = min(1.0, center + margin)
    return lower, upper


@dataclass(frozen=True)
class GateCriteria:
    min_games: int = 1
    promote_threshold: float = 0.55
    stop_threshold: float = 0.45

    def __post_init__(self) -> None:
        if self.min_games < 1:
            raise ValueError("min_games must be >= 1")
        if not 0.0 <= self.promote_threshold <= 1.0:
            raise ValueError("promote_threshold must be within [0, 1]")
        if not 0.0 <= self.stop_threshold <= 1.0:
            raise ValueError("stop_threshold must be within [0, 1]")


@dataclass(frozen=True)
class GateDecision:
    promote: bool
    stop: bool
    lower_bound: float
    upper_bound: float


def should_promote(
    successes: int,
    trials: int,
    min_games: int,
    threshold: float,
    z: float = 1.96,
) -> bool:
    if trials < min_games:
        return False
    lower, _ = wilson_interval(successes, trials, z=z)
    return lower >= threshold


def should_stop(
    successes: int,
    trials: int,
    min_games: int,
    threshold: float,
    z: float = 1.96,
) -> bool:
    if trials < min_games:
        return False
    _, upper = wilson_interval(successes, trials, z=z)
    return upper < threshold


def evaluate_gate(
    successes: int,
    trials: int,
    criteria: GateCriteria,
    z: float = 1.96,
) -> GateDecision:
    lower, upper = wilson_interval(successes, trials, z=z)
    promote = should_promote(
        successes, trials, criteria.min_games, criteria.promote_threshold, z=z
    )
    stop = should_stop(
        successes, trials, criteria.min_games, criteria.stop_threshold, z=z
    )
    return GateDecision(
        promote=promote, stop=stop, lower_bound=lower, upper_bound=upper
    )
