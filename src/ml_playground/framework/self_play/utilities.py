from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Callable, Iterable


class Outcome(str, Enum):
    WIN = "win"
    LOSS = "loss"
    DRAW = "draw"


class ReplayPolicy(str, Enum):
    NONE = "none"
    LOSSES_ONLY = "losses_only"
    ALL = "all"


class OutcomeFilter(str, Enum):
    ALL = "all"
    NO_DRAWS = "no_draws"
    WINS_ONLY = "wins_only"
    LOSSES_ONLY = "losses_only"
    NON_LOSSES = "non_losses"


def should_replay(outcome: Outcome, policy: ReplayPolicy) -> bool:
    if policy is ReplayPolicy.ALL:
        return True
    if policy is ReplayPolicy.NONE:
        return False
    return outcome is Outcome.LOSS


def allows_outcome(outcome: Outcome, outcome_filter: OutcomeFilter) -> bool:
    if outcome_filter is OutcomeFilter.ALL:
        return True
    if outcome_filter is OutcomeFilter.NO_DRAWS:
        return outcome is not Outcome.DRAW
    if outcome_filter is OutcomeFilter.WINS_ONLY:
        return outcome is Outcome.WIN
    if outcome_filter is OutcomeFilter.LOSSES_ONLY:
        return outcome is Outcome.LOSS
    return outcome is not Outcome.LOSS


def filter_outcomes(
    outcomes: Iterable[Outcome], outcome_filter: OutcomeFilter
) -> tuple[Outcome, ...]:
    return tuple(
        outcome for outcome in outcomes if allows_outcome(outcome, outcome_filter)
    )


@dataclass(frozen=True)
class GateCounts:
    wins: int = 0
    losses: int = 0
    draws: int = 0

    @property
    def total(self) -> int:
        return self.wins + self.losses + self.draws

    def record(self, outcome: Outcome) -> "GateCounts":
        if outcome is Outcome.WIN:
            return GateCounts(self.wins + 1, self.losses, self.draws)
        if outcome is Outcome.LOSS:
            return GateCounts(self.wins, self.losses + 1, self.draws)
        return GateCounts(self.wins, self.losses, self.draws + 1)


def accumulate_outcomes(outcomes: Iterable[Outcome]) -> GateCounts:
    counts = GateCounts()
    for outcome in outcomes:
        counts = counts.record(outcome)
    return counts


def gate_metrics(counts: GateCounts) -> dict[str, float]:
    total = counts.total
    if total == 0:
        win_rate = 0.0
        loss_rate = 0.0
        draw_rate = 0.0
    else:
        win_rate = counts.wins / total
        loss_rate = counts.losses / total
        draw_rate = counts.draws / total
    return {
        "wins": float(counts.wins),
        "losses": float(counts.losses),
        "draws": float(counts.draws),
        "total": float(total),
        "win_rate": win_rate,
        "loss_rate": loss_rate,
        "draw_rate": draw_rate,
    }


def emit_gate_metrics(
    counts: GateCounts,
    emit: Callable[[str, float], None],
    prefix: str = "self_play",
) -> None:
    for name, value in gate_metrics(counts).items():
        emit(f"{prefix}.{name}", value)
