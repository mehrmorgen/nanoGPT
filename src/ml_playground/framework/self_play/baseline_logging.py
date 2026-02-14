from __future__ import annotations

from typing import Callable, Mapping


def depth_metric_items(
    metrics: Mapping[str, float],
    depth: int,
    prefix: str = "self_play.depth",
) -> list[tuple[str, float]]:
    return [(f"{prefix}.{depth}.{name}", value) for name, value in metrics.items()]


def baseline_metric_items(
    current_metrics: Mapping[str, float],
    baseline_metrics: Mapping[str, float],
    *,
    current_depth: int,
    baseline_depth: int,
    include_baseline: bool,
    prefix: str = "self_play.depth",
) -> list[tuple[str, float]]:
    items = depth_metric_items(current_metrics, current_depth, prefix=prefix)
    if include_baseline:
        items.extend(
            depth_metric_items(baseline_metrics, baseline_depth, prefix=prefix)
        )
    return items


def emit_baseline_metrics(
    emit: Callable[[str, float], None],
    current_metrics: Mapping[str, float],
    baseline_metrics: Mapping[str, float],
    *,
    current_depth: int,
    baseline_depth: int,
    include_baseline: bool,
    prefix: str = "self_play.depth",
) -> None:
    for name, value in baseline_metric_items(
        current_metrics,
        baseline_metrics,
        current_depth=current_depth,
        baseline_depth=baseline_depth,
        include_baseline=include_baseline,
        prefix=prefix,
    ):
        emit(name, value)
