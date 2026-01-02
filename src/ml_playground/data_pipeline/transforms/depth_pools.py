from __future__ import annotations

from typing import Callable, Iterable, Mapping, Sequence, TypeVar

T = TypeVar("T")


def partition_by_depth(
    records: Iterable[T], depth_fn: Callable[[T], int]
) -> dict[int, tuple[T, ...]]:
    pools: dict[int, list[T]] = {}
    for record in records:
        depth = depth_fn(record)
        pools.setdefault(depth, []).append(record)
    return {depth: tuple(items) for depth, items in pools.items()}


def normalize_blend_weights(weights: Mapping[int, float]) -> dict[int, float]:
    if not weights:
        raise ValueError("weights must not be empty")
    if any(weight < 0 for weight in weights.values()):
        raise ValueError("weights must be non-negative")
    total = sum(weights.values())
    if total <= 0:
        raise ValueError("weights must sum to a positive value")
    return {depth: weight / total for depth, weight in weights.items()}


def allocate_blend_counts(
    pools: Mapping[int, Sequence[T]],
    weights: Mapping[int, float],
    target_size: int | None = None,
) -> dict[int, int]:
    if set(weights) != set(pools):
        raise ValueError("weights must cover the same depths as pools")

    total_available = sum(len(pool) for pool in pools.values())
    if total_available == 0:
        return {depth: 0 for depth in pools}

    if target_size is None:
        target_size = total_available
    if target_size < 0 or target_size > total_available:
        raise ValueError("target_size must be within available pool size")

    normalized = normalize_blend_weights(weights)
    raw = {depth: normalized[depth] * target_size for depth in pools}
    counts = {
        depth: min(int(raw_count), len(pools[depth]))
        for depth, raw_count in raw.items()
    }
    remainder = target_size - sum(counts.values())
    if remainder == 0:
        return counts

    order = sorted(
        pools,
        key=lambda depth: (-(raw[depth] - int(raw[depth])), depth),
    )
    for depth in order:
        if remainder == 0:
            break
        capacity = len(pools[depth]) - counts[depth]
        if capacity <= 0:
            continue
        add = min(capacity, remainder)
        counts[depth] += add
        remainder -= add

    if remainder != 0:
        raise ValueError("unable to allocate blend counts within pool limits")
    return counts


def blend_pools(
    pools: Mapping[int, Sequence[T]],
    weights: Mapping[int, float] | None = None,
    target_size: int | None = None,
) -> list[T]:
    if not pools:
        return []
    if weights is None:
        weights = {depth: 1.0 for depth in pools}

    counts = allocate_blend_counts(pools, weights, target_size=target_size)
    sequences = {depth: tuple(pools[depth]) for depth in pools}
    indices = {depth: 0 for depth in pools}
    depths = sorted(counts)
    remaining = sum(counts.values())
    blended: list[T] = []
    while remaining > 0:
        for depth in depths:
            if counts[depth] == 0:
                continue
            blended.append(sequences[depth][indices[depth]])
            indices[depth] += 1
            counts[depth] -= 1
            remaining -= 1
            if remaining == 0:
                break
    return blended
