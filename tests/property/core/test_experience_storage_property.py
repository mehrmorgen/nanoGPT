from __future__ import annotations

from hypothesis import given, settings, strategies as st

from ml_playground.core.experience_storage import (
    ExperienceEntry,
    InMemoryExperienceStorage,
)


@settings(max_examples=25, deadline=50, derandomize=True)
@given(
    moves=st.lists(st.integers(min_value=0, max_value=9), max_size=5),
    winner=st.integers(min_value=-1, max_value=1),
    start_player=st.integers(min_value=-1, max_value=1),
)
def test_experience_entry_hash_stable_and_sensitive(
    moves: list[int],
    winner: int,
    start_player: int,
) -> None:
    """Experience entry hashes are stable and change when moves change."""
    entry = ExperienceEntry(
        moves=tuple(moves),
        winner=winner,
        start_player=start_player,
    )
    first = entry.get_hash()
    second = entry.get_hash()
    assert first == second

    appended = ExperienceEntry(
        moves=tuple(moves) + (max(moves or [0]) + 1,),
        winner=winner,
        start_player=start_player,
    )
    assert appended.get_hash() != first


@settings(max_examples=25, deadline=50, derandomize=True)
@given(
    move_sets=st.lists(
        st.lists(st.integers(min_value=0, max_value=3), max_size=4),
        min_size=1,
        max_size=4,
        unique_by=tuple,
    )
)
def test_inmemory_storage_prioritizes_high_scores(move_sets: list[list[int]]) -> None:
    """In-memory storage returns entries ordered by priority score."""
    storage = InMemoryExperienceStorage()
    entries = []
    for idx, moves in enumerate(move_sets):
        entry = ExperienceEntry(
            moves=tuple(moves),
            winner=1,
            start_player=0,
            priority_score=float(idx),
        )
        storage.store(entry)
        entries.append(entry)

    expected = list(reversed(entries))
    assert storage.get_by_priority(len(entries)) == expected
