"""Tests for Connect Four data generation helpers."""

from __future__ import annotations

import random

from ml_playground.experiments.connect_four import data_generator as dg
from ml_playground.experiments.connect_four.game import mirror_state


def test_serialize_game_structure() -> None:
    rng = random.Random(123)
    record = dg.play_random_game(rng)
    text = dg.serialize_game(record)
    lines = text.splitlines()

    assert lines[0] == "[START]"
    assert lines[-1] in {"[DRAW]", "[WIN:1]", "[WIN:2]"}
    assert any(line.startswith("[MOVE:") for line in lines)
    assert len(lines) >= 3


def test_mirror_serialization_reflects_board() -> None:
    rng = random.Random(5)
    record = dg.play_random_game(rng)
    original = dg.serialize_game(record).splitlines()
    mirrored = dg.mirror_serialized_game(record).splitlines()

    assert original[0] == mirrored[0]
    assert original[-1] == mirrored[-1]
    assert mirror_state(original[1]) == mirrored[1]

    original_moves = [line for line in original if line.startswith("[MOVE:")]
    mirrored_moves = [line for line in mirrored if line.startswith("[MOVE:")]
    assert len(original_moves) == len(mirrored_moves)
    for left, right in zip(original_moves, mirrored_moves):
        move_col = int(left[6:-1])
        mirrored_col = int(right[6:-1])
        assert move_col + mirrored_col == 6


def test_generate_games_with_and_without_augmentation() -> None:
    games_no_aug = dg.generate_games(3, seed=42, augment=False)
    games_aug = dg.generate_games(2, seed=42, augment=True)

    assert len(games_no_aug) == 3
    assert len(games_aug) == 4  # 2 originals + 2 mirrored
    assert all(game.startswith("[START]") for game in games_no_aug)
    assert any("[WIN:" in game for game in games_aug)
