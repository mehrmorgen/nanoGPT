"""Unit tests for the Connect Four board mechanics."""

from __future__ import annotations

import pytest

from ml_playground.experiments.connect_four.game import (
    Board,
    InvalidMoveError,
    mirror_move,
    mirror_state,
)


def test_drop_and_valid_moves() -> None:
    board = Board()
    assert board.valid_moves() == list(range(7))

    board.drop(0, 1)
    board.drop(0, 2)
    assert board.grid[5][0] == 1
    assert board.grid[4][0] == 2
    assert board.is_valid_move(0)

    for _ in range(6):
        board.drop(1, 1)
    assert not board.is_valid_move(1)

    with pytest.raises(InvalidMoveError):
        board.drop(1, 2)
    with pytest.raises(InvalidMoveError):
        board.drop(7, 1)


def test_winner_detection_vertical_horizontal_diagonal() -> None:
    vertical = Board()
    for _ in range(4):
        vertical.drop(0, 1)
    assert vertical.winner() == 1

    horizontal = Board()
    for col in range(4):
        horizontal.drop(col, 2)
    assert horizontal.winner() == 2

    diagonal = Board()
    diagonal.drop(0, 1)
    diagonal.drop(1, 2)
    diagonal.drop(1, 1)
    diagonal.drop(2, 2)
    diagonal.drop(2, 2)
    diagonal.drop(2, 1)
    diagonal.drop(3, 2)
    diagonal.drop(3, 2)
    diagonal.drop(3, 2)
    diagonal.drop(3, 1)
    assert diagonal.winner() == 1


def test_mirror_helpers() -> None:
    board = Board()
    board.drop(0, 1)
    board.drop(6, 2)
    original = board.render()
    mirrored = board.mirrored().render()

    assert mirror_state(original) == mirrored
    assert mirror_move(0) == 6
    assert mirror_move(6) == 0
