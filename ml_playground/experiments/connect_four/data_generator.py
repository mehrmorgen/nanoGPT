"""Utilities to create Connect Four self-play datasets."""

from __future__ import annotations

import random
from typing import Iterable, Iterator, List, Sequence

from .game import Board, GameRecord, mirror_move, mirror_state


def play_random_game(rng: random.Random) -> GameRecord:
    board = Board()
    states: List[str] = [board.render()]
    moves: List[int] = []
    current_player = 1

    while True:
        valid = board.valid_moves()
        if not valid:
            return GameRecord(tuple(states), tuple(moves), None)
        column = rng.choice(valid)
        board.drop(column, current_player)
        moves.append(column)
        states.append(board.render())

        winner = board.winner()
        if winner is not None:
            return GameRecord(tuple(states), tuple(moves), winner)
        if board.is_full():
            return GameRecord(tuple(states), tuple(moves), None)

        current_player = 2 if current_player == 1 else 1


def serialize_game(record: GameRecord) -> str:
    lines: List[str] = ["[START]"]
    total_states = len(record.states)
    for idx, state in enumerate(record.states):
        lines.append(state)
        if idx < total_states - 1:
            move = record.moves[idx]
            lines.append(f"[MOVE:{move}]")
    if record.winner is None:
        lines.append("[DRAW]")
    else:
        lines.append(f"[WIN:{record.winner}]")
    return "\n".join(lines)


def mirror_serialized_game(record: GameRecord) -> str:
    mirrored_states = [mirror_state(state) for state in record.states]
    mirrored_moves = [mirror_move(move) for move in record.moves]
    mirrored = GameRecord(tuple(mirrored_states), tuple(mirrored_moves), record.winner)
    return serialize_game(mirrored)


def generate_games(
    num_games: int,
    *,
    seed: int | None = None,
    augment: bool = True,
) -> list[str]:
    rng = random.Random(seed)
    sequences: List[str] = []
    for _ in range(num_games):
        record = play_random_game(rng)
        sequences.append(serialize_game(record))
        if augment:
            sequences.append(mirror_serialized_game(record))
    return sequences


def iter_serialized_games(
    games: Iterable[GameRecord],
    *,
    augment: bool = True,
) -> Iterator[str]:
    for record in games:
        yield serialize_game(record)
        if augment:
            yield mirror_serialized_game(record)


def join_games(games: Sequence[str]) -> str:
    return "\n\n".join(games)
