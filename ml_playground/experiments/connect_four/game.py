"""Core Connect Four board logic used for data generation and evaluation."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Iterable, List, Sequence

ROWS = 6
COLUMNS = 7
CONNECT = 4


class InvalidMoveError(ValueError):
    """Raised when attempting to place a piece in an invalid column."""


@dataclass
class Board:
    """Represent the Connect Four board state."""

    grid: List[List[int]] = field(
        default_factory=lambda: [[0 for _ in range(COLUMNS)] for _ in range(ROWS)]
    )

    def copy(self) -> "Board":
        return Board([row[:] for row in self.grid])

    def is_valid_move(self, column: int) -> bool:
        return 0 <= column < COLUMNS and self.grid[0][column] == 0

    def valid_moves(self) -> List[int]:
        return [c for c in range(COLUMNS) if self.is_valid_move(c)]

    def drop(self, column: int, player: int) -> None:
        if player not in (1, 2):
            raise InvalidMoveError(f"Invalid player id: {player}")
        if not self.is_valid_move(column):
            raise InvalidMoveError(f"Column {column} is full or out of bounds")
        for row in range(ROWS - 1, -1, -1):
            if self.grid[row][column] == 0:
                self.grid[row][column] = player
                return
        raise InvalidMoveError(f"Column {column} has no available slots")

    def is_full(self) -> bool:
        return all(self.grid[0][col] != 0 for col in range(COLUMNS))

    def winner(self) -> int | None:
        directions = ((0, 1), (1, 0), (1, 1), (1, -1))
        for row in range(ROWS):
            for col in range(COLUMNS):
                player = self.grid[row][col]
                if player == 0:
                    continue
                for d_row, d_col in directions:
                    if self._has_run(row, col, d_row, d_col, player):
                        return player
        return None

    def _has_run(self, row: int, col: int, d_row: int, d_col: int, player: int) -> bool:
        for offset in range(1, CONNECT):
            r = row + d_row * offset
            c = col + d_col * offset
            if r < 0 or r >= ROWS or c < 0 or c >= COLUMNS:
                return False
            if self.grid[r][c] != player:
                return False
        return True

    def render(self) -> str:
        rows: list[str] = []
        for row in self.grid:
            rows.append("|".join(_render_cell(cell) for cell in row))
        return "\n".join(rows)

    def mirrored(self) -> "Board":
        mirrored_grid = [list(reversed(row)) for row in self.grid]
        return Board(mirrored_grid)


def _render_cell(value: int) -> str:
    if value == 1:
        return "1"
    if value == 2:
        return "2"
    return "."


@dataclass
class GameRecord:
    states: Sequence[str]
    moves: Sequence[int]
    winner: int | None

    def is_draw(self) -> bool:
        return self.winner is None

    def mirrored(self) -> "GameRecord":
        mirrored_states = [mirror_state(s) for s in self.states]
        mirrored_moves = [mirror_move(m) for m in self.moves]
        return GameRecord(mirrored_states, mirrored_moves, self.winner)


def mirror_state(state: str) -> str:
    rows = [row.split("|") for row in state.splitlines()]
    mirrored_rows = ["|".join(reversed(row)) for row in rows]
    return "\n".join(mirrored_rows)


def mirror_move(column: int) -> int:
    return COLUMNS - 1 - column


def replay_moves(moves: Iterable[int]) -> GameRecord:
    board = Board()
    states: List[str] = [board.render()]
    history: List[int] = []
    player = 1
    for move in moves:
        history.append(move)
        board.drop(move, player)
        states.append(board.render())
        winner = board.winner()
        if winner is not None:
            return GameRecord(tuple(states), tuple(history), winner)
        if board.is_full():
            return GameRecord(tuple(states), tuple(history), None)
        player = 2 if player == 1 else 1
    return GameRecord(tuple(states), tuple(history), board.winner())
