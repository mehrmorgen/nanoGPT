"""Core Connect Four game logic shared across preparer and sampler."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Iterator, List

from ml_playground.core.error_handling import DataError


@dataclass(frozen=True)
class MoveRecord:
    """Serialized representation of a board state and the chosen column."""

    board_state: str
    column: int


class ConnectFourGame:
    """Stateful Connect Four environment for data generation and evaluation."""

    ROWS: int = 6
    COLS: int = 7
    CONNECT: int = 4

    _TOKEN_MAP = {0: ".", 1: "X", 2: "O"}

    def __init__(self) -> None:
        self._board: List[List[int]] = [
            [0 for _ in range(self.COLS)] for _ in range(self.ROWS)
        ]
        self._current_player: int = 1
        self._moves_played: int = 0
        self.winner: int | None = None

    # ------------------------------------------------------------------
    # Board representation helpers
    # ------------------------------------------------------------------
    @property
    def current_player(self) -> int:
        return self._current_player

    @property
    def current_player_token(self) -> str:
        return self._TOKEN_MAP[self._current_player]

    def board_to_string(self) -> str:
        return "".join(
            self._TOKEN_MAP[self._board[row][col]]
            for row in range(self.ROWS)
            for col in range(self.COLS)
        )

    def render(self) -> str:
        rows = []
        for row in range(self.ROWS):
            rows.append(
                " ".join(self._TOKEN_MAP[self._board[row][col]] for col in range(self.COLS))
            )
        footer = "0 1 2 3 4 5 6"
        return "\n".join(rows + [footer])

    # ------------------------------------------------------------------
    # Game mechanics
    # ------------------------------------------------------------------
    def reset(self) -> None:
        for row in range(self.ROWS):
            for col in range(self.COLS):
                self._board[row][col] = 0
        self._current_player = 1
        self._moves_played = 0
        self.winner = None

    def valid_moves(self) -> list[int]:
        return [c for c in range(self.COLS) if self._board[0][c] == 0]

    def is_full(self) -> bool:
        return self._moves_played >= self.ROWS * self.COLS

    def is_over(self) -> bool:
        return self.winner is not None or self.is_full()

    def _drop_piece(self, column: int) -> int:
        if column < 0 or column >= self.COLS:
            raise DataError(f"Column {column} is out of bounds")
        for row in range(self.ROWS - 1, -1, -1):
            if self._board[row][column] == 0:
                self._board[row][column] = self._current_player
                self._moves_played += 1
                return row
        raise DataError(f"Column {column} is full")

    def _check_winner(self, row: int, column: int) -> bool:
        player = self._board[row][column]
        directions = ((1, 0), (0, 1), (1, 1), (1, -1))
        for dr, dc in directions:
            count = 1
            for step in (1, -1):
                r, c = row + dr * step, column + dc * step
                while 0 <= r < self.ROWS and 0 <= c < self.COLS:
                    if self._board[r][c] != player:
                        break
                    count += 1
                    if count >= self.CONNECT:
                        return True
                    r += dr * step
                    c += dc * step
        return False

    def _switch_player(self) -> None:
        self._current_player = 2 if self._current_player == 1 else 1

    def make_move(self, column: int) -> bool:
        """Apply a move for the current player.

        Returns True if the move wins the game. Winner is stored in ``self.winner``.
        """

        if self.is_over():
            raise DataError("Game already finished")
        row = self._drop_piece(column)
        if self._check_winner(row, column):
            self.winner = self._current_player
            return True
        if self.is_full():
            self.winner = 0  # draw
            return False
        self._switch_player()
        return False

    # ------------------------------------------------------------------
    # Data generation utilities
    # ------------------------------------------------------------------
    def play_random_game(self, rng) -> list[MoveRecord]:
        """Simulate a random game yielding board/move pairs."""

        records: list[MoveRecord] = []
        while not self.is_over():
            moves = self.valid_moves()
            if not moves:
                break
            move = rng.choice(moves)
            records.append(MoveRecord(self.board_to_string(), move))
            self.make_move(move)
        return records

    def iter_states(self, moves: Iterable[int]) -> Iterator[MoveRecord]:
        """Replay a sequence of moves yielding board/move pairs."""

        for move in moves:
            yield MoveRecord(self.board_to_string(), move)
            self.make_move(move)
            if self.is_over():
                break
