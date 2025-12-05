from __future__ import annotations

import numpy as np
from numpy.typing import NDArray


class VierGewinnt:
    def __init__(self, rows: int = 6, cols: int = 7) -> None:
        self.rows = rows
        self.cols = cols
        self.board: NDArray[np.int_] = np.zeros((rows, cols), dtype=int)
        self.current_player = 1
        self.move_history: list[int] = []

    def make_move(self, col: int) -> int:
        if col < 0 or col >= self.cols or self.board[0, col] != 0:
            raise ValueError("Invalid move")

        for r in range(self.rows - 1, -1, -1):
            if self.board[r, col] == 0:
                self.board[r, col] = self.current_player
                break

        self.move_history.append(col)

        if self.check_win(self.current_player):
            return self.current_player

        self.current_player = 3 - self.current_player
        return 0

    def check_win(self, player: int) -> bool:
        for r in range(self.rows):
            for c in range(self.cols - 3):
                if all(self.board[r, c + i] == player for i in range(4)):
                    return True

        for r in range(self.rows - 3):
            for c in range(self.cols):
                if all(self.board[r + i, c] == player for i in range(4)):
                    return True

        for r in range(self.rows - 3):
            for c in range(self.cols - 3):
                if all(self.board[r + i, c + i] == player for i in range(4)):
                    return True

        for r in range(3, self.rows):
            for c in range(self.cols - 3):
                if all(self.board[r - i, c + i] == player for i in range(4)):
                    return True

        return False

    def is_full(self) -> bool:
        return not any(self.board[0, :] == 0)

    def get_valid_moves(self) -> list[int]:
        return [c for c in range(self.cols) if self.board[0, c] == 0]

    def to_string(self) -> str:
        return np.array2string(self.board)

    def reset(self) -> None:
        self.board = np.zeros((self.rows, self.cols), dtype=int)
        self.current_player = 1
        self.move_history = []
