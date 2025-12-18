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

    def check_win_from_position(self, row: int, col: int, player: int) -> bool:
        """Check if placing a piece at (row, col) creates a win for player.
        This is optimized to only check the 4 directions around the last move."""
        # Horizontal
        count = 1
        # Check left
        for c in range(col - 1, -1, -1):
            if self.board[row, c] == player:
                count += 1
            else:
                break
        # Check right
        for c in range(col + 1, self.cols):
            if self.board[row, c] == player:
                count += 1
            else:
                break
        if count >= 4:
            return True

        # Vertical
        count = 1
        # Check down
        for r in range(row - 1, -1, -1):
            if self.board[r, col] == player:
                count += 1
            else:
                break
        # Check up
        for r in range(row + 1, self.rows):
            if self.board[r, col] == player:
                count += 1
            else:
                break
        if count >= 4:
            return True

        # Diagonal (down-right)
        count = 1
        # Check down-left
        for i in range(1, min(row, col) + 1):
            if self.board[row - i, col - i] == player:
                count += 1
            else:
                break
        # Check up-right
        for i in range(1, min(self.rows - row, self.cols - col)):
            if self.board[row + i, col + i] == player:
                count += 1
            else:
                break
        if count >= 4:
            return True

        # Diagonal (up-right)
        count = 1
        # Check up-left
        for i in range(1, min(self.rows - row - 1, col) + 1):
            if self.board[row + i, col - i] == player:
                count += 1
            else:
                break
        # Check down-right
        for i in range(1, min(row, self.cols - col - 1) + 1):
            if self.board[row - i, col + i] == player:
                count += 1
            else:
                break
        if count >= 4:
            return True

        return False

    def get_last_move_position(self) -> tuple[int, int] | None:
        """Get the (row, col) position of the last move."""
        if not self.move_history:
            return None
        col = self.move_history[-1]
        for row in range(self.rows):
            if self.board[row, col] != 0:
                return row, col
        return None

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
