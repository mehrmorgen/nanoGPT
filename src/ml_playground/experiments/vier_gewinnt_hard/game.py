"""
Provides a game engine for Connect Four (Vier Gewinnt).
This includes board state management, move validation, and a text-based board representation.
"""

import numpy as np


class VierGewinntGame:
    """
    Manages the state and rules of a Connect Four game.
    """

    def __init__(self):
        """Initializes an empty 6x7 game board."""
        self.board = np.zeros((6, 7), dtype=int)
        self.moves_made = 0
        self.winner = 0  # 0: no winner, 1: player 1, 2: player 2

    def make_move(self, move: int):
        """
        Applies a move to the board for the current player.

        Args:
            move: The column (0-6) where the player makes a move.

        Returns:
            A tuple (success, message) where success is a boolean indicating
            if the move was valid, and message contains an error description
            if the move was invalid.
        """
        if self.winner != 0:
            return False, f"Game has already been won by Player {self.winner}."

        player = self.get_current_player()
        if not 0 <= move <= 6:
            return False, f"Invalid move: column {move} is out of bounds (0-6)."
        if self.board[5][move] != 0:
            return False, f"Invalid move: column {move} is full."

        for row in range(6):
            if self.board[row][move] == 0:
                self.board[row][move] = player
                self.moves_made += 1
                if self.check_win(player):
                    self.winner = player
                return True, None

        return False, "Internal error: Column full but not detected."

    def check_win(self, player: int) -> bool:
        """Checks if the given player has won the game."""
        # Check horizontal
        for row in range(6):
            for col in range(4):
                if all(self.board[row][col + i] == player for i in range(4)):
                    return True
        # Check vertical
        for row in range(3):
            for col in range(7):
                if all(self.board[row + i][col] == player for i in range(4)):
                    return True
        # Check diagonal (down-right)
        for row in range(3):
            for col in range(4):
                if all(self.board[row + i][col + i] == player for i in range(4)):
                    return True
        # Check diagonal (up-right)
        for row in range(3, 6):
            for col in range(4):
                if all(self.board[row - i][col + i] == player for i in range(4)):
                    return True
        return False

    def get_current_player(self) -> int:
        """
        Determines the current player based on the number of moves made.
        Player 1 starts, then Player 2, and so on.

        Returns:
            The current player number (1 or 2).
        """
        return (self.moves_made % 2) + 1

    def __str__(self) -> str:
        """
        Returns a string representation of the current board state.
        'X' represents Player 1, 'O' represents Player 2, and '.' represents an empty cell.
        """
        s = "Board state:\n"
        for row in reversed(range(6)):
            s += "| "
            for col in range(7):
                piece = self.board[row][col]
                if piece == 0:
                    s += ". "
                elif piece == 1:
                    s += "X "  # Player 1
                else:
                    s += "O "  # Player 2
            s += "|\n"
        s += "-----------------\n"
        s += "  0 1 2 3 4 5 6  \n"
        if self.winner != 0:
            s += f"\nPlayer {self.winner} has won!\n"
        return s
