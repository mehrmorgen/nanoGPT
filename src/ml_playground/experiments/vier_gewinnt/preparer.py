"""
Generates a dataset of Connect Four games.
Each game is a sequence of moves.
The board is 7x6.
Moves are integers from 0 to 6, representing the column.
"""

import os
import random
import numpy as np


class Preparer:
    def __init__(self, num_games=10000):
        self.num_games = num_games

    def get_available_moves(self, board):
        """Returns a list of available moves (columns that are not full)."""
        return [col for col in range(7) if board[5][col] == 0]

    def make_move(self, board, move, player):
        """Makes a move on the board."""
        for row in range(6):
            if board[row][move] == 0:
                board[row][move] = player
                return True
        return False

    def check_win(self, board, player):
        """Checks if the given player has won."""
        # Check horizontal
        for row in range(6):
            for col in range(4):
                if all(board[row][col + i] == player for i in range(4)):
                    return True
        # Check vertical
        for row in range(3):
            for col in range(7):
                if all(board[row + i][col] == player for i in range(4)):
                    return True
        # Check diagonal (down-right)
        for row in range(3):
            for col in range(4):
                if all(board[row + i][col + i] == player for i in range(4)):
                    return True
        # Check diagonal (up-right)
        for row in range(3, 6):
            for col in range(4):
                if all(board[row - i][col + i] == player for i in range(4)):
                    return True
        return False

    def generate_game(self):
        """Generates a single game of Connect Four."""
        board = np.zeros((6, 7), dtype=int)
        moves = []
        player = 1
        while len(moves) < 42:
            available_moves = self.get_available_moves(board)
            if not available_moves:
                break
            move = random.choice(available_moves)
            moves.append(move)
            self.make_move(board, move, player)
            if self.check_win(board, player):
                break
            player = 3 - player  # Switch player (1 -> 2, 2 -> 1)
        return moves

    def prepare(self):
        dataset_dir = os.path.join(os.path.dirname(__file__), "datasets")
        if not os.path.exists(dataset_dir):
            os.makedirs(dataset_dir)

        file_path = os.path.join(dataset_dir, "games.txt")

        with open(file_path, "w") as f:
            for _ in range(self.num_games):
                game = self.generate_game()
                f.write(",".join(map(str, game)) + "\n")


def main():
    preparer = Preparer()
    preparer.prepare()


if __name__ == "__main__":
    main()
