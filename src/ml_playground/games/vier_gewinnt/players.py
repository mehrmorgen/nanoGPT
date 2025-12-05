from __future__ import annotations
import random
from typing import Optional, Tuple
import numpy as np
from .engine import VierGewinnt


class Player:
    def get_move(self, game: VierGewinnt) -> int:
        raise NotImplementedError


class RandomPlayer(Player):
    def get_move(self, game: VierGewinnt) -> int:
        return random.choice(game.get_valid_moves())


class HeuristicPlayer(Player):
    def get_move(self, game: VierGewinnt) -> int:
        valid_moves = game.get_valid_moves()

        # Prioritize winning moves
        for move in valid_moves:
            if self.is_winning_move(game, move, game.current_player):
                return move

        # Block opponent's winning moves
        opponent = 3 - game.current_player
        for move in valid_moves:
            if self.is_winning_move(game, move, opponent):
                return move

        # Simple heuristic: prefer center columns
        preferred_moves = [3, 2, 4, 1, 5, 0, 6]
        for move in preferred_moves:
            if move in valid_moves:
                return move

        return random.choice(valid_moves)

    def is_winning_move(self, game: VierGewinnt, col: int, player: int) -> bool:
        temp_game = VierGewinnt(rows=game.rows, cols=game.cols)
        temp_game.board = np.copy(game.board)
        temp_game.current_player = player

        for r in range(temp_game.rows - 1, -1, -1):
            if temp_game.board[r, col] == 0:
                temp_game.board[r, col] = player
                break

        return temp_game.check_win(player)


class MinimaxPlayer(Player):
    def __init__(self, depth: int = 4) -> None:
        self.depth = depth
        self.player_id: Optional[int] = None

    def get_move(self, game: VierGewinnt) -> int:
        self.player_id = game.current_player
        _, move = self.minimax(game, self.depth, True, -np.inf, np.inf)
        if move is None:
            # Should not happen if valid moves exist
            return random.choice(game.get_valid_moves())
        return move

    def minimax(
        self,
        game: VierGewinnt,
        depth: int,
        maximizing_player: bool,
        alpha: float,
        beta: float,
    ) -> Tuple[float, Optional[int]]:
        if depth == 0 or game.check_win(1) or game.check_win(2) or game.is_full():
            return float(self.evaluate_board(game)), None

        valid_moves = game.get_valid_moves()

        if maximizing_player:
            max_eval = -np.inf
            best_move = random.choice(valid_moves)
            for move in valid_moves:
                temp_game = self.create_temp_game(game, move, game.current_player)
                evaluation, _ = self.minimax(temp_game, depth - 1, False, alpha, beta)
                if evaluation > max_eval:
                    max_eval = evaluation
                    best_move = move
                alpha = max(alpha, evaluation)
                if beta <= alpha:
                    break
            return max_eval, best_move
        else:  # Minimizing player
            min_eval = np.inf
            best_move = random.choice(valid_moves)
            for move in valid_moves:
                temp_game = self.create_temp_game(game, move, 3 - game.current_player)
                evaluation, _ = self.minimax(temp_game, depth - 1, True, alpha, beta)
                if evaluation < min_eval:
                    min_eval = evaluation
                    best_move = move
                beta = min(beta, evaluation)
                if beta <= alpha:
                    break
            return min_eval, best_move

    def evaluate_board(self, game: VierGewinnt) -> int:
        # Terminal states
        if self.player_id is None:
            return 0
        if game.check_win(self.player_id):
            return 100000
        elif game.check_win(3 - self.player_id):
            return -100000
        elif game.is_full():
            return 0

        # Heuristic scoring
        score = 0

        # 1. Center column preference (tokens in center are more valuable)
        center_col = game.cols // 2
        center_count: int = int(np.sum(game.board[:, center_col] == self.player_id))
        score += center_count * 3

        # 2. Window scoring (horizontal, vertical, diagonal)
        # We want to award points for having potential connections
        score += self.score_position(game, self.player_id)
        score -= self.score_position(
            game, 3 - self.player_id
        )  # Penalize opponent's potential

        return score

    def score_position(self, game: VierGewinnt, player: int) -> int:
        score = 0

        # Horizontal
        for r in range(game.rows):
            row_array = [int(i) for i in game.board[r, :].tolist()]
            for c in range(game.cols - 3):
                window = row_array[c : c + 4]
                score += self.evaluate_window(window, player)

        # Vertical
        for c in range(game.cols):
            col_array = [int(i) for i in game.board[:, c].tolist()]
            for r in range(game.rows - 3):
                window = col_array[r : r + 4]
                score += self.evaluate_window(window, player)

        # Positive Diagonal
        for r in range(game.rows - 3):
            for c in range(game.cols - 3):
                window = [int(game.board[r + i][c + i]) for i in range(4)]
                score += self.evaluate_window(window, player)

        # Negative Diagonal
        for r in range(game.rows - 3):
            for c in range(game.cols - 3):
                window = [int(game.board[r + 3 - i][c + i]) for i in range(4)]
                score += self.evaluate_window(window, player)

        return score

    def evaluate_window(self, window: list[int], player: int) -> int:
        score = 0
        opp_player = 3 - player

        if window.count(player) == 4:
            score += 100
        elif window.count(player) == 3 and window.count(0) == 1:
            score += 5
        elif window.count(player) == 2 and window.count(0) == 2:
            score += 2

        if window.count(opp_player) == 3 and window.count(0) == 1:
            score -= 4

        return score

    def create_temp_game(self, game: VierGewinnt, col: int, player: int) -> VierGewinnt:
        temp_game = VierGewinnt(rows=game.rows, cols=game.cols)
        temp_game.board = np.copy(game.board)
        temp_game.current_player = player
        for r in range(temp_game.rows - 1, -1, -1):
            if temp_game.board[r, col] == 0:
                temp_game.board[r, col] = player
                break
        return temp_game
