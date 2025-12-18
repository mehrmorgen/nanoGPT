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

        return random.choice(valid_moves)

    def is_winning_move(self, game: VierGewinnt, col: int, player: int) -> bool:
        temp_game = VierGewinnt(rows=game.rows, cols=game.cols)
        temp_game.board = np.copy(game.board)
        temp_game.current_player = player

        try:
            temp_game.make_move(col)
        except ValueError:
            return False
        return temp_game.check_win(player)


class MinimaxPlayer(Player):
    def __init__(self, depth: int = 4) -> None:
        self.depth = depth
        self.player_id: Optional[int] = None

    def get_move(self, game: VierGewinnt) -> int:
        self.player_id = game.current_player
        # Get the last move position for optimized terminal detection
        last_move_pos = game.get_last_move_position()
        _, move = self.minimax(game, self.depth, True, -np.inf, np.inf, last_move_pos)
        if move is None:
            return random.choice(game.get_valid_moves())
        return move

    def _require_player_id(self) -> int:
        if self.player_id is None:
            raise ValueError("player_id must be set before calling minimax")
        return self.player_id

    def minimax(
        self,
        game: VierGewinnt,
        depth: int,
        maximizing_player: bool,
        alpha: float,
        beta: float,
        last_move_pos: tuple[int, int] | None = None,
    ) -> Tuple[float, Optional[int]]:
        valid_moves = game.get_valid_moves()

        # Optimized terminal detection: only check around last move
        is_terminal = False
        if last_move_pos:
            row, col = last_move_pos
            # Only need to check if the last move created a win
            is_terminal = game.check_win_from_position(row, col, 1) or \
                         game.check_win_from_position(row, col, 2) or \
                         game.is_full()
        else:
            # For initial call, fall back to full check
            is_terminal = game.check_win(1) or game.check_win(2) or game.is_full()
        
        if depth == 0 or is_terminal:
            return self.evaluate_board(game), None

        if maximizing_player:
            max_eval = -np.inf
            best_move = None
            for move in valid_moves:
                # Make move in-place
                player = self._require_player_id()
                move_row = self._make_inplace_move(game, move, player)
                
                # Recurse with the new position
                eval_score, _ = self.minimax(game, depth - 1, False, alpha, beta, (move_row, move))
                
                # Undo the move
                self._undo_inplace_move(game, move)
                
                if eval_score > max_eval:
                    max_eval = eval_score
                    best_move = move
                alpha = max(alpha, eval_score)
                if beta <= alpha:
                    break
            return max_eval, best_move
        else:
            min_eval = np.inf
            best_move = None
            for move in valid_moves:
                # Make move in-place
                player = 3 - self._require_player_id()
                move_row = self._make_inplace_move(game, move, player)
                
                # Recurse with the new position
                eval_score, _ = self.minimax(game, depth - 1, True, alpha, beta, (move_row, move))
                
                # Undo the move
                self._undo_inplace_move(game, move)
                
                if eval_score < min_eval:
                    min_eval = eval_score
                    best_move = move
                beta = min(beta, eval_score)
                if beta <= alpha:
                    break
            return min_eval, best_move

    def evaluate_board(self, game: VierGewinnt) -> int:
        if self.player_id is None:
            return 0

        if game.check_win(self.player_id):
            return 100000
        if game.check_win(3 - self.player_id):
            return -100000

        # Use single-pass evaluation instead of scoring both players separately
        score = self.score_position_both_players(game)

        return score

    def score_position(self, game: VierGewinnt, player: int) -> int:
        score = 0
        board = game.board
        rows, cols = game.rows, game.cols

        # Horizontal
        for r in range(rows):
            row_array = [int(i) for i in list(board[r, :])]
            for c in range(cols - 3):
                window = row_array[c : c + 4]
                score += self.evaluate_window(window, player)

        # Vertical
        for c in range(cols):
            col_array = [int(i) for i in list(board[:, c])]
            for r in range(rows - 3):
                window = col_array[r : r + 4]
                score += self.evaluate_window(window, player)

        # Positive diagonal
        for r in range(rows - 3):
            for c in range(cols - 3):
                window = [board[r + i][c + i] for i in range(4)]
                score += self.evaluate_window(window, player)

        # Negative diagonal
        for r in range(3, rows):
            for c in range(cols - 3):
                window = [board[r - i][c + i] for i in range(4)]
                score += self.evaluate_window(window, player)

        return score

    def score_position_both_players(self, game: VierGewinnt) -> int:
        """Evaluate board position for both players in a single pass."""
        score = 0
        board = game.board
        rows, cols = game.rows, game.cols
        player = self.player_id if self.player_id is not None else 1
        opponent = 3 - player

        # Horizontal
        for r in range(rows):
            row_array = [int(i) for i in list(board[r, :])]
            for c in range(cols - 3):
                window = row_array[c : c + 4]
                score += self.evaluate_window_both(window, player, opponent)

        # Vertical
        for c in range(cols):
            col_array = [int(i) for i in list(board[:, c])]
            for r in range(rows - 3):
                window = col_array[r : r + 4]
                score += self.evaluate_window_both(window, player, opponent)

        # Positive diagonal
        for r in range(rows - 3):
            for c in range(cols - 3):
                window = [board[r + i][c + i] for i in range(4)]
                score += self.evaluate_window_both(window, player, opponent)

        # Negative diagonal
        for r in range(3, rows):
            for c in range(cols - 3):
                window = [board[r - i][c + i] for i in range(4)]
                score += self.evaluate_window_both(window, player, opponent)

        return score

    def evaluate_window(self, window: list[int], player: int) -> int:
        score = 0
        opponent = 3 - player
        if window.count(player) == 4:
            score += 100
        elif window.count(player) == 3 and window.count(0) == 1:
            score += 10
        elif window.count(player) == 2 and window.count(0) == 2:
            score += 5

        if window.count(opponent) == 3 and window.count(0) == 1:
            score -= 80
        return score

    def evaluate_window_both(self, window: list[int], player: int, opponent: int) -> int:
        """Evaluate a window for both players in a single pass."""
        score = 0
        player_count = window.count(player)
        opponent_count = window.count(opponent)
        empty_count = window.count(0)
        
        # Player scoring
        if player_count == 4:
            score += 100
        elif player_count == 3 and empty_count == 1:
            score += 10
        elif player_count == 2 and empty_count == 2:
            score += 5
        
        # Opponent penalty (note the asymmetry)
        if opponent_count == 3 and empty_count == 1:
            score -= 80
        
        return score

    def create_temp_game(self, game: VierGewinnt, col: int, player: int) -> VierGewinnt:
        temp_game = VierGewinnt(rows=game.rows, cols=game.cols)
        temp_game.board = np.copy(game.board)
        temp_game.current_player = player
        for r in range(temp_game.rows - 1, -1, -1):
            if temp_game.board[r, col] == 0:
                temp_game.board[r, col] = player
                break
        temp_game.current_player = 3 - player
        return temp_game

    def _make_inplace_move(self, game: VierGewinnt, col: int, player: int) -> int:
        """Make a move in-place and return the row where the piece was placed."""
        for r in range(game.rows - 1, -1, -1):
            if game.board[r, col] == 0:
                game.board[r, col] = player
                game.move_history.append(col)
                return r
        raise ValueError(f"Column {col} is full")
    
    def _undo_inplace_move(self, game: VierGewinnt, col: int) -> None:
        """Undo the last move in the specified column."""
        if not game.move_history:
            return
        # Remove the last move from history
        game.move_history.pop()
        # Clear the top piece in the column
        for r in range(game.rows):
            if game.board[r, col] != 0:
                game.board[r, col] = 0
                return
