import random
import numpy as np
from .engine import VierGewinnt


class Player:
    def get_move(self, game: VierGewinnt):
        raise NotImplementedError


class RandomPlayer(Player):
    def get_move(self, game: VierGewinnt):
        return random.choice(game.get_valid_moves())


class HeuristicPlayer(Player):
    def get_move(self, game: VierGewinnt):
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

    def is_winning_move(self, game: VierGewinnt, col, player):
        temp_game = VierGewinnt(rows=game.rows, cols=game.cols)
        temp_game.board = np.copy(game.board)
        temp_game.current_player = player

        for r in range(temp_game.rows - 1, -1, -1):
            if temp_game.board[r, col] == 0:
                temp_game.board[r, col] = player
                break

        return temp_game.check_win(player)


class MinimaxPlayer(Player):
    def __init__(self, depth=4):
        self.depth = depth

    def get_move(self, game: VierGewinnt):
        _, move = self.minimax(game, self.depth, True, -np.inf, np.inf)
        return move

    def minimax(self, game, depth, maximizing_player, alpha, beta):
        if depth == 0 or game.check_win(1) or game.check_win(2) or game.is_full():
            return self.evaluate_board(game), None

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

    def evaluate_board(self, game):
        if game.check_win(self.player):
            return 100
        elif game.check_win(3 - self.player):
            return -100
        else:
            return 0  # Simple evaluation, can be improved

    def create_temp_game(self, game, col, player):
        temp_game = VierGewinnt(rows=game.rows, cols=game.cols)
        temp_game.board = np.copy(game.board)
        temp_game.current_player = player
        for r in range(temp_game.rows - 1, -1, -1):
            if temp_game.board[r, col] == 0:
                temp_game.board[r, col] = player
                break
        return temp_game

    @property
    def player(self):
        # This is a bit of a hack, the minimax player needs to know which player it is
        # In a real implementation, this would be passed in the constructor
        return 1
