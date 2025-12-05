import argparse
import sys
from pathlib import Path

project_root = Path(__file__).resolve().parents[4]
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from ml_playground.experiments.vier_gewinnt.engine import VierGewinnt
from ml_playground.experiments.vier_gewinnt.players import (
    RandomPlayer,
    HeuristicPlayer,
    MinimaxPlayer,
)
from ml_playground.experiments.vier_gewinnt.sampler_player import SamplerPlayer

PLAYER_TYPES = {
    "human": None,  # Human player will be handled separately
    "easy": RandomPlayer,
    "medium": HeuristicPlayer,
    "hard": MinimaxPlayer,
    "easy_ai": lambda: SamplerPlayer("vier_gewinnt_easy"),
    "medium_ai": lambda: SamplerPlayer("vier_gewinnt_medium"),
    "hard_ai": lambda: SamplerPlayer("vier_gewinnt_hard"),
}


def print_board(board):
    print("\n" + "-" * (board.shape[1] * 4 + 1))
    for r in range(board.shape[0]):
        row_str = "|"
        for c in range(board.shape[1]):
            if board[r, c] == 1:
                row_str += " X |"
            elif board[r, c] == 2:
                row_str += " O |"
            else:
                row_str += "   |"
        print(row_str)
        print("-" * (board.shape[1] * 4 + 1))
    print("  " + "   ".join(map(str, range(board.shape[1]))) + "  ")


def play_game(player1_type, player2_type):
    game = VierGewinnt()

    player1 = None
    if player1_type != "human":
        player1 = PLAYER_TYPES[player1_type]()

    player2 = None
    if player2_type != "human":
        player2 = PLAYER_TYPES[player2_type]()

    print("Starting Vier Gewinnt!")
    print_board(game.board)

    while True:
        current_player_obj = None
        if game.current_player == 1:
            print("Player 1 (X)'s turn.")
            current_player_obj = player1
        else:
            print("Player 2 (O)'s turn.")
            current_player_obj = player2

        move = -1
        if current_player_obj is None:  # Human player
            while True:
                try:
                    col = int(input(f"Player {game.current_player}, enter column (0-6): "))
                    if col in game.get_valid_moves():
                        move = col
                        break
                    else:
                        print("Invalid move. Try again.")
                except ValueError:
                    print("Invalid input. Please enter a number.")
        else:  # AI player
            print(f"Player {game.current_player} ({current_player_obj.__class__.__name__}) is thinking...")
            move = current_player_obj.get_move(game)
            print(f"Player {game.current_player} chose column {move}")

        if move == -1:  # No valid moves for AI, or game is full
            print("Game ended in a draw (no valid moves).")
            break

        winner = game.make_move(move)
        print_board(game.board)

        if winner != 0:
            print(f"Player {winner} wins!")
            break
        if game.is_full():
            print("It's a draw!")
            break


def main():
    parser = argparse.ArgumentParser(description="Play Vier Gewinnt against an AI.")
    parser.add_argument(
        "--player1",
        choices=PLAYER_TYPES.keys(),
        default="human",
        help="Type of player 1",
    )
    parser.add_argument(
        "--player2",
        choices=PLAYER_TYPES.keys(),
        default="easy",
        help="Type of player 2",
    )
    args = parser.parse_args()

    play_game(args.player1, args.player2)


if __name__ == "__main__":
    main()
