import argparse
from .engine import VierGewinnt
from .players import RandomPlayer, HeuristicPlayer, MinimaxPlayer

PLAYER_CLASSES = {
    "random": RandomPlayer,
    "heuristic": HeuristicPlayer,
    "minimax": MinimaxPlayer,
}


def play_game(player1, player2):
    game = VierGewinnt()
    move_history = []

    while True:
        player = player1 if game.current_player == 1 else player2

        try:
            move = player.get_move(game)
            move_history.append(move)
            winner = game.make_move(move)
        except ValueError:
            # Invalid move by a player, treat as a loss for that player
            winner = 3 - game.current_player
            break

        if winner != 0:
            break
        if game.is_full():
            break

    return winner, move_history


def main():
    parser = argparse.ArgumentParser(
        description="Generate training data for Vier Gewinnt."
    )
    parser.add_argument(
        "player1", choices=PLAYER_CLASSES.keys(), help="Type of player 1"
    )
    parser.add_argument(
        "player2", choices=PLAYER_CLASSES.keys(), help="Type of player 2"
    )
    parser.add_argument("num_games", type=int, help="Number of games to play")
    parser.add_argument("output_file", help="File to save the training data")
    args = parser.parse_args()

    player1 = PLAYER_CLASSES[args.player1]()
    player2 = PLAYER_CLASSES[args.player2]()

    with open(args.output_file, "w") as f:
        for i in range(args.num_games):
            winner, move_history = play_game(player1, player2)
            f.write(f"{winner}:{','.join(map(str, move_history))}\n")
            if (i + 1) % 10 == 0:
                print(f"Played {i + 1}/{args.num_games} games")


if __name__ == "__main__":
    main()
