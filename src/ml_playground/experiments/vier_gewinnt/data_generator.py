from __future__ import annotations

import argparse
from pathlib import Path
from typing import Dict, List, Tuple

from .engine import VierGewinnt
from .players import (
    HeuristicPlayer,
    MinimaxPlayer,
    Player,
    RandomPlayer,
)


PLAYER_CLASSES: Dict[str, type[Player]] = {
    "random": RandomPlayer,
    "heuristic": HeuristicPlayer,
    "minimax": MinimaxPlayer,
}


def play_game(player1: Player, player2: Player) -> Tuple[int, List[int]]:
    game = VierGewinnt()
    move_history: List[int] = []

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


def main() -> None:
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
    default_output_file = Path(__file__).parent / "datasets" / "games.txt"
    parser.add_argument(
        "output_file",
        nargs="?",
        default=str(default_output_file),
        help="File to save the training data",
    )
    parser.add_argument(
        "--progress-every",
        type=int,
        default=1000,
        help="Print progress every N simulation attempts (0 disables).",
    )
    args = parser.parse_args()

    player1 = PLAYER_CLASSES[args.player1]()
    player2 = PLAYER_CLASSES[args.player2]()

    seen_games: set[bytes] = set()
    attempts = 0
    max_attempts = max(args.num_games * 20, args.num_games + 10)

    output_path = Path(args.output_file)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8") as f:
        while len(seen_games) < args.num_games:
            if attempts >= max_attempts:
                raise RuntimeError(
                    "Unable to generate the requested number of unique games. "
                    "The selected players may be too deterministic for this quota."
                )

            _, move_history = play_game(player1, player2)
            attempts += 1

            # moves are 0..6, so bytes(move_history) is a compact and fast hash key
            key = bytes(move_history)
            if key in seen_games:
                if args.progress_every and attempts % args.progress_every == 0:
                    print(
                        f"Collected {len(seen_games)}/{args.num_games} unique games "
                        f"after {attempts} simulations"
                    )
                continue

            seen_games.add(key)
            f.write(",".join(map(str, move_history)) + "\n")

            if args.progress_every and attempts % args.progress_every == 0:
                print(
                    f"Collected {len(seen_games)}/{args.num_games} unique games "
                    f"after {attempts} simulations"
                )


if __name__ == "__main__":
    main()
