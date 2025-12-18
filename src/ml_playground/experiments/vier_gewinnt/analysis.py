"""Analyze Connect Four move-only datasets and emit JSON metrics.

Reads a file where each line is either:
- ``winner:m0,m1,...`` (preferred raw format)
- ``m0,m1,...`` (moves only, as produced by the preparer)

Outputs aggregated metrics to ``out/analysis.json`` by default.
"""

from __future__ import annotations

import argparse
import json
import math
from collections import Counter
from pathlib import Path
from statistics import mean, median
from typing import Iterable


ROWS = 6
COLS = 7
DEFAULT_INPUT = Path(__file__).parent / "datasets" / "hard" / "dataset.txt"
DEFAULT_OUTPUT = Path(__file__).parent / "out" / "analysis.json"


class GameResult:
    def __init__(
        self,
        moves: list[int],
        winner: int | None,
        termination: str,
        illegal_reason: str | None = None,
        ended_at: int | None = None,
        continued_after_end: bool = False,
    ) -> None:
        self.moves = moves
        self.winner = winner
        self.termination = termination
        self.illegal_reason = illegal_reason
        self.ended_at = ended_at
        self.continued_after_end = continued_after_end


def parse_line(line: str) -> tuple[int | None, list[int]] | None:
    stripped = line.strip()
    if not stripped:
        return None

    if ":" in stripped:
        raw_winner, raw_moves = stripped.split(":", maxsplit=1)
        try:
            winner = int(raw_winner)
        except ValueError:
            winner = None
        moves_part = raw_moves
    else:
        winner = None
        moves_part = stripped

    try:
        moves = [int(tok) for tok in moves_part.split(",") if tok]
    except ValueError:
        return None
    return winner, moves


def check_direction(board: list[list[int]], r: int, c: int, dr: int, dc: int, player: int) -> int:
    count = 0
    rr, cc = r, c
    while 0 <= rr < ROWS and 0 <= cc < COLS and board[rr][cc] == player:
        count += 1
        rr += dr
        cc += dc
    return count


def detect_win(board: list[list[int]], r: int, c: int, player: int) -> str | None:
    directions = {
        "horizontal": (0, 1),
        "vertical": (1, 0),
        "diagonal_pos": (1, 1),
        "diagonal_neg": (-1, 1),
    }
    for name, (dr, dc) in directions.items():
        forward = check_direction(board, r, c, dr, dc, player)
        backward = check_direction(board, r, c, -dr, -dc, player) - 1  # subtract double-counted origin
        if forward + backward >= 4:
            return name
    return None


def simulate_game(moves: list[int]) -> GameResult:
    board = [[0 for _ in range(COLS)] for _ in range(ROWS)]
    heights = [0 for _ in range(COLS)]
    illegal_reason: str | None = None
    termination = "incomplete"
    ended_at: int | None = None
    winner: int | None = None

    for idx, move in enumerate(moves):
        player = 1 if idx % 2 == 0 else 2

        if move < 0 or move >= COLS:
            illegal_reason = "out_of_range_move"
            winner = 3 - player
            termination = "illegal"
            ended_at = idx
            break

        if heights[move] >= ROWS:
            illegal_reason = "column_full"
            winner = 3 - player
            termination = "illegal"
            ended_at = idx
            break

        row = ROWS - 1 - heights[move]
        board[row][move] = player
        heights[move] += 1

        win_type = detect_win(board, row, move, player)
        if win_type:
            winner = player
            termination = win_type
            ended_at = idx + 1
            break

        if sum(heights) == ROWS * COLS:
            winner = 0
            termination = "draw"
            ended_at = idx + 1
            break

    if winner is None:
        # Game ended without a winner (e.g., truncated log)
        termination = "incomplete"
        ended_at = len(moves)

    continued_after_end = ended_at is not None and ended_at < len(moves)
    return GameResult(
        moves=moves,
        winner=winner,
        termination=termination,
        illegal_reason=illegal_reason,
        ended_at=ended_at,
        continued_after_end=continued_after_end,
    )


def shannon_entropy(counts: Iterable[int]) -> float:
    total = sum(counts)
    if total == 0:
        return 0.0
    entropy = 0.0
    for count in counts:
        if count == 0:
            continue
        p = count / total
        entropy -= p * math.log2(p)
    return entropy


def aggregate_results(results: list[GameResult]) -> dict:
    invalid_reasons: Counter[str] = Counter()
    win_counts: Counter[int | str] = Counter()
    termination_types: Counter[str] = Counter()
    lengths: list[int] = []
    column_freq = [0] * COLS
    column_freq_p1 = [0] * COLS
    column_freq_p2 = [0] * COLS
    opening_moves: Counter[int] = Counter()

    for res in results:
        if res.illegal_reason:
            invalid_reasons[res.illegal_reason] += 1
        if res.continued_after_end:
            invalid_reasons["continued_after_end"] += 1

        if res.winner is not None:
            win_counts[res.winner] += 1
        else:
            win_counts["unknown"] += 1

        termination_types[res.termination] += 1
        lengths.append(res.ended_at or 0)

        if res.moves:
            opening_moves[res.moves[0]] += 1
        for idx, move in enumerate(res.moves):
            if 0 <= move < COLS:
                column_freq[move] += 1
                if idx % 2 == 0:
                    column_freq_p1[move] += 1
                else:
                    column_freq_p2[move] += 1

    total_games = len(results)
    valid_games = total_games - sum(invalid_reasons.values())

    length_stats = {
        "min": min(lengths) if lengths else 0,
        "max": max(lengths) if lengths else 0,
        "mean": mean(lengths) if lengths else 0,
        "median": median(lengths) if lengths else 0,
    }

    move_entropy = shannon_entropy(column_freq)

    return {
        "total_games": total_games,
        "valid_games_estimate": valid_games,
        "win_counts": dict(win_counts),
        "termination_types": dict(termination_types),
        "invalid_reasons": dict(invalid_reasons),
        "length_stats": length_stats,
        "first_move_advantage": {
            "p1_wins": win_counts.get(1, 0),
            "p2_wins": win_counts.get(2, 0),
            "draws": win_counts.get(0, 0),
        },
        "column_frequency": column_freq,
        "column_frequency_p1": column_freq_p1,
        "column_frequency_p2": column_freq_p2,
        "opening_move_distribution": dict(sorted(opening_moves.items())),
        "move_entropy_bits": move_entropy,
    }


def load_games(path: Path) -> list[GameResult]:
    results: list[GameResult] = []
    with path.open(encoding="utf-8") as f:
        for line in f:
            parsed = parse_line(line)
            if not parsed:
                continue
            _, moves = parsed
            results.append(simulate_game(moves))
    return results


def main() -> None:
    parser = argparse.ArgumentParser(description="Analyze Connect Four move logs.")
    parser.add_argument(
        "--input",
        type=Path,
        default=DEFAULT_INPUT,
        help="Path to dataset file (winner:moves or moves per line).",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=DEFAULT_OUTPUT,
        help="Where to write JSON metrics.",
    )
    args = parser.parse_args()

    results = load_games(args.input)
    aggregated = aggregate_results(results)

    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("w", encoding="utf-8") as f:
        json.dump(
            {
                "source": str(args.input),
                "metrics": aggregated,
            },
            f,
            indent=2,
        )


if __name__ == "__main__":
    main()
