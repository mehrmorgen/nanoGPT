"""Utility to reshape Connect Four move logs into training datasets."""

from __future__ import annotations

import argparse
import logging
import os

logging.basicConfig(level=logging.INFO)


class Preparer:
    def __init__(self, input_file, output_dir):
        self.input_file = input_file
        self.output_dir = output_dir

    def prepare(self) -> None:
        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir)

        output_file_path = os.path.join(self.output_dir, "games.txt")

        valid_games = 0
        invalid_lines = 0

        with (
            open(self.input_file, "r", encoding="utf-8") as f_in,
            open(output_file_path, "w", encoding="utf-8") as f_out,
        ):
            for line_number, line in enumerate(f_in, start=1):
                stripped = line.strip()
                if not stripped:
                    continue

                # Expected format: "winner:moves"
                try:
                    winner, moves = stripped.split(":", maxsplit=1)
                except ValueError:
                    invalid_lines += 1
                    logging.warning(
                        "Line %d is invalid (missing winner/moves delimiter)",
                        line_number,
                    )
                    continue

                if not winner or not moves:
                    invalid_lines += 1
                    logging.warning("Line %d has empty winner or moves", line_number)
                    continue

                f_out.write(moves + "\n")
                valid_games += 1

        logging.info(
            "Prepared %d games from %s (skipped %d malformed lines)",
            valid_games,
            self.input_file,
            invalid_lines,
        )


def main() -> None:
    parser = argparse.ArgumentParser(description="Prepare Connect Four game data.")
    parser.add_argument("input_file", help="Path to the raw game data file.")
    parser.add_argument(
        "--output_dir",
        default="datasets",
        help="Directory to save the prepared data.",
    )
    args = parser.parse_args()

    # Construct the full path for the output directory
    output_dir_path = os.path.join(os.path.dirname(__file__), args.output_dir)

    preparer = Preparer(args.input_file, output_dir_path)
    preparer.prepare()


if __name__ == "__main__":
    main()
