"""
Prepares the Connect Four game data for training.
Reads game data from a text file, where each line is a comma-separated sequence of moves.
"""

import os
import argparse


class Preparer:
    def __init__(self, input_file, output_dir):
        self.input_file = input_file
        self.output_dir = output_dir

    def prepare(self):
        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir)

        output_file_path = os.path.join(self.output_dir, "games.txt")

        with open(self.input_file, "r") as f_in, open(output_file_path, "w") as f_out:
            for line in f_in:
                # The format is winner:moves
                parts = line.strip().split(":")
                if len(parts) == 2:
                    moves = parts[1]
                    f_out.write(moves + "\n")


def main():
    parser = argparse.ArgumentParser(description="Prepare Connect Four game data.")
    parser.add_argument("input_file", help="Path to the raw game data file.")
    parser.add_argument(
        "--output_dir", default="datasets", help="Directory to save the prepared data."
    )
    args = parser.parse_args()

    # Construct the full path for the output directory
    output_dir_path = os.path.join(os.path.dirname(__file__), args.output_dir)

    preparer = Preparer(args.input_file, output_dir_path)
    preparer.prepare()


if __name__ == "__main__":
    main()
