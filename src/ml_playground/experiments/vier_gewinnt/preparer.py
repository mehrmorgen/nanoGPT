"""Utility to reshape Connect Four move logs into training datasets."""

from __future__ import annotations

import argparse
import logging
import os
from pathlib import Path
from typing import Optional

from ml_playground.configuration.models import PreparerConfig, SharedConfig
from ml_playground.core.tokenizer import create_tokenizer
from ml_playground.data_pipeline.transforms.io import write_bin_and_meta
from ml_playground.data_pipeline.transforms.tokenization import prepare_with_tokenizer

logging.basicConfig(level=logging.INFO)


class Preparer:
    def __init__(self, input_file: str | None = None, output_dir: str | None = None):
        self.input_file = input_file
        self.output_dir = output_dir

    def prepare(
        self,
        cfg: Optional[PreparerConfig] = None,
        shared: Optional[SharedConfig] = None,
    ) -> None:
        if cfg and shared:
            input_path = cfg.raw_text_path
            output_path = shared.dataset_dir
            tokenizer_type = cfg.tokenizer_type
        else:
            input_path = Path(self.input_file) if self.input_file else None
            output_path = Path(self.output_dir) if self.output_dir else None
            tokenizer_type = "char"

        if not input_path or not output_path:
            raise ValueError("Input file and output directory must be provided.")

        if not output_path.exists():
            output_path.mkdir(parents=True, exist_ok=True)

        # output_file_path = output_path / "games.txt"
        dataset_txt_path = output_path / "dataset.txt"

        valid_games = []
        invalid_lines = 0

        with open(input_path, "r", encoding="utf-8") as f_in:
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

                valid_games.append(moves)

        logging.info(
            "Prepared %d games from %s (skipped %d malformed lines)",
            len(valid_games),
            input_path,
            invalid_lines,
        )

        full_text = "\n".join(valid_games)

        # Write dataset.txt (100% of training data as readable text)
        with open(dataset_txt_path, "w", encoding="utf-8") as f:
            f.write(full_text)
        logging.info(f"Wrote readable dataset to {dataset_txt_path}")

        # Generate binaries for training
        tokenizer = create_tokenizer(tokenizer_type)
        # Default split 0.9, hardcoded here or can be extracted from cfg.extras if needed
        train_ids, val_ids, meta, tokenizer = prepare_with_tokenizer(
            full_text, tokenizer, split=0.9
        )

        # Ensure we use a dummy data config or None to default filenames
        write_bin_and_meta(
            output_path,
            train_ids,
            val_ids,
            meta,
            logger=logging.getLogger(__name__),
        )
        logging.info(f"Wrote binaries and meta to {output_path}")


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
