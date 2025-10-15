from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Sequence
import random

import numpy as np

from ml_playground.configuration.models import PreparerConfig
from ml_playground.core.error_handling import DataError, ProgressReporter
from ml_playground.core.tokenizer import create_tokenizer
from ml_playground.data_pipeline.transforms.io import (
    diff_file_states,
    snapshot_file_states,
    write_bin_and_meta,
)
from ml_playground.data_pipeline.transforms.tokenization import (
    create_standardized_metadata,
    split_train_val,
)
from ml_playground.experiments.protocol import (
    Preparer as _PreparerProto,
    PrepareReport,
)


@dataclass
class _ConnectFourGame:
    """Minimal state tracker for Connect Four gameplay."""

    rows: int = 6
    cols: int = 7
    current_player: int = 1

    def __post_init__(self) -> None:
        self.board: list[list[int]] = [[0 for _ in range(self.cols)] for _ in range(self.rows)]
        self.moves: list[int] = []

    def valid_moves(self) -> list[int]:
        return [c for c in range(self.cols) if self.board[0][c] == 0]

    def drop(self, col: int) -> None:
        for row in range(self.rows - 1, -1, -1):
            if self.board[row][col] == 0:
                self.board[row][col] = self.current_player
                self.moves.append(col + 1)  # store as 1-based for readability
                self.current_player = 3 - self.current_player
                return
        raise DataError(f"Column {col} is full; invalid move in Connect Four simulation")

    def has_winner(self, player: int) -> bool:
        # Horizontal and vertical checks
        for r in range(self.rows):
            for c in range(self.cols):
                if self.board[r][c] != player:
                    continue
                if c + 3 < self.cols and all(self.board[r][c + i] == player for i in range(4)):
                    return True
                if r + 3 < self.rows and all(self.board[r + i][c] == player for i in range(4)):
                    return True
                if r + 3 < self.rows and c + 3 < self.cols and all(
                    self.board[r + i][c + i] == player for i in range(4)
                ):
                    return True
                if r - 3 >= 0 and c + 3 < self.cols and all(
                    self.board[r - i][c + i] == player for i in range(4)
                ):
                    return True
        return False

    def is_full(self) -> bool:
        return all(self.board[0][c] != 0 for c in range(self.cols))

    def move_sequence(self) -> Sequence[int]:
        return tuple(self.moves)


def _simulate_game(rng: random.Random) -> Sequence[int]:
    game = _ConnectFourGame()
    while not game.is_full():
        candidates = game.valid_moves()
        if not candidates:
            break
        col = rng.choice(candidates)
        game.drop(col)
        if game.has_winner(3 - game.current_player):
            break
    return game.move_sequence()


def _format_sequences(sequences: Iterable[Sequence[int]]) -> list[str]:
    formatted: list[str] = []
    for seq in sequences:
        if not seq:
            continue
        formatted.append(" ".join(str(move) for move in seq))
    return formatted


class ConnectFourPreparer(_PreparerProto):
    def prepare(self, cfg: PreparerConfig) -> PrepareReport:  # type: ignore[override]
        extras = getattr(cfg, "extras", {}) or {}
        dataset_dir_override = extras.get("dataset_dir_override")
        base_dir_override = extras.get("base_dir")

        if dataset_dir_override is not None:
            ds_dir = Path(dataset_dir_override)
            exp_dir = ds_dir.parent
        else:
            exp_dir = Path(base_dir_override) if base_dir_override else Path(__file__).resolve().parent
            ds_dir = exp_dir / "datasets"
        ds_dir.mkdir(parents=True, exist_ok=True)

        outputs = [ds_dir / "train.bin", ds_dir / "val.bin", ds_dir / "meta.pkl"]
        pre = snapshot_file_states(outputs)

        logger = cfg.logger
        progress = ProgressReporter(logger, total_steps=5)
        progress.start("Preparing Connect Four dataset")

        num_games = int(extras.get("num_games", 10_000))
        min_moves = int(extras.get("min_moves", 7))
        split_ratio = float(extras.get("train_val_split", 0.9))
        if not 0.0 < split_ratio < 1.0:
            raise DataError("train_val_split must be between 0 and 1 (exclusive)")

        seed = extras.get("seed")
        rng = random.Random(seed) if seed is not None else random.Random()

        sequences: list[Sequence[int]] = []
        for _ in range(num_games):
            moves = _simulate_game(rng)
            if len(moves) >= min_moves:
                sequences.append(moves)

        if not sequences:
            raise DataError("Connect Four simulation produced no games meeting the minimum move threshold")

        progress.update(1, f"Simulated {len(sequences)} games")

        text_sequences = _format_sequences(sequences)
        dataset_text = "\n".join(text_sequences)
        train_text, val_text = split_train_val(dataset_text, split=split_ratio)
        progress.update(1, "Split dataset into train/val")

        tokenizer = create_tokenizer("char")
        train_ids = np.array(tokenizer.encode(train_text), dtype=np.uint16)
        val_ids = np.array(tokenizer.encode(val_text), dtype=np.uint16)
        progress.update(1, "Tokenized train/val sequences")

        meta_extras = {
            "connect_four": {
                "num_games": len(sequences),
                "min_moves": min_moves,
                "train_val_split": split_ratio,
            }
        }
        meta = create_standardized_metadata(tokenizer, len(train_ids), len(val_ids), extras=meta_extras)
        progress.update(1, "Created metadata")

        write_bin_and_meta(ds_dir, train_ids, val_ids, meta, logger=logger)
        progress.finish("Connect Four dataset preparation completed")

        created, updated, skipped = diff_file_states(outputs, pre)
        msgs = (
            f"[connect_four] prepared dataset at {ds_dir}",
            f"[connect_four.outputs.created] {[str(p) for p in created]}",
            f"[connect_four.outputs.updated] {[str(p) for p in updated]}",
            f"[connect_four.outputs.skipped] {[str(p) for p in skipped]}",
        )

        return PrepareReport(
            created_files=tuple(created),
            updated_files=tuple(updated),
            skipped_files=tuple(skipped),
            messages=msgs,
        )
