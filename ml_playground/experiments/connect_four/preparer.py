from __future__ import annotations

from pathlib import Path
from typing import Sequence

import numpy as np

from ml_playground.configuration.models import PreparerConfig
from ml_playground.core.error_handling import DataError, ProgressReporter
from ml_playground.data_pipeline.transforms.io import (
    diff_file_states,
    snapshot_file_states,
    write_bin_and_meta,
)
from ml_playground.data_pipeline.transforms.tokenization import (
    create_standardized_metadata,
)
from ml_playground.core.tokenizer import create_tokenizer
from ml_playground.experiments.protocol import Preparer as _PreparerProto, PrepareReport

BOARD_COLUMNS = 7
BOARD_ROWS = 6
BOARD_SIZE = BOARD_COLUMNS * BOARD_ROWS
_PLAYER_ONE = 1
_PLAYER_TWO = 2


class ConnectFourPreparer(_PreparerProto):
    """Generate a synthetic Connect Four dataset via self-play."""

    def prepare(self, cfg: PreparerConfig) -> PrepareReport:  # type: ignore[override]
        extras = cfg.extras or {}
        base_dir = extras.get("base_dir")
        exp_dir = Path(base_dir) if base_dir else Path(__file__).resolve().parent
        ds_dir = exp_dir / "datasets"
        ds_dir.mkdir(parents=True, exist_ok=True)
        outputs = [ds_dir / "train.bin", ds_dir / "val.bin", ds_dir / "meta.pkl"]

        num_games = self._coerce_positive_int(extras.get("num_games", 512), "num_games")
        train_fraction = self._coerce_fraction(extras.get("train_fraction", 0.9))
        seed = extras.get("seed", 1337)
        if not isinstance(seed, int):
            raise DataError("seed must be an integer")

        rng = np.random.default_rng(seed)

        progress = ProgressReporter(cfg.logger, total_steps=6)
        progress.start("Starting Connect Four dataset preparation")

        progress.update(1, "Simulating games")
        records = self._generate_dataset(rng, num_games)
        if len(records) < 2:
            raise DataError("Connect Four dataset requires at least two move records")

        progress.update(1, "Splitting train/val examples")
        train_lines, val_lines = self._split_records(records, train_fraction)

        train_text = "".join(train_lines)
        val_text = "".join(val_lines)

        progress.update(1, "Building tokenizer")
        tokenizer = self._build_tokenizer(train_text, val_text)

        progress.update(1, "Encoding dataset")
        train_ids = np.array(tokenizer.encode(train_text), dtype=np.uint16)
        val_ids = np.array(tokenizer.encode(val_text), dtype=np.uint16)

        progress.update(1, "Writing metadata and binaries")
        meta = create_standardized_metadata(
            tokenizer,
            train_tokens=int(train_ids.size),
            val_tokens=int(val_ids.size),
            extras={
                "board_rows": BOARD_ROWS,
                "board_columns": BOARD_COLUMNS,
                "tokens_per_position": BOARD_SIZE + 3,
                "examples_train": len(train_lines),
                "examples_val": len(val_lines),
            },
        )

        snapshot = snapshot_file_states(outputs)
        write_bin_and_meta(ds_dir, train_ids, val_ids, meta, logger=cfg.logger)
        progress.finish("Connect Four dataset preparation completed")

        created, updated, skipped = diff_file_states(outputs, snapshot)
        messages = (
            f"[connect_four] prepared dataset at {ds_dir}",
            f"[connect_four.outputs.created] {[str(p) for p in created]}",
            f"[connect_four.outputs.updated] {[str(p) for p in updated]}",
            f"[connect_four.outputs.skipped] {[str(p) for p in skipped]}",
            f"[connect_four.examples] train={len(train_lines)} val={len(val_lines)}",
        )

        return PrepareReport(
            created_files=tuple(created),
            updated_files=tuple(updated),
            skipped_files=tuple(skipped),
            messages=messages,
        )

    @staticmethod
    def _coerce_positive_int(value: object, field: str) -> int:
        if not isinstance(value, int) or value <= 0:
            raise DataError(f"{field} must be a positive integer")
        return value

    @staticmethod
    def _coerce_fraction(value: object) -> float:
        if isinstance(value, (int, float)):
            fraction = float(value)
            if 0.0 < fraction < 1.0:
                return fraction
        raise DataError("train_fraction must be between 0 and 1 (exclusive)")

    def _generate_dataset(self, rng: np.random.Generator, num_games: int) -> list[str]:
        lines: list[str] = []
        for _ in range(num_games):
            board = np.zeros((BOARD_ROWS, BOARD_COLUMNS), dtype=np.int8)
            current_player = _PLAYER_ONE
            move_index = 0
            while True:
                available = [c for c in range(BOARD_COLUMNS) if board[0, c] == 0]
                if not available:
                    break
                state_before = board.copy()
                column = int(rng.choice(available))
                row = self._drop_disc(board, column, current_player)
                lines.append(
                    self._encode_record(state_before, current_player, column, move_index)
                )
                move_index += 1
                if self._is_winning_move(board, row, column, current_player):
                    break
                if move_index >= BOARD_SIZE:
                    break
                current_player = _PLAYER_TWO if current_player == _PLAYER_ONE else _PLAYER_ONE
        return lines

    @staticmethod
    def _split_records(records: Sequence[str], train_fraction: float) -> tuple[list[str], list[str]]:
        total = len(records)
        train_count = max(1, min(total - 1, int(total * train_fraction)))
        train_lines = list(records[:train_count])
        val_lines = list(records[train_count:])
        if not val_lines:
            raise DataError("Validation split produced no examples; adjust train_fraction")
        return train_lines, val_lines

    @staticmethod
    def _build_tokenizer(train_text: str, val_text: str):
        charset = sorted(set(train_text + val_text))
        vocab = {ch: idx for idx, ch in enumerate(charset)}
        return create_tokenizer("char", vocab=vocab)

    @staticmethod
    def _drop_disc(board: np.ndarray, column: int, player: int) -> int:
        for row in range(BOARD_ROWS - 1, -1, -1):
            if board[row, column] == 0:
                board[row, column] = player
                return row
        raise DataError(f"Column {column} is full")

    @staticmethod
    def _encode_record(board: np.ndarray, player: int, column: int, move_index: int) -> str:
        flat = "".join(str(int(cell)) for cell in board.flatten())
        player_token = "A" if player == _PLAYER_ONE else "B"
        move_token = chr(ord("a") + column)
        ply_token = chr(ord("0") + (move_index % 10))
        return f"{flat}{player_token}{move_token}{ply_token}\n"

    @staticmethod
    def _is_winning_move(board: np.ndarray, row: int, column: int, player: int) -> bool:
        directions = ((1, 0), (0, 1), (1, 1), (1, -1))
        for dr, dc in directions:
            if ConnectFourPreparer._count_aligned(board, row, column, player, dr, dc) >= 4:
                return True
        return False

    @staticmethod
    def _count_aligned(
        board: np.ndarray, row: int, column: int, player: int, dr: int, dc: int
    ) -> int:
        count = 1
        for step in (1, -1):
            r, c = row, column
            while True:
                r += dr * step
                c += dc * step
                if 0 <= r < BOARD_ROWS and 0 <= c < BOARD_COLUMNS and board[r, c] == player:
                    count += 1
                else:
                    break
        return count


__all__ = ["ConnectFourPreparer"]
