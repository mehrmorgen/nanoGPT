from __future__ import annotations

import pickle
from pathlib import Path
from typing import Iterable, Sequence

import numpy as np

from ml_playground.configuration.models import PreparerConfig
from ml_playground.core.error_handling import DataError, ProgressReporter
from ml_playground.data_pipeline.transforms.io import (
    diff_file_states,
    snapshot_file_states,
    write_bin_and_meta,
)
from ml_playground.experiments.protocol import (
    Preparer as _PreparerProto,
    PrepareReport,
)


class ConnectFourPreparer(_PreparerProto):
    """Prepare a synthetic Connect Four dataset of board states and moves."""

    ROWS = 6
    COLS = 7
    BOARD_SIZE = ROWS * COLS
    EMPTY_TOKEN = 0
    CURRENT_PLAYER_TOKEN = 1
    OPPONENT_TOKEN = 2
    MOVE_OFFSET = 3
    SEPARATOR_TOKEN = 10
    META_VERSION = 1

    def prepare(self, cfg: PreparerConfig) -> PrepareReport:  # type: ignore[override]
        extras = getattr(cfg, "extras", {}) or {}
        base_dir_override = (
            extras.get("dataset_dir_override")
            or extras.get("base_dir_override")
            or extras.get("base_dir")
        )
        if base_dir_override is not None:
            exp_dir = Path(base_dir_override)
        else:
            exp_dir = Path(__file__).resolve().parent
        ds_dir = exp_dir / "datasets"
        ds_dir.mkdir(parents=True, exist_ok=True)
        outputs = [ds_dir / "train.bin", ds_dir / "val.bin", ds_dir / "meta.pkl"]

        force_rebuild = bool(extras.get("force_rebuild", False))
        if not force_rebuild and self._artifacts_look_valid(outputs):
            msgs = (
                f"[connect_four] dataset already prepared at {ds_dir}; skipping.",
                "[connect_four.outputs.created] []",
                "[connect_four.outputs.updated] []",
                f"[connect_four.outputs.skipped] {[str(p) for p in outputs]}",
            )
            return PrepareReport(
                created_files=tuple(),
                updated_files=tuple(),
                skipped_files=tuple(outputs),
                messages=msgs,
            )

        try:
            num_games = int(extras.get("num_games", 10000))
        except (TypeError, ValueError) as exc:
            raise DataError("prepare.extras.num_games must be an integer") from exc
        if num_games <= 0:
            raise DataError("prepare.extras.num_games must be >= 1")

        try:
            val_split = float(extras.get("val_split", 0.1))
        except (TypeError, ValueError) as exc:
            raise DataError("prepare.extras.val_split must be a float between 0 and 1") from exc
        if not (0.0 < val_split < 1.0):
            raise DataError("prepare.extras.val_split must be between 0 and 1 (exclusive)")

        seed_raw = extras.get("seed", 1337)
        try:
            seed = int(seed_raw)
        except (TypeError, ValueError) as exc:
            raise DataError("prepare.extras.seed must be an integer") from exc
        rng = np.random.default_rng(seed)

        progress = ProgressReporter(cfg.logger, total_steps=4)
        progress.start("Starting Connect Four dataset preparation")

        progress.update(1, "Generating synthetic games")
        samples = self._generate_samples(num_games, rng)
        if not samples:
            raise DataError("No training samples were generated; check generation parameters")

        samples_arr = np.stack(samples).astype(np.uint16)
        if samples_arr.shape[0] < 2:
            raise DataError(
                "Need at least two samples to create train/val splits; increase prepare.extras.num_games"
            )
        rng.shuffle(samples_arr)

        progress.update(1, "Splitting train/val sets")
        train_count = max(1, int(round(samples_arr.shape[0] * (1.0 - val_split))))
        if train_count >= samples_arr.shape[0]:
            train_count = samples_arr.shape[0] - 1
        if train_count <= 0:
            raise DataError(
                "Training split resulted in zero samples; adjust prepare.extras.val_split or num_games"
            )
        val_count = samples_arr.shape[0] - train_count
        if val_count <= 0:
            raise DataError(
                "Validation split resulted in zero samples; adjust prepare.extras.val_split"
            )

        train_samples = samples_arr[:train_count]
        val_samples = samples_arr[train_count:]

        train_ids = train_samples.reshape(-1).astype(np.uint16, copy=False)
        val_ids = val_samples.reshape(-1).astype(np.uint16, copy=False)

        meta = self._build_meta(train_ids.size, val_ids.size)

        pre_state = snapshot_file_states(outputs)

        progress.update(1, "Writing dataset artifacts")
        write_bin_and_meta(ds_dir, train_ids, val_ids, meta, logger=cfg.logger)

        created, updated, skipped = diff_file_states(outputs, pre_state)

        progress.finish("Connect Four dataset preparation completed")

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

    def _generate_samples(
        self, num_games: int, rng: np.random.Generator
    ) -> list[np.ndarray]:
        samples: list[np.ndarray] = []
        for _ in range(num_games):
            samples.extend(self._simulate_game(rng))
        return samples

    def _simulate_game(self, rng: np.random.Generator) -> list[np.ndarray]:
        board = np.zeros((self.ROWS, self.COLS), dtype=np.int8)
        player = 1
        moves_played = 0
        max_moves = self.BOARD_SIZE
        game_samples: list[np.ndarray] = []
        while moves_played < max_moves:
            valid_cols = [c for c in range(self.COLS) if board[0, c] == 0]
            if not valid_cols:
                break
            col = int(rng.choice(valid_cols))
            sample = self._encode_sample(board, player, col)
            game_samples.append(sample)
            row = self._drop_token(board, col, player)
            moves_played += 1
            if row < 0:
                break
            if self._check_winner(board, player, row, col):
                break
            player = 2 if player == 1 else 1
        return game_samples

    def _encode_sample(self, board: np.ndarray, player: int, col: int) -> np.ndarray:
        board_view = np.where(board == 0, self.EMPTY_TOKEN, board)
        if player == 1:
            board_view = np.where(board == 1, self.CURRENT_PLAYER_TOKEN, board_view)
            board_view = np.where(board == 2, self.OPPONENT_TOKEN, board_view)
        else:
            board_view = np.where(board == 2, self.CURRENT_PLAYER_TOKEN, board_view)
            board_view = np.where(board == 1, self.OPPONENT_TOKEN, board_view)

        sample = np.empty(self.BOARD_SIZE + 2, dtype=np.uint16)
        sample[: self.BOARD_SIZE] = board_view.reshape(-1)
        sample[self.BOARD_SIZE] = self.SEPARATOR_TOKEN
        sample[self.BOARD_SIZE + 1] = self.MOVE_OFFSET + col
        return sample

    def _drop_token(self, board: np.ndarray, col: int, player: int) -> int:
        for row in range(self.ROWS - 1, -1, -1):
            if board[row, col] == 0:
                board[row, col] = player
                return row
        return -1

    def _check_winner(self, board: np.ndarray, player: int, row: int, col: int) -> bool:
        directions: Sequence[tuple[int, int]] = (
            (0, 1),
            (1, 0),
            (1, 1),
            (1, -1),
        )
        for dr, dc in directions:
            count = 1
            count += self._count_direction(board, player, row, col, dr, dc)
            count += self._count_direction(board, player, row, col, -dr, -dc)
            if count >= 4:
                return True
        return False

    def _count_direction(
        self,
        board: np.ndarray,
        player: int,
        row: int,
        col: int,
        dr: int,
        dc: int,
    ) -> int:
        count = 0
        r, c = row + dr, col + dc
        while 0 <= r < self.ROWS and 0 <= c < self.COLS and board[r, c] == player:
            count += 1
            r += dr
            c += dc
        return count

    def _build_meta(self, train_tokens: int, val_tokens: int) -> dict[str, object]:
        itos: dict[int, str] = {
            self.EMPTY_TOKEN: "empty",
            self.CURRENT_PLAYER_TOKEN: "current",
            self.OPPONENT_TOKEN: "opponent",
            self.SEPARATOR_TOKEN: "sep",
        }
        for col in range(self.COLS):
            itos[self.MOVE_OFFSET + col] = f"move_{col}"
        stoi = {name: token for token, name in itos.items()}
        meta = {
            "meta_version": self.META_VERSION,
            "tokenizer_type": "connect_four",
            "tokenizer": "connect_four",
            "vocab_size": max(itos.keys()) + 1,
            "train_tokens": int(train_tokens),
            "val_tokens": int(val_tokens),
            "has_encode": False,
            "has_decode": False,
            "dtype": "uint16",
            "board_rows": self.ROWS,
            "board_cols": self.COLS,
            "board_size": self.BOARD_SIZE,
            "separator_token": self.SEPARATOR_TOKEN,
            "move_token_offset": self.MOVE_OFFSET,
            "itos": itos,
            "stoi": stoi,
            "description": "Connect Four board-state followed by move token",
        }
        return meta

    def _artifacts_look_valid(self, outputs: Iterable[Path]) -> bool:
        outputs = list(outputs)
        if not outputs:
            return False
        for path in outputs:
            if not path.exists():
                return False
            try:
                if path.stat().st_size == 0:
                    return False
            except OSError:
                return False
        meta_path = outputs[-1]
        try:
            with meta_path.open("rb") as fh:
                meta = pickle.load(fh)
        except (OSError, pickle.UnpicklingError, EOFError):
            return False
        if not isinstance(meta, dict):
            return False
        return (
            meta.get("meta_version") == self.META_VERSION
            and meta.get("tokenizer_type") == "connect_four"
        )
