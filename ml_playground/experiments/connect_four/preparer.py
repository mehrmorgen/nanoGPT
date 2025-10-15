from __future__ import annotations

import random
from pathlib import Path

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
    split_train_val,
)
from ml_playground.core.tokenizer import create_tokenizer
from ml_playground.experiments.protocol import PrepareReport, Preparer as _PreparerProto

from .game import ConnectFourGame


def generate_connect_four_sequences(
    *, num_games: int, rng: random.Random | None = None
) -> list[str]:
    """Generate serialized Connect Four move sequences."""

    if num_games <= 0:
        raise DataError("num_games must be positive")

    local_rng = rng or random.Random()
    examples: list[str] = []
    for _ in range(num_games):
        game = ConnectFourGame()
        for record in game.play_random_game(local_rng):
            examples.append(f"{record.board_state}{record.column}\n")
    return examples


class ConnectFourPreparer(_PreparerProto):
    """Prepare synthetic Connect Four board/move data."""

    DEFAULT_NUM_GAMES = 2000
    DEFAULT_TRAIN_SPLIT = 0.9

    def prepare(self, cfg: PreparerConfig) -> PrepareReport:  # type: ignore[override]
        extras = cfg.extras or {}
        dataset_dir_override = extras.get("dataset_dir")
        if dataset_dir_override:
            ds_dir = Path(dataset_dir_override)
        else:
            base_dir = extras.get("base_dir")
            exp_dir = Path(base_dir) if base_dir else Path(__file__).resolve().parent
            ds_dir = exp_dir / "datasets"
        ds_dir.mkdir(parents=True, exist_ok=True)

        outputs = [
            ds_dir / "train.bin",
            ds_dir / "val.bin",
            ds_dir / "meta.pkl",
        ]
        before = snapshot_file_states(outputs)

        seed = extras.get("seed")
        rng = random.Random(int(seed)) if seed is not None else random.Random()

        num_games = extras.get("num_games", self.DEFAULT_NUM_GAMES)
        try:
            num_games_int = int(num_games)
        except (TypeError, ValueError) as exc:
            raise DataError("num_games must be an integer") from exc
        if num_games_int <= 0:
            raise DataError("num_games must be positive")

        split_ratio = extras.get("train_split", self.DEFAULT_TRAIN_SPLIT)
        try:
            split_float = float(split_ratio)
        except (TypeError, ValueError) as exc:
            raise DataError("train_split must be a float") from exc
        if not 0 < split_float < 1:
            raise DataError("train_split must be between 0 and 1")

        generator = extras.get("game_generator")

        progress = ProgressReporter(cfg.logger, total_steps=6)
        progress.start("Starting Connect Four dataset preparation")

        progress.update(1, "Generating synthetic games")
        if callable(generator):
            try:
                generated = generator(num_games=num_games_int, rng=rng)
            except TypeError:
                generated = generator(num_games=num_games_int)
            sequences = list(generated)
        else:
            sequences = generate_connect_four_sequences(num_games=num_games_int, rng=rng)

        if not sequences:
            raise DataError("Generated dataset is empty")

        progress.update(1, "Building tokenizer vocabulary")
        dataset_text = "".join(sequences)
        unique_chars = sorted(set(dataset_text))
        if not unique_chars:
            raise DataError("Dataset text has no characters")
        tokenizer = create_tokenizer("char", vocab={ch: idx for idx, ch in enumerate(unique_chars)})

        progress.update(1, "Splitting train and validation text")
        train_text, val_text = split_train_val(dataset_text, split=split_float)

        progress.update(1, "Encoding datasets")
        train_ids = np.array(tokenizer.encode(train_text), dtype=np.uint16)
        val_ids = np.array(tokenizer.encode(val_text), dtype=np.uint16)

        progress.update(1, "Creating metadata")
        meta_extras = {
            "alphabet": "".join(unique_chars),
            "rows": ConnectFourGame.ROWS,
            "cols": ConnectFourGame.COLS,
            "connect": ConnectFourGame.CONNECT,
            "examples": len(sequences),
            "train_split": split_float,
            "tokens_per_example": len(sequences[0]) if sequences else 0,
        }
        meta = create_standardized_metadata(
            tokenizer,
            train_tokens=len(train_ids),
            val_tokens=len(val_ids),
            extras=meta_extras,
        )

        progress.update(1, "Writing artifacts")

        writer = extras.get("writer_fn")
        if callable(writer):
            writer(ds_dir, train_ids, val_ids, meta, logger=cfg.logger)
        else:
            write_bin_and_meta(ds_dir, train_ids, val_ids, meta, logger=cfg.logger)

        progress.finish("Connect Four dataset preparation completed")

        created, updated, skipped = diff_file_states(outputs, before)
        messages = (
            f"[connect_four] prepared dataset at {ds_dir}",
            f"[connect_four.outputs.created] {[str(p) for p in created]}",
            f"[connect_four.outputs.updated] {[str(p) for p in updated]}",
            f"[connect_four.outputs.skipped] {[str(p) for p in skipped]}",
        )
        return PrepareReport(
            created_files=tuple(created),
            updated_files=tuple(updated),
            skipped_files=tuple(skipped),
            messages=messages,
        )
