"""Dataset preparer for the Connect Four experiment."""

from __future__ import annotations

from pathlib import Path
from typing import Callable, Iterable, Sequence, cast

import numpy as np

from ml_playground.configuration.models import PreparerConfig
from ml_playground.core.error_handling import ProgressReporter
from ml_playground.core.tokenizer import create_tokenizer
from ml_playground.core.tokenizer_protocol import Tokenizer
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

from .data_generator import generate_games, join_games

DEFAULT_GAME_COUNT = 10_000


class ConnectFourPreparer(_PreparerProto):
    """Generate and tokenize Connect Four self-play game data."""

    def prepare(self, cfg: PreparerConfig) -> PrepareReport:  # type: ignore[override]
        extras = cfg.extras or {}

        base_dir = extras.get("base_dir")
        exp_dir = Path(base_dir) if base_dir else Path(__file__).resolve().parent
        ds_dir = exp_dir / "datasets"
        ds_dir.mkdir(parents=True, exist_ok=True)

        games_path = ds_dir / "games.txt"
        train_path = ds_dir / "train.bin"
        val_path = ds_dir / "val.bin"
        meta_path = ds_dir / "meta.pkl"

        outputs = [train_path, val_path, meta_path, games_path]
        snapshot = snapshot_file_states(outputs)

        progress = ProgressReporter(cfg.logger, total_steps=6)
        progress.start("Preparing Connect Four dataset")

        force_regen = bool(extras.get("force_regen", False))
        num_games = int(extras.get("num_games", DEFAULT_GAME_COUNT))
        augment = bool(extras.get("augment", True))
        seed = extras.get("seed")
        if seed is not None:
            seed = int(seed)

        games_text_override = extras.get("games_texts")
        game_generator = extras.get("game_generator")
        if game_generator is not None and not callable(game_generator):
            raise ValueError("game_generator must be callable if provided")

        if games_text_override is not None:
            if not isinstance(games_text_override, Iterable):
                raise ValueError("games_texts must be an iterable of strings")
            progress.update(1, "Using provided games_texts override")
            games_sequences = [str(item) for item in games_text_override]
            games_text = join_games(games_sequences)
            games_path.write_text(games_text, encoding="utf-8")
        else:
            regenerate = force_regen or not games_path.exists()
            if regenerate:
                progress.update(1, "Generating self-play games")
                if callable(game_generator):
                    generator = cast(
                        Callable[[int, int | None, bool], Sequence[str]], game_generator
                    )
                    raw_sequences = list(
                        generator(num_games, seed=seed, augment=augment)
                    )
                else:
                    raw_sequences = generate_games(num_games, seed=seed, augment=augment)
                sequences = [str(item) for item in raw_sequences]
                games_text = join_games(sequences)
                games_path.write_text(games_text, encoding="utf-8")
            else:
                progress.update(1, "Loading cached games")
                games_text = games_path.read_text(encoding="utf-8")

        progress.update(1, "Splitting train/val text")
        train_text, val_text = split_train_val(games_text)

        progress.update(1, "Building tokenizer vocabulary")
        tokenizer = _build_char_tokenizer(train_text, val_text)

        progress.update(1, "Encoding train tokens")
        train_ids = np.array(tokenizer.encode(train_text), dtype=np.uint16)

        progress.update(1, "Encoding validation tokens")
        val_ids = np.array(tokenizer.encode(val_text), dtype=np.uint16)

        progress.update(1, "Writing binary dataset")
        meta = create_standardized_metadata(
            tokenizer,
            train_tokens=len(train_ids),
            val_tokens=len(val_ids),
            extras={"description": "Connect Four random self-play"},
        )
        write_bin_and_meta(ds_dir, train_ids, val_ids, meta, logger=cfg.logger)

        progress.finish("Connect Four dataset ready")

        created, updated, skipped = diff_file_states(outputs, snapshot)
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


def _build_char_tokenizer(train_text: str, val_text: str) -> Tokenizer:
    all_text = train_text + val_text
    chars = sorted(set(all_text))
    if not chars:
        raise ValueError("No characters available to build tokenizer vocabulary")
    vocab = {ch: idx for idx, ch in enumerate(chars)}
    return create_tokenizer("char", vocab=vocab)
