"""Tests for the Connect Four dataset preparer."""

from __future__ import annotations

import pickle
from pathlib import Path
from typing import List

from ml_playground.configuration.models import PreparerConfig
from ml_playground.experiments.connect_four.game import Board
from ml_playground.experiments.connect_four.preparer import ConnectFourPreparer


def _simple_draw_game() -> str:
    board = Board()
    start = board.render()
    board.drop(3, 1)
    after_move = board.render()
    return "\n".join(["[START]", start, "[MOVE:3]", after_move, "[DRAW]"])


def test_preparer_with_provided_games(tmp_path: Path) -> None:
    ds_dir = tmp_path / "datasets"
    cfg = PreparerConfig(
        extras={
            "base_dir": tmp_path,
            "games_texts": [_simple_draw_game()],
            "augment": False,
        }
    )

    report = ConnectFourPreparer().prepare(cfg)

    train_path = ds_dir / "train.bin"
    val_path = ds_dir / "val.bin"
    meta_path = ds_dir / "meta.pkl"
    games_path = ds_dir / "games.txt"

    for path in (train_path, val_path, meta_path, games_path):
        assert path.exists()
        assert path.stat().st_size > 0

    with meta_path.open("rb") as f:
        meta = pickle.load(f)
    assert meta["tokenizer_type"] == "char"
    assert meta["train_tokens"] > 0
    assert any(message.startswith("[connect_four]") for message in report.messages)

    games_text = games_path.read_text(encoding="utf-8")
    assert "[DRAW]" in games_text


def test_preparer_invokes_custom_generator(tmp_path: Path) -> None:
    calls: List[tuple[int, int | None, bool]] = []

    def generator(num_games: int, seed: int | None = None, augment: bool = True):
        calls.append((num_games, seed, augment))
        return [_simple_draw_game()]

    cfg = PreparerConfig(
        extras={
            "base_dir": tmp_path,
            "game_generator": generator,
            "num_games": 4,
            "seed": 17,
            "augment": False,
            "force_regen": True,
        }
    )

    ConnectFourPreparer().prepare(cfg)
    assert calls == [(4, 17, False)]
