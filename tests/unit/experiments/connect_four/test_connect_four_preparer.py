from __future__ import annotations

import pickle
import random
from pathlib import Path

from ml_playground.configuration.models import PreparerConfig
from ml_playground.experiments.connect_four.preparer import (
    ConnectFourGame,
    ConnectFourPreparer,
)


def test_make_move_stacks_tokens() -> None:
    game = ConnectFourGame()
    assert game.make_move(3, 1)
    assert game.board[5][3] == 1
    assert game.make_move(3, 2)
    assert game.board[4][3] == 2


def test_check_winner_detects_lines() -> None:
    game = ConnectFourGame()
    for col in range(4):
        game.make_move(col, 1)
    assert game.check_winner(1)

    game.reset()
    for _ in range(4):
        game.make_move(0, 2)
    assert game.check_winner(2)

    game.reset()
    diag_coords = [(5, 0), (4, 1), (3, 2), (2, 3)]
    for r, c in diag_coords:
        game.board[r][c] = 1
    assert game.check_winner(1)


def test_generate_examples_shape() -> None:
    preparer = ConnectFourPreparer()
    examples = list(preparer._generate_examples(1, rng=random.Random(0)))
    assert examples
    board, move = examples[0]
    assert len(board) == 42
    assert set(board) <= {"0", "1", "2"}
    assert move in {str(i) for i in range(7)}


def test_prepare_writes_artifacts(tmp_path: Path) -> None:
    extras = {"base_dir": tmp_path, "num_games": 2, "random_seed": 7}
    cfg = PreparerConfig(extras=extras)

    report = ConnectFourPreparer().prepare(cfg)

    ds_dir = tmp_path / "datasets"
    train_path = ds_dir / "train.bin"
    val_path = ds_dir / "val.bin"
    meta_path = ds_dir / "meta.pkl"

    for path in (train_path, val_path, meta_path):
        assert path.exists()
        assert path.stat().st_size > 0

    meta = pickle.loads(meta_path.read_bytes())
    assert meta["tokenizer_type"] == "char"
    connect_meta = meta.get("connect_four")
    assert connect_meta is not None
    assert connect_meta["rows"] == 6
    assert connect_meta["cols"] == 7
    assert connect_meta["separator"] == "|"
    assert connect_meta["num_examples"] > 0

    assert set(report.created_files) == {train_path, val_path, meta_path}
    assert not report.updated_files
    assert not report.skipped_files
