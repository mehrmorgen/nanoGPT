from __future__ import annotations
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterable
import random

from ml_playground.configuration.models import PreparerConfig
from ml_playground.core.error_handling import DataError, ProgressReporter
from ml_playground.core.tokenizer import CharTokenizer
from ml_playground.data_pipeline.transforms.io import (
    diff_file_states,
    snapshot_file_states,
    write_bin_and_meta,
)
from ml_playground.data_pipeline.transforms.tokenization import prepare_with_tokenizer
from ml_playground.experiments.protocol import (
    Preparer as _PreparerProto,
    PrepareReport,
)


@dataclass(slots=True)
class ConnectFourGame:
    """Lightweight Connect Four board for synthetic data generation."""

    rows: int = 6
    cols: int = 7
    board: list[list[int]] = field(init=False)

    def __post_init__(self) -> None:
        object.__setattr__(
            self, "board", [[0 for _ in range(self.cols)] for _ in range(self.rows)]
        )

    def reset(self) -> None:
        for r in range(self.rows):
            for c in range(self.cols):
                self.board[r][c] = 0

    def is_valid_move(self, col: int) -> bool:
        return 0 <= col < self.cols and self.board[0][col] == 0

    def get_valid_moves(self) -> list[int]:
        return [col for col in range(self.cols) if self.is_valid_move(col)]

    def make_move(self, col: int, player: int) -> bool:
        if not self.is_valid_move(col):
            return False
        for row in range(self.rows - 1, -1, -1):
            if self.board[row][col] == 0:
                self.board[row][col] = player
                return True
        return False

    def check_winner(self, player: int) -> bool:
        target = player
        # Horizontal
        for r in range(self.rows):
            for c in range(self.cols - 3):
                if all(self.board[r][c + offset] == target for offset in range(4)):
                    return True
        # Vertical
        for c in range(self.cols):
            for r in range(self.rows - 3):
                if all(self.board[r + offset][c] == target for offset in range(4)):
                    return True
        # Diagonal down-right
        for r in range(self.rows - 3):
            for c in range(self.cols - 3):
                if all(self.board[r + offset][c + offset] == target for offset in range(4)):
                    return True
        # Diagonal up-right
        for r in range(3, self.rows):
            for c in range(self.cols - 3):
                if all(self.board[r - offset][c + offset] == target for offset in range(4)):
                    return True
        return False

    def is_full(self) -> bool:
        return all(self.board[0][c] != 0 for c in range(self.cols))

    def board_string(self) -> str:
        return "".join(str(cell) for row in self.board for cell in row)


class ConnectFourPreparer(_PreparerProto):
    """Prepare synthetic Connect Four board → move examples."""

    DEFAULT_NUM_GAMES = 10_000
    SEPARATOR = "|"

    def prepare(self, cfg: PreparerConfig) -> PrepareReport:  # type: ignore[override]
        extras = getattr(cfg, "extras", {}) or {}
        base_dir = extras.get("base_dir")
        exp_dir = Path(base_dir) if base_dir else Path(__file__).resolve().parent
        ds_dir = exp_dir / "datasets"
        ds_dir.mkdir(parents=True, exist_ok=True)

        outputs = [ds_dir / "train.bin", ds_dir / "val.bin", ds_dir / "meta.pkl"]
        pre_snapshot = snapshot_file_states(outputs)

        num_games = self._coerce_positive_int(extras.get("num_games"), self.DEFAULT_NUM_GAMES)
        rng_seed = extras.get("random_seed")
        rng = random.Random(rng_seed if rng_seed is not None else 1337)
        game_template = ConnectFourGame()

        progress = ProgressReporter(cfg.logger, total_steps=4)
        progress.start("Preparing Connect Four dataset")

        progress.update(1, "Generating synthetic games")
        examples = list(self._generate_examples(num_games, rng=rng))
        if not examples:
            raise DataError("Connect Four preparer produced no training examples")

        progress.update(1, "Formatting sequences")
        sequences = [f"{board}{self.SEPARATOR}{move}" for board, move in examples]
        corpus = "\n".join(sequences)

        progress.update(1, "Encoding train/val splits")
        tokenizer = CharTokenizer()
        train_arr, val_arr, meta, _ = prepare_with_tokenizer(corpus, tokenizer)
        meta.setdefault("connect_four", {})
        meta["connect_four"] = {
            "rows": game_template.rows,
            "cols": game_template.cols,
            "separator": self.SEPARATOR,
            "num_examples": len(sequences),
        }

        progress.update(1, "Writing dataset artifacts")
        write_bin_and_meta(ds_dir, train_arr, val_arr, meta, logger=cfg.logger)

        progress.finish("Connect Four dataset preparation complete")

        created, updated, skipped = diff_file_states(outputs, pre_snapshot)
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

    def _generate_examples(
        self, num_games: int, *, rng: random.Random
    ) -> Iterable[tuple[str, str]]:
        game = ConnectFourGame()
        for _ in range(num_games):
            game.reset()
            current_player = 1
            while True:
                valid_moves = game.get_valid_moves()
                if not valid_moves:
                    break
                board_before = game.board_string()
                move = rng.choice(valid_moves)
                yield board_before, str(move)
                game.make_move(move, current_player)
                if game.check_winner(current_player):
                    break
                current_player = 3 - current_player
                if game.is_full():
                    break

    @staticmethod
    def _coerce_positive_int(value: object, default: int) -> int:
        if value is None:
            return default
        try:
            result = int(value)
        except (TypeError, ValueError) as exc:
            raise DataError("extras.num_games must be an integer") from exc
        if result <= 0:
            raise DataError("extras.num_games must be positive")
        return result
