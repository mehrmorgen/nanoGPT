from __future__ import annotations

import logging
from collections import deque
from pathlib import Path
from typing import Callable, Iterable

import torch
import torch.nn.functional as F

from ml_playground.configuration.models import SamplerConfig, SharedConfig
from ml_playground.core.error_handling import DataError
from ml_playground.experiments.connect_four.game import ConnectFourGame
from ml_playground.experiments.protocol import (
    SampleReport,
    Sampler as _SamplerProto,
)
from ml_playground.sampling.runner import Sampler as _CoreSampler


def _default_shared(cfg: SamplerConfig) -> SharedConfig:
    runtime = cfg.runtime
    if runtime is None:
        raise ValueError("Runtime configuration is required for sampling")
    exp_dir = Path(__file__).resolve().parent
    out_dir = runtime.out_dir
    dataset_dir = exp_dir / "datasets"
    config_path = exp_dir / "config.toml"
    project_home = exp_dir
    out_dir = out_dir if isinstance(out_dir, Path) else Path(out_dir)
    return SharedConfig(
        experiment="connect_four",
        config_path=config_path,
        project_home=project_home,
        dataset_dir=dataset_dir,
        train_out_dir=out_dir,
        sample_out_dir=out_dir,
    )


def _normalize_moves(moves: Iterable[int | str]) -> deque[int]:
    normalized: deque[int] = deque()
    for move in moves:
        try:
            normalized.append(int(move))
        except (TypeError, ValueError) as exc:
            raise DataError(f"Invalid move provided: {move}") from exc
    return normalized


class ConnectFourSampler(_SamplerProto):
    """Play a Connect Four game against the trained model."""

    def __init__(self) -> None:
        self._logger = logging.getLogger("ml_playground.connect_four.sampler")

    def sample(  # type: ignore[override]
        self, cfg: SamplerConfig, shared: SharedConfig | None = None
    ) -> SampleReport:
        shared_cfg = shared or _default_shared(cfg)
        extras = cfg.extras or {}

        sampler_factory = extras.get("sampler_factory")
        core_sampler = None
        if callable(sampler_factory):
            core_sampler = sampler_factory(cfg, shared_cfg)
        if core_sampler is None:
            core_sampler = _CoreSampler(cfg, shared_cfg)

        input_fn: Callable[[str], str] = extras.get("input_fn", input)
        human_moves = _normalize_moves(extras.get("human_moves", []))
        policy = extras.get("policy", "sample").lower()
        human_player = extras.get("human_player", "X").upper()
        if human_player not in {"X", "O"}:
            raise DataError("human_player must be 'X' or 'O'")

        logger = getattr(cfg, "logger", self._logger)
        logger.info("[connect_four] launching interactive sampler")
        game = ConnectFourGame()

        model_token = "O" if human_player == "X" else "X"
        transcript: list[str] = []

        while not game.is_over():
            board_view = game.render()
            logger.info("\n%s", board_view)
            if game.current_player_token == human_player:
                column = self._next_human_move(game, human_moves, input_fn, logger)
                transcript.append(f"human({human_player}) -> {column}")
            else:
                column, prob = self._model_move(core_sampler, game, cfg, policy)
                transcript.append(
                    f"model({model_token}) -> {column} (p={prob:.3f})"
                )
                logger.info(
                    "Model selects column %s with probability %.3f", column, prob
                )
            game.make_move(column)

        result_message = self._describe_result(game, human_player)
        logger.info(result_message)
        transcript.append(result_message)
        return SampleReport(messages=tuple(transcript))

    # ------------------------------------------------------------------
    # Human interaction helpers
    # ------------------------------------------------------------------
    def _next_human_move(
        self,
        game: ConnectFourGame,
        queue: deque[int],
        input_fn: Callable[[str], str],
        logger,
    ) -> int:
        valid = game.valid_moves()
        if queue:
            column = queue.popleft()
            if column not in valid:
                raise DataError(f"Column {column} is not available")
            logger.info("Human selects column %s (from scripted moves)", column)
            return column

        prompt = f"Choose a column {valid}: "
        while True:
            try:
                response = input_fn(prompt)
            except EOFError as exc:
                raise DataError("No move provided for human player") from exc
            try:
                column = int(response.strip())
            except (TypeError, ValueError):
                logger.info("Invalid column: %s", response)
                continue
            if column in valid:
                logger.info("Human selects column %s", column)
                return column
            logger.info("Column %s is not available", column)

    # ------------------------------------------------------------------
    # Model policy helpers
    # ------------------------------------------------------------------
    def _model_move(
        self,
        core_sampler: _CoreSampler,
        game: ConnectFourGame,
        cfg: SamplerConfig,
        policy: str,
    ) -> tuple[int, float]:
        valid_moves = game.valid_moves()
        if not valid_moves:
            raise DataError("No valid moves for model")

        device = core_sampler.runtime_cfg.device
        board_tokens = core_sampler.tokenizer.encode(game.board_to_string())
        if not board_tokens:
            raise DataError("Tokenizer returned empty encoding for board state")
        input_ids = torch.tensor(board_tokens, dtype=torch.long, device=device)[
            None, :
        ]
        with torch.no_grad():
            logits = core_sampler.model(input_ids)
        last_logits = logits[0, -1, :]

        temperature = max(cfg.sample.temperature, 1e-5)
        scaled = last_logits / temperature

        top_k = cfg.sample.top_k
        if top_k > 0 and top_k < scaled.numel():
            values, indices = torch.topk(scaled, top_k)
            mask = torch.full_like(scaled, float("-inf"))
            mask.scatter_(0, indices, values)
            scaled = mask

        probs = F.softmax(scaled, dim=-1)

        move_tokens: dict[int, int] = {}
        for column in range(game.COLS):
            encoded = core_sampler.tokenizer.encode(str(column))
            if len(encoded) != 1:
                raise DataError("Tokenizer must map each column to exactly one token")
            move_tokens[column] = encoded[0]

        weights = torch.tensor(
            [probs[move_tokens[m]].item() for m in valid_moves], dtype=torch.float32
        )
        if weights.sum() <= 0 or policy == "greedy":
            idx = int(torch.argmax(weights).item())
            probability = float(weights[idx])
            return valid_moves[idx], probability

        distribution = torch.distributions.Categorical(probs=weights)
        choice = int(distribution.sample().item())
        probability = float(weights[choice])
        return valid_moves[choice], probability

    @staticmethod
    def _describe_result(game: ConnectFourGame, human_token: str) -> str:
        if game.winner == 0:
            return "[connect_four] result: draw"
        if game.winner is None:
            return "[connect_four] result: unfinished"
        winner_token = "X" if game.winner == 1 else "O"
        if winner_token == human_token:
            return f"[connect_four] result: human ({human_token}) wins"
        return f"[connect_four] result: model ({winner_token}) wins"
