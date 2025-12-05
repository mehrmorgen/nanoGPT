from __future__ import annotations

import logging
import pathlib
from pathlib import Path
from typing import Callable, List, Tuple, Union, Optional, Type

import numpy as np
import torch

from ml_playground.configuration.loading import load_full_experiment_config
from ml_playground.core.tokenizer import CharTokenizer
from ml_playground.models.core.model import GPT

from .engine import VierGewinnt
from .players import Player

WINDOWS_PATH_CLASS: Optional[Type[Path]] = getattr(pathlib, "WindowsPath", None)

project_root = Path(__file__).resolve().parents[4]
SafeGlobal = Union[Callable[..., object], Tuple[Callable[..., object], str]]


class SamplerPlayer(Player):
    def __init__(self, experiment_name: str, device: str = "cpu"):
        self.experiment_name = experiment_name
        self.device = device
        self.model = self._load_model()
        self.tokenizer = self._load_tokenizer()

    def _load_model(self):
        config_path = (
            project_root
            / "src"
            / "ml_playground"
            / "experiments"
            / self.experiment_name
            / "config.toml"
        )
        exp_config = load_full_experiment_config(
            config_path=config_path,
            project_home=project_root,
            experiment_name=self.experiment_name,
        )
        train_cfg = exp_config.train
        shared_cfg = exp_config.shared

        model_config = train_cfg.model

        meta_path = (
            project_root
            / "src"
            / "ml_playground"
            / "experiments"
            / self.experiment_name
            / "datasets"
            / "meta.pkl"
        )
        import pickle

        with open(meta_path, "rb") as f:
            meta = pickle.load(f)

        updated_model_config = model_config.model_copy(
            update={"vocab_size": meta["vocab_size"]}
        )

        model = GPT(updated_model_config, logger=train_cfg.logger)
        model.to(self.device)

        checkpoint_dir = shared_cfg.train_out_dir
        checkpoint_path = self._resolve_checkpoint_path(checkpoint_dir)

        import torch.serialization

        safe_globals: List[SafeGlobal] = [logging.getLogger]
        if WINDOWS_PATH_CLASS is not None:
            safe_globals.append((lambda: WINDOWS_PATH_CLASS, "WindowsPath"))

        torch.serialization.add_safe_globals(safe_globals)
        checkpoint = torch.load(checkpoint_path, map_location=self.device)
        state_dict = checkpoint["model"]
        model.load_state_dict(state_dict)
        print(f"Loaded model from {checkpoint_path}")

        model.eval()
        return model

    def _load_tokenizer(self):
        meta_path = (
            project_root
            / "src"
            / "ml_playground"
            / "experiments"
            / self.experiment_name
            / "datasets"
            / "meta.pkl"
        )
        import pickle

        with open(meta_path, "rb") as f:
            meta = pickle.load(f)

        stoi = meta["stoi"]
        tokenizer = CharTokenizer(vocab=stoi)
        return tokenizer

    def _resolve_checkpoint_path(self, checkpoint_dir: Path) -> Path:
        best_candidates = sorted(checkpoint_dir.glob("ckpt_best_*.pt"))
        if best_candidates:
            return best_candidates[-1]

        fallback = checkpoint_dir / "ckpt_last_00005001.pt"
        if fallback.exists():
            return fallback

        raise FileNotFoundError(
            f"No checkpoint found for {self.experiment_name} in {checkpoint_dir}"
        )

    def get_move(self, game: VierGewinnt):
        valid_moves = game.get_valid_moves()
        if not valid_moves:
            return -1  # No valid moves

        move_history = game.move_history
        input_str = "1:" + ",".join(map(str, move_history))

        context = torch.tensor(
            self.tokenizer.encode(input_str), dtype=torch.long, device=self.device
        ).unsqueeze(0)

        block_size = self.model.config.block_size
        if context.size(1) > block_size:
            context = context[:, -block_size:]

        logits, _ = self.model(context)
        logits = logits[:, -1, :]

        probs = torch.nn.functional.softmax(logits, dim=-1)

        valid_token_indices = [self.tokenizer.stoi[str(move)] for move in valid_moves]

        filtered_probs = torch.zeros_like(probs)
        for token_idx in valid_token_indices:
            filtered_probs[0, token_idx] = probs[0, token_idx]

        if filtered_probs.sum() == 0:
            return np.random.choice(valid_moves)

        filtered_probs = filtered_probs / filtered_probs.sum()
        next_token_idx = int(torch.multinomial(filtered_probs, num_samples=1).item())

        predicted_char = self.tokenizer.itos[next_token_idx]

        try:
            predicted_move = int(predicted_char)
        except ValueError:
            return np.random.choice(valid_moves)

        if predicted_move in valid_moves:
            return predicted_move
        return np.random.choice(valid_moves)
