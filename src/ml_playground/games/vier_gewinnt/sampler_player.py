from __future__ import annotations

import logging
import pathlib
from pathlib import Path
from typing import Callable, List, Tuple, Union, Optional, Type

import numpy as np
import torch

from ml_playground.configuration.loading import load_full_experiment_config
from ml_playground.core.tokenizer import CharTokenizer
from ml_playground.games.vier_gewinnt.engine import VierGewinnt
from ml_playground.games.vier_gewinnt.players import Player
from ml_playground.models.core.model import GPT

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
        # Load experiment configuration
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

        # Initialize model
        model_config = train_cfg.model

        # Load meta.pkl to get tokenizer info and actual vocab_size
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

        # Create a new ModelConfig instance with the updated vocab_size
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
        # Load meta.pkl to get tokenizer info
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

        # Get the current move history from the game
        move_history = game.move_history

        # Convert move history to a string format that the tokenizer understands
        # The training data is "winner:move1,move2,move3,..."
        # So, the input to the model should be "move1,move2,move3,..."
        # We need to prepend a dummy winner and a colon for the tokenizer to work correctly
        # The tokenizer expects a string like "1:0,1,2,..."

        # Create a dummy input string for the tokenizer
        # The actual moves are 0-6. The tokenizer also has ',' and ':'
        # The model is trained on sequences of moves.
        # We need to feed the current sequence of moves to the model.

        # The tokenizer's stoi mapping includes '0' through '6', ',', ':', and '\n'.
        # The model expects a sequence of tokens.

        # Let's construct the input string as "dummy_winner:move1,move2,..."
        # The dummy winner doesn't matter for prediction, only the moves.
        input_str = "1:" + ",".join(map(str, move_history))

        # Encode the input string
        context = torch.tensor(
            self.tokenizer.encode(input_str), dtype=torch.long, device=self.device
        ).unsqueeze(0)

        # Generate the next token (move)
        # The model expects block_size, so we might need to pad or truncate
        # For now, let's assume the model can handle variable length input up to block_size

        # The model will predict the next token. We want it to predict a move (0-6).
        # We need to sample from the logits of the last token.

        # The model's forward pass returns logits.
        # We need to get the logits for the next token.

        # The model is trained to predict the next character in the sequence.
        # If the input is "1:0,1,2", it should predict the next move.

        # Let's use the model's generate method for simplicity, but we need to ensure it samples valid moves.
        # The generate method typically samples from the entire vocabulary.
        # We need to filter for valid moves.

        # For now, let's simplify and just get the logits for the next token
        # and then manually sample a valid move.

        # Ensure the context is not longer than the model's block size
        block_size = self.model.config.block_size
        if context.size(1) > block_size:
            context = context[:, -block_size:]

        # Get logits for the next token
        logits, _ = self.model(context)
        # Pluck the logits at the final step and scale by temperature
        logits = logits[:, -1, :]  # becomes (1, vocab_size)

        # Apply softmax to get probabilities
        probs = torch.nn.functional.softmax(logits, dim=-1)

        # Filter probabilities for valid moves (0-6)
        # The tokenizer maps '0' to 2, '1' to 3, ..., '6' to 8.
        # So, valid token indices are 2 through 8.

        valid_token_indices = [self.tokenizer.stoi[str(move)] for move in valid_moves]

        # Set probabilities of invalid tokens to zero
        filtered_probs = torch.zeros_like(probs)
        for token_idx in valid_token_indices:
            filtered_probs[0, token_idx] = probs[0, token_idx]

        # Resample from filtered probabilities
        if (
            filtered_probs.sum() == 0
        ):  # If all valid moves have zero probability, fall back to random
            return np.random.choice(valid_moves)

        filtered_probs = filtered_probs / filtered_probs.sum()  # Normalize

        # Sample a token index
        next_token_idx = int(torch.multinomial(filtered_probs, num_samples=1).item())

        # Decode the token index back to a move
        predicted_char = self.tokenizer.itos[next_token_idx]

        # Convert the predicted character to an integer move
        try:
            predicted_move = int(predicted_char)
        except ValueError:
            # If the model predicts a non-move character (e.g., ',', ':', '\n'),
            # fall back to a random valid move.
            return np.random.choice(valid_moves)

        if predicted_move in valid_moves:
            return predicted_move
        else:
            # If the predicted move is somehow invalid (e.g., column is full),
            # fall back to a random valid move.
            return np.random.choice(valid_moves)
