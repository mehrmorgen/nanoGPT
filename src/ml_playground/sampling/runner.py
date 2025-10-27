"""ml_playground.sampler: sampling utilities.

Device seeding/TF32 is centrally handled in the CLI. This module constructs
device, dtype, and autocast contexts locally without exposing legacy shims.
"""

from __future__ import annotations
from dataclasses import dataclass
from contextlib import AbstractContextManager, nullcontext
from pathlib import Path
from typing import Callable, Iterable, List, Protocol, Sequence, cast
import logging
import torch
from torch import autocast

from ml_playground.training.checkpointing.checkpoint_manager import (
    Checkpoint,
    CheckpointManager,
)
from ml_playground.configuration.models import (
    ModelConfig,
    SamplerConfig,
    READ_POLICY_BEST,
    RuntimeConfig,
    SharedConfig,
)
from ml_playground.core.error_handling import DataError, FileOperationError
from ml_playground.models.core.model import GPT
from ml_playground.data_pipeline.transforms.io import setup_tokenizer


class TokenizerProtocol(Protocol):
    def encode(self, text: str) -> list[int]: ...

    def decode(self, token_ids: Sequence[int]) -> str: ...

    def decode_tensor(self, token_tensor: torch.Tensor) -> str: ...


"""
Centralized sampling utilities for ml_playground experiments.

This module provides standardized utilities for model sampling including:
- Checkpoint loading with proper error handling
- Error handling with centralized exception types

All experiments should use these utilities to ensure consistency and proper error handling.
"""


@dataclass(frozen=True)
class SamplerDependencies:
    cuda_is_available: Callable[[], bool]
    cuda_manual_seed: Callable[[int], None]


def default_sampler_dependencies() -> SamplerDependencies:
    def _cuda_is_available() -> bool:
        cuda_module = getattr(torch, "cuda", None)
        if cuda_module is None:
            return False
        return bool(cuda_module.is_available())

    def _cuda_manual_seed(seed: int) -> None:
        cuda_module = getattr(torch, "cuda", None)
        if cuda_module is None:
            return
        manual_seed = getattr(cuda_module, "manual_seed", None)
        if callable(manual_seed):
            manual_seed(seed)

    return SamplerDependencies(
        cuda_is_available=_cuda_is_available,
        cuda_manual_seed=_cuda_manual_seed,
    )


class Sampler:
    """Generate samples from a trained `GPT` model using a strict configuration."""

    def __init__(
        self,
        cfg: SamplerConfig,
        shared: SharedConfig,
        *,
        deps: SamplerDependencies | None = None,
    ):
        """Instantiate the sampler and eagerly load required runtime state.

        Args:
            cfg: Fully validated sampler configuration produced by the CLI.
            shared: Shared experiment metadata, including dataset and output directories.

        Raises:
            ValueError: If the runtime section of the configuration is missing.
            DataError: If tokenizer metadata cannot be located.
        """
        self.cfg = cfg
        self.shared = shared
        self.deps: SamplerDependencies = deps or default_sampler_dependencies()
        runtime_cfg = cast(RuntimeConfig | None, getattr(cfg, "runtime", None))
        if runtime_cfg is None:
            raise ValueError("Runtime configuration is missing")
        self.runtime_cfg = runtime_cfg
        self.sample_cfg = cfg.sample

        self.out_dir = shared.sample_out_dir
        # Use a stable, module-level logger name for predictable capture in tests
        self.logger = logging.getLogger("ml_playground.sampler")

        self._setup_torch_env()

        self.model = self._load_checkpoint_and_model()
        self.tokenizer: TokenizerProtocol = self._setup_tokenizer()
        self._prompt_tensor: torch.Tensor | None = None
        self._cached_prompt_ids: tuple[int, ...] | None = None

    def _setup_torch_env(self) -> None:
        """Seed global torch RNG state and prepare autocast context."""
        manual_seed = cast(Callable[[int], object], torch.manual_seed)
        manual_seed(self.runtime_cfg.seed)
        # Guard CUDA-specific calls for non-CUDA environments
        try:
            if self.deps.cuda_is_available():
                self.deps.cuda_manual_seed(self.runtime_cfg.seed)
        except (RuntimeError, AssertionError, AttributeError):
            pass

        self.device_type = "cuda" if "cuda" in self.runtime_cfg.device else "cpu"
        pt_dtype = {
            "float32": torch.float32,
            "bfloat16": torch.bfloat16,
            "float16": torch.float16,
        }[self.runtime_cfg.dtype]
        self.ctx = cast(
            AbstractContextManager[object],
            nullcontext()
            if self.device_type == "cpu"
            else autocast(device_type=self.device_type, dtype=pt_dtype),
        )

    def _load_checkpoint_and_model(self) -> GPT:
        """Load the configured checkpoint and materialize a `GPT` model."""
        checkpoint = self._load_checkpoint()
        model = self._init_model_from_checkpoint(checkpoint)
        if getattr(self.runtime_cfg, "compile", False):
            compile_fn = getattr(self.cfg, "compile_model_fn", None)
            if compile_fn is None:
                raise ValueError(
                    "SamplerConfig.compile_model_fn must be provided when runtime.compile is True"
                )
            model = cast(GPT, compile_fn(model))  # type: ignore[call-arg]
        return model

    def _load_checkpoint(self) -> Checkpoint:
        """Load a checkpoint according to the configured read policy.

        Uses DI hook `cfg.checkpoint_load_fn` if provided, otherwise defaults
        to manager's load_latest/load_best.
        """
        ckpt_mgr = CheckpointManager(out_dir=self.out_dir)
        # DI override
        if self.cfg.checkpoint_load_fn is not None:
            return self.cfg.checkpoint_load_fn(
                manager=ckpt_mgr, cfg=self.cfg, logger=self.logger
            )

        if self.runtime_cfg.checkpointing.read_policy == READ_POLICY_BEST:
            return ckpt_mgr.load_best_checkpoint(
                device=self.runtime_cfg.device, logger=self.logger
            )
        return ckpt_mgr.load_latest_checkpoint(
            device=self.runtime_cfg.device, logger=self.logger
        )

    def _init_model_from_checkpoint(self, checkpoint: Checkpoint) -> GPT:
        model_cfg = ModelConfig(**checkpoint.model_args)
        # DI override for model factory
        if self.cfg.model_factory is not None:
            model = self.cfg.model_factory(model_cfg, self.logger)
        else:
            model = GPT(model_cfg, self.logger)
        model.load_state_dict(checkpoint.model, strict=False)
        model.eval()
        model.to(self.runtime_cfg.device)
        return model

    def _setup_tokenizer(self):
        """Load tokenizer metadata from the sampling output directory."""
        tokenizer = setup_tokenizer(self.out_dir)
        if tokenizer:
            return cast(TokenizerProtocol, tokenizer)
        raise DataError(
            f"Tokenizer metadata not found in sampling output directory: {self.out_dir}.\n"
            "Expected 'meta.pkl' to exist. Run 'train' first to propagate metadata to the sampling directory.",
            reason="Sampling directory missing meta.pkl",
            rationale="Sampling assumes training artifacts were synced so tokenizer configuration is available",
        )

    def _get_start_ids(self) -> list[int]:
        """Resolve the configured prompt source and tokenize it."""
        start_text = self.sample_cfg.start
        if start_text.startswith("FILE:"):
            prompt_path = Path(start_text[5:])
            try:
                start_text = prompt_path.read_text(encoding="utf-8")
            except (OSError, IOError) as e:
                # Replace bare except with explicit file operation error
                raise FileOperationError(
                    f"Failed to read prompt file {prompt_path}: {e}",
                    reason=f"{e.__class__.__name__} raised while reading prompt file",
                    rationale="Sampling requires the prompt file to be readable to seed generation",
                ) from e
        encoded = self.tokenizer.encode(start_text)
        return [int(token) for token in encoded]

    def run(self) -> None:
        """Generate one or more samples and stream them through the logger."""
        start_ids = self._get_start_ids()
        if not start_ids:
            return

        prompt_ids = tuple(start_ids)
        device = torch.device(self.runtime_cfg.device)
        if (
            self._prompt_tensor is None
            or self._cached_prompt_ids != prompt_ids
            or self._prompt_tensor.device != device
        ):
            self._prompt_tensor = torch.as_tensor(
                start_ids, dtype=torch.long, device=device
            ).unsqueeze(0)
            self._cached_prompt_ids = prompt_ids

        x = self._prompt_tensor

        self.logger.info("Sampling...")
        with torch.no_grad():
            with self.ctx:
                for _ in range(self.sample_cfg.num_samples):
                    y = self.model.generate(
                        x,
                        self.sample_cfg.max_new_tokens,
                        temperature=self.sample_cfg.temperature,
                        top_k=self.sample_cfg.top_k,
                    )
                    output_tensor = y[0].detach().cpu()
                    output = self._decode_tokens(output_tensor)
                    self.logger.info(output)
                    self.logger.info("---------------")

    def _decode_tokens(self, token_tensor: torch.Tensor) -> str:
        tensor = token_tensor
        if tensor.dtype != torch.long:
            tensor = tensor.to(torch.long)
        if tensor.device.type != "cpu":
            tensor = tensor.cpu()

        decoder = getattr(self.tokenizer, "decode_tensor", None)
        if callable(decoder):
            return cast(str, decoder(tensor))

        flat_tensor = tensor.flatten()
        normalized: List[int] = [
            int(val.item()) for val in cast(Iterable[torch.Tensor], flat_tensor)
        ]
        return self.tokenizer.decode(normalized)


def sample(
    cfg: SamplerConfig,
    shared: SharedConfig | None = None,
    *,
    deps: SamplerDependencies | None = None,
) -> None:
    """Run sampling with optional shared configuration fallback."""

    if shared is None:
        runtime = cast(RuntimeConfig | None, getattr(cfg, "runtime", None))
        if runtime is None:
            raise ValueError("Runtime configuration is missing")
        out_dir = runtime.out_dir
        shared = SharedConfig(
            experiment="unknown",
            config_path=out_dir / "config.toml",
            project_home=out_dir.parent if out_dir.parent else out_dir,
            dataset_dir=out_dir,
            train_out_dir=out_dir,
            sample_out_dir=out_dir,
        )

    sampler_instance = Sampler(cfg, shared, deps=deps)
    sampler_instance.run()


__all__ = [
    "Sampler",
    "SamplerDependencies",
    "default_sampler_dependencies",
    "sample",
]
