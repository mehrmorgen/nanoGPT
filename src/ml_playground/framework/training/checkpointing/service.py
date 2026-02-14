"""Checkpoint management helpers for the training loop."""

from __future__ import annotations

import shutil
from pathlib import Path
from typing import Any, Callable, Optional, cast

import torch
from torch.optim import Optimizer

from ml_playground.framework.training.checkpointing.checkpoint_manager import (
    Checkpoint,
    CheckpointManager,
)
from ml_playground.framework.configuration.models import (
    TrainerConfig,
    MetadataConfig,
    READ_POLICY_BEST,
)
from ml_playground.framework.core.error_handling import CheckpointError
from ml_playground.framework.core.logging_protocol import LoggerLike
from ml_playground.framework.models.core.model import GPT
from ml_playground.framework.training.ema import EMA


__all__ = [
    "create_manager",
    "load_checkpoint",
    "apply_checkpoint",
    "save_checkpoint",
    "propagate_metadata",
]

DEFAULT_COPY_FN: Callable[[Path, Path], None] = cast(
    Callable[[Path, Path], None], shutil.copy2
)


def create_manager(cfg: TrainerConfig, metadata: MetadataConfig) -> CheckpointManager:
    """Construct a checkpoint manager respecting the retention policy."""
    return CheckpointManager(
        out_dir=metadata.train_out_dir,
        atomic=cfg.runtime.ckpt_atomic,
        keep_last=cfg.runtime.checkpointing.keep.last,
        keep_best=cfg.runtime.checkpointing.keep.best,
    )


def load_checkpoint(
    manager: CheckpointManager,
    cfg: TrainerConfig,
    *,
    logger: LoggerLike,
) -> Optional[Checkpoint]:
    """Load the latest or best checkpoint according to the read policy."""
    # DI override if provided
    if cfg.checkpoint_load_fn is not None:
        try:
            # Use explicit cast on the call result to satisfy basedpyright strict mode
            raw_ckpt = cast(
                object, cfg.checkpoint_load_fn(manager=manager, cfg=cfg, logger=logger)
            )
            return cast(Optional[Checkpoint], raw_ckpt)
        except (
            CheckpointError,
            RuntimeError,
        ) as exc:
            logger.warning(f"checkpoint_load_fn failed: {exc}")
            return None

    if not manager.out_dir.exists():
        return None

    try:
        if cfg.runtime.checkpointing.read_policy == READ_POLICY_BEST:
            return manager.load_best_checkpoint(
                device=cfg.runtime.device, logger=logger
            )
        return manager.load_latest_checkpoint(device=cfg.runtime.device, logger=logger)
    except CheckpointError as exc:
        logger.warning(
            f"Could not load checkpoint ({cfg.runtime.checkpointing.read_policy}): {exc}"
        )
        return None


def apply_checkpoint(
    checkpoint: Checkpoint,
    *,
    model: GPT,
    optimizer: Optimizer,
    ema: Optional[EMA],
) -> tuple[int, float]:
    """Apply checkpoint state to the model/optimizer and return iteration metrics."""
    model.load_state_dict(checkpoint.model, strict=False)
    optimizer.load_state_dict(cast(dict[str, Any], checkpoint.optimizer))
    iter_num = checkpoint.iter_num
    best_val_loss = checkpoint.best_val_loss
    if ema and checkpoint.ema:
        ema.shadow = cast(dict[str, torch.Tensor], checkpoint.ema)
    return iter_num, best_val_loss


def save_checkpoint(
    manager: CheckpointManager,
    cfg: TrainerConfig,
    *,
    model: GPT,
    optimizer: Optimizer,
    ema: Optional[EMA],
    iter_num: int,
    best_val_loss: float,
    logger: LoggerLike,
    is_best: bool,
) -> None:
    """Persist the current training state via the checkpoint manager."""
    checkpoint = Checkpoint(
        model=dict(model.state_dict()),
        optimizer=dict(optimizer.state_dict()),
        model_args=cfg.model.model_dump(),
        iter_num=iter_num,
        best_val_loss=best_val_loss,
        config=cfg.model_dump(),
        ema=dict(ema.shadow) if ema else None,
    )
    # DI override if provided
    if cfg.checkpoint_save_fn is not None:
        try:
            cfg.checkpoint_save_fn(
                manager=manager,
                cfg=cfg,
                checkpoint=checkpoint,
                is_best=is_best,
                logger=logger,
            )
            return
        except (
            CheckpointError,
            RuntimeError,
        ) as exc:
            logger.warning(
                f"checkpoint_save_fn failed, falling back to default save: {exc}"
            )

    manager.save_checkpoint(
        checkpoint,
        metric=best_val_loss,
        iter_num=iter_num,
        logger=logger,
        is_best=is_best,
    )


def propagate_metadata(
    cfg: TrainerConfig,
    metadata: MetadataConfig,
    *,
    logger: LoggerLike,
    copy_fn: Callable[[Path, Path], None] = DEFAULT_COPY_FN,
) -> None:
    """Copy dataset metadata into train and sample output directories when available."""
    try:
        meta_src = cfg.data.meta_path(metadata.dataset_dir)
    except (
        OSError,
        ValueError,
        TypeError,
        RuntimeError,
    ) as exc:
        if logger:
            logger.warning(f"Failed to resolve meta source path: {exc}")
        return

    if not meta_src or not meta_src.exists():
        return

    destinations = [metadata.train_out_dir]
    if metadata.sample_out_dir not in destinations:
        destinations.append(metadata.sample_out_dir)

    for dst_dir in destinations:
        try:
            dst_dir.mkdir(parents=True, exist_ok=True)
            copy_fn(meta_src, dst_dir / meta_src.name)
        except (OSError, IOError) as exc:
            if logger:
                logger.warning(f"Failed to copy meta file to {dst_dir}: {exc}")
