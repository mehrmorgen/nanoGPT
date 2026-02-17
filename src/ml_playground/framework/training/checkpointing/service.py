"""Checkpointing service API for training loops."""

from __future__ import annotations

import shutil
from pathlib import Path
from typing import Callable, Mapping, cast

import torch

from ml_playground.framework.configuration.models import (
    READ_POLICY_BEST,
    READ_POLICY_LATEST,
    MetadataConfig,
    TrainerConfig,
)
from ml_playground.framework.core.error_handling import CheckpointError
from ml_playground.framework.core.logging_protocol import LoggerLike
from ml_playground.framework.models.core.model import GPT
from ml_playground.framework.training.checkpointing.checkpoint_manager import (
    Checkpoint,
    CheckpointManager,
)
from ml_playground.framework.training.ema import EMA
from ml_playground.framework.training.types import OptimizerLike

__all__ = [
    "apply_checkpoint",
    "create_manager",
    "load_checkpoint",
    "save_checkpoint",
    "propagate_metadata",
]


def create_manager(cfg: TrainerConfig, metadata: MetadataConfig) -> CheckpointManager:
    """Create a checkpoint manager configured for the trainer."""
    runtime = cfg.runtime
    return CheckpointManager(
        out_dir=metadata.train_out_dir,
        atomic=runtime.ckpt_atomic,
        keep_last=runtime.checkpointing.keep.last,
        keep_best=runtime.checkpointing.keep.best,
        naming_policy=runtime.ckpt_naming_policy,
        counter_label=runtime.ckpt_domain_label,
    )


def _build_checkpoint(
    cfg: TrainerConfig,
    *,
    model: GPT,
    optimizer: OptimizerLike,
    ema: EMA | None,
    iter_num: int,
    best_val_loss: float,
) -> Checkpoint:
    model_state = cast(Mapping[str, torch.Tensor], model.state_dict())
    optimizer_state = cast(dict[str, object], optimizer.state_dict())
    model_cfg = model.config
    model_args: dict[str, object] = {
        "n_layer": model_cfg.n_layer,
        "n_head": model_cfg.n_head,
        "n_embd": model_cfg.n_embd,
        "block_size": model_cfg.block_size,
        "bias": model_cfg.bias,
        "vocab_size": model_cfg.vocab_size,
        "dropout": model_cfg.dropout,
    }
    ema_payload = dict(ema.shadow) if ema is not None else None
    cfg_payload = cast(Mapping[str, object], cfg.model_dump())
    return Checkpoint(
        model=model_state,
        optimizer=optimizer_state,
        model_args=model_args,
        iter_num=iter_num,
        best_val_loss=best_val_loss,
        config=cfg_payload,
        ema=ema_payload,
    )


def save_checkpoint(
    manager: CheckpointManager,
    cfg: TrainerConfig,
    *,
    model: GPT,
    optimizer: OptimizerLike,
    ema: EMA | None,
    iter_num: int,
    best_val_loss: float,
    logger: LoggerLike,
    is_best: bool = False,
    counter_value: int | None = None,
) -> None:
    """Save a checkpoint, using DI overrides when configured."""
    checkpoint = _build_checkpoint(
        cfg,
        model=model,
        optimizer=optimizer,
        ema=ema,
        iter_num=iter_num,
        best_val_loss=best_val_loss,
    )

    override = cfg.checkpoint_save_fn
    if override is not None:
        try:
            override(
                manager=manager,
                cfg=cfg,
                checkpoint=checkpoint,
                metric=best_val_loss,
                iter_num=iter_num,
                logger=logger,
                is_best=is_best,
                counter_value=counter_value,
            )
            return
        except Exception as exc:
            logger.warning(
                f"checkpoint_save_fn failed, falling back to default save: {exc}"
            )

    manager.save_checkpoint(
        checkpoint,
        metric=best_val_loss,
        iter_num=iter_num,
        logger=logger,
        is_best=is_best,
        counter_value=counter_value,
    )


def load_checkpoint(
    manager: CheckpointManager,
    cfg: TrainerConfig,
    *,
    logger: LoggerLike,
) -> Checkpoint | None:
    """Load a checkpoint according to the configured read policy."""
    override = cfg.checkpoint_load_fn
    if override is not None:
        try:
            return cast(
                Checkpoint,
                override(manager=manager, cfg=cfg, logger=logger),
            )
        except Exception as exc:
            logger.warning(f"checkpoint_load_fn failed: {exc}")
            return None

    if not manager.out_dir.exists():
        return None

    read_policy = cfg.runtime.checkpointing.read_policy
    label = "best" if read_policy == READ_POLICY_BEST else "latest"
    try:
        if read_policy == READ_POLICY_BEST:
            return manager.load_best_checkpoint(
                device=cfg.runtime.device, logger=logger
            )
        if read_policy == READ_POLICY_LATEST:
            return manager.load_latest_checkpoint(
                device=cfg.runtime.device, logger=logger
            )
        raise ValueError(f"Unsupported read policy: {read_policy}")
    except CheckpointError as exc:
        logger.warning(
            "\n".join(
                (
                    f"Could not load checkpoint ({label}): {exc.message}",
                    f"Reason: {exc.reason}",
                    f"Rationale: {exc.rationale}",
                )
            )
        )
        return None


def apply_checkpoint(
    checkpoint: Checkpoint,
    *,
    model: GPT,
    optimizer: OptimizerLike,
    ema: EMA | None,
) -> tuple[int, float]:
    """Apply checkpoint state to the model, optimizer, and EMA tracker."""
    model_state = cast(Mapping[str, torch.Tensor], checkpoint.model)
    model.load_state_dict(model_state, strict=False)
    optimizer.load_state_dict(cast(dict[str, object], checkpoint.optimizer))
    if ema is not None and checkpoint.ema is not None:
        ema.shadow = cast(dict[str, torch.Tensor], dict(checkpoint.ema))
    return checkpoint.iter_num, checkpoint.best_val_loss


def _copy_meta(src: Path, dst: Path) -> None:
    shutil.copy2(src, dst)


def propagate_metadata(
    cfg: TrainerConfig,
    metadata: MetadataConfig,
    *,
    logger: LoggerLike,
    copy_fn: Callable[[Path, Path], None] = _copy_meta,
) -> None:
    """Copy dataset metadata to training and sampling output directories."""
    if not cfg.runtime.ckpt_write_metadata:
        return

    try:
        meta_src = cfg.data.meta_path(metadata.dataset_dir)
    except Exception as exc:
        logger.warning(f"Failed to resolve meta source path: {exc}")
        return

    if not meta_src.exists():
        return

    dest_dirs = {metadata.train_out_dir, metadata.sample_out_dir}
    for dest_dir in dest_dirs:
        dest_dir.mkdir(parents=True, exist_ok=True)
        dest_path = dest_dir / meta_src.name
        try:
            copy_fn(meta_src, dest_path)
        except (OSError, IOError) as exc:
            logger.warning(f"Failed to copy meta file to {dest_path}: {exc}")
