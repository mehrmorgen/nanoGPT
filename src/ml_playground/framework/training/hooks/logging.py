"""Logging helpers for the training loop."""

from __future__ import annotations

from ml_playground.framework.core.logging_protocol import LoggerLike
from ml_playground.framework.models.core.model import GPT


__all__ = ["log_training_step"]


def _format_eta_seconds(total_seconds: float) -> str:
    whole_seconds = max(0, int(total_seconds))
    hours, rem = divmod(whole_seconds, 3600)
    minutes, seconds = divmod(rem, 60)
    return f"{hours:02d}:{minutes:02d}:{seconds:02d}"


def log_training_step(
    logger: LoggerLike,
    iter_num: int,
    loss_value: float,
    dt: float,
    local_iter_num: int,
    raw_model: GPT,
    running_mfu: float,
    batch_size: int,
    grad_accum_steps: int,
    max_iters: int,
    running_dt_ema: float | None,
    dt_ema_alpha: float,
    elapsed_seconds: float = 0.0,
) -> tuple[float, float | None]:
    """Log training progress and compute updated model FLOPS utilization."""
    scaled_loss = loss_value * grad_accum_steps
    updated_dt_ema = (
        dt
        if running_dt_ema is None
        else (1.0 - dt_ema_alpha) * running_dt_ema + dt_ema_alpha * dt
    )
    remaining_iters = max(0, max_iters - iter_num)
    eta = _format_eta_seconds(updated_dt_ema * remaining_iters)
    elapsed = _format_eta_seconds(elapsed_seconds)

    if local_iter_num >= 5:
        mfu = raw_model.estimate_mfu(batch_size * grad_accum_steps, dt)
        running_mfu = (
            mfu if running_mfu == -1.0 else 0.9 * running_mfu + 0.1 * float(mfu)
        )

    mfu_pct = max(0.0, min(float(running_mfu), 100.0))
    logger.info(
        f"iter {iter_num}: loss {scaled_loss:.4f}, time {dt * 1000:.2f}ms, mfu {mfu_pct:.2f}%, elapsed {elapsed}, eta {eta}"
    )
    return running_mfu, updated_dt_ema
