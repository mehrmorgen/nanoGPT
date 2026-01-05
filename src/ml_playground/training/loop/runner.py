"""Primary training loop orchestration."""

from __future__ import annotations

import os
import platform
import sys
import time
from contextlib import AbstractContextManager
from dataclasses import dataclass
from typing import Any, Callable, Dict, Optional, Tuple, cast

import torch
from torch.amp.grad_scaler import GradScaler

from ml_playground.configuration.models import (
    TrainerConfig,
    SharedConfig,
    LRSchedule,
    OptimConfig,
    RuntimeConfig,
)
from ml_playground.core.logging_protocol import LoggerLike
from ml_playground.training.ema import EMA
from ml_playground.models.core.model import GPT

from ml_playground.core.error_handling import CheckpointError
from ml_playground.training.checkpointing.service import (
    apply_checkpoint,
    create_manager,
    load_checkpoint,
    propagate_metadata,
    save_checkpoint,
)
from ml_playground.training.hooks.components import initialize_components
from ml_playground.training.hooks.data import initialize_batches
from ml_playground.training.hooks.evaluation import run_evaluation
from ml_playground.training.hooks.logging import log_training_step
from ml_playground.training.hooks.model import initialize_model
from ml_playground.training.hooks.runtime import RuntimeContext, setup_runtime
from ml_playground.training.loop.scheduler import get_lr
from ml_playground.training.mlflow_integration import MLflowManager


__all__ = ["Trainer", "train", "get_lr"]


@dataclass(frozen=True)
class TrainerDependencies:
    initialize_batches: Callable[[TrainerConfig, SharedConfig], Any]
    initialize_model: Callable[[TrainerConfig, Any], Tuple[Any, Any]]
    initialize_components: Callable[
        [Any, TrainerConfig, RuntimeContext, str],
        Tuple[Any, GradScaler, Optional[EMA]],
    ]
    create_manager: Callable[[TrainerConfig, SharedConfig], Any]
    create_mlflow_manager: Callable[
        [RuntimeConfig, SharedConfig, LoggerLike], MLflowManager
    ]
    load_checkpoint: Callable[..., Optional[Any]]
    apply_checkpoint: Callable[..., Tuple[int, float]]
    save_checkpoint: Callable[..., None]
    propagate_metadata: Callable[..., None]
    run_evaluation: Callable[..., Dict[str, float]]
    get_lr: Callable[[int, LRSchedule, OptimConfig], float]
    train_step: Optional[Callable[[Any, torch.Tensor, torch.Tensor], torch.Tensor]] = (
        None
    )
    vmap: Optional[
        Callable[
            [Callable[[torch.Tensor, torch.Tensor], torch.Tensor]],
            Callable[[torch.Tensor, torch.Tensor], torch.Tensor],
        ]
    ] = None


def default_trainer_dependencies() -> TrainerDependencies:
    def _init_components(
        model: Any,
        cfg: TrainerConfig,
        runtime: RuntimeContext,
        log_dir: str,
    ) -> Tuple[Any, GradScaler, Optional[EMA]]:
        return initialize_components(model, cfg, runtime, log_dir=log_dir)

    return TrainerDependencies(
        initialize_batches=initialize_batches,
        initialize_model=initialize_model,
        initialize_components=_init_components,
        create_manager=create_manager,
        create_mlflow_manager=lambda runtime, shared, logger: MLflowManager(
            runtime,
            shared,
            logger,
            os_module=os,
            platform_module=platform,
            sys_module=sys,
        ),
        load_checkpoint=load_checkpoint,
        apply_checkpoint=apply_checkpoint,
        save_checkpoint=save_checkpoint,
        propagate_metadata=propagate_metadata,
        run_evaluation=run_evaluation,
        get_lr=get_lr,
        vmap=getattr(torch, "vmap", None),
    )


class Trainer:
    """Coordinate the end-to-end training loop for a configured experiment."""

    def __init__(
        self,
        cfg: TrainerConfig,
        shared: SharedConfig,
        deps: Optional[TrainerDependencies] = None,
    ):
        self.cfg = cfg
        self.shared = shared
        self.logger = cfg.logger

        self.deps = deps or default_trainer_dependencies()
        self._vmap = self.deps.vmap

        self.runtime: RuntimeContext = setup_runtime(cfg)
        self.ctx: AbstractContextManager[object] = self.runtime.autocast_context
        self.device_type = self.runtime.device_type

        self.batches = self.deps.initialize_batches(cfg, shared)
        self.model, self.optimizer = self.deps.initialize_model(cfg, self.logger)

        self.out_dir = shared.train_out_dir
        self.out_dir.mkdir(parents=True, exist_ok=True)
        self.ckpt_mgr = self.deps.create_manager(cfg, shared)

        (
            self.model,
            self.scaler,
            self.ema,
        ) = self.deps.initialize_components(
            self.model,
            cfg,
            self.runtime,
            str(self.out_dir),
        )

        self.iter_num = 0
        self.local_iter_num = 0
        self.running_mfu = -1.0
        self.best_val_loss = 1e9

        self.mlflow = self.deps.create_mlflow_manager(cfg.runtime, shared, self.logger)
        self.mlflow.setup()
        self.mlflow.log_config(cfg)

        checkpoint = self.deps.load_checkpoint(self.ckpt_mgr, cfg, logger=self.logger)
        if checkpoint:
            self.iter_num, self.best_val_loss = self.deps.apply_checkpoint(
                checkpoint,
                model=cast(GPT, getattr(self.model, "_orig_mod", self.model)),
                optimizer=self.optimizer,
                ema=self.ema,
            )

    @property
    def scaler(self) -> GradScaler:
        return self._scaler

    @scaler.setter
    def scaler(self, value: GradScaler) -> None:
        self._scaler = value

    @property
    def ema(self) -> Optional[EMA]:
        return self._ema

    @ema.setter
    def ema(self, value: Optional[EMA]) -> None:
        self._ema = value

    def run(self) -> Tuple[int, float]:
        """Execute the main training loop until reaching the maximum iteration count."""
        self.logger.info("Starting training loop")
        try:
            X, Y = self.batches.get_batch("train")
        except KeyboardInterrupt:
            self.logger.info("Training loop interrupted")
            should_save_checkpoint = False
            self.mlflow.finish()
            raise

        t0 = time.time()
        raw_model = cast(GPT, getattr(self.model, "_orig_mod", self.model))
        should_save_checkpoint = True

        try:
            while True:
                # Check if we should run evaluation
                if self.iter_num % self.cfg.runtime.eval_interval == 0:
                    losses = self.deps.run_evaluation(
                        self.cfg,
                        logger=self.logger,
                        iter_num=self.iter_num,
                        lr=self.optimizer.param_groups[0]["lr"],
                        raw_model=raw_model,
                        batches=self.batches,
                        ctx=self.ctx,
                    )
                    if losses["val"] < self.best_val_loss:
                        self.best_val_loss = losses["val"]
                        if self.iter_num > 0:
                            self.deps.save_checkpoint(
                                self.ckpt_mgr,
                                self.cfg,
                                model=raw_model,
                                optimizer=self.optimizer,
                                ema=self.ema,
                                iter_num=self.iter_num,
                                best_val_loss=self.best_val_loss,
                                logger=self.logger,
                                is_best=True,
                            )

                if self.iter_num == 0 and self.cfg.runtime.eval_only:
                    break

                # Termination condition
                if self.iter_num >= self.cfg.runtime.max_iters:
                    break

                # Optimization step
                lr = self.deps.get_lr(self.iter_num, self.cfg.schedule, self.cfg.optim)
                for param_group in self.optimizer.param_groups:
                    param_group["lr"] = lr

                # Forward backward update
                try:
                    if self.deps.train_step:
                        loss = self.deps.train_step(self, X, Y)
                    else:
                        loss = self._train_step(X, Y)
                except KeyboardInterrupt:
                    raise
                except Exception as exc:
                    self.logger.error(
                        f"Training step failed at iteration {self.iter_num}: {exc}"
                    )
                    raise

                # Fetch next batch
                X, Y = self.batches.get_batch("train")

                # Logging
                if self.iter_num % self.cfg.runtime.log_interval == 0:
                    t1 = time.time()
                    dt = t1 - t0
                    t0 = t1
                    self.running_mfu = log_training_step(
                        logger=self.logger,
                        iter_num=self.iter_num,
                        loss_value=loss.item(),
                        dt=dt,
                        local_iter_num=self.local_iter_num,
                        raw_model=raw_model,
                        running_mfu=self.running_mfu,
                        batch_size=self.cfg.data.batch_size,
                        grad_accum_steps=self.cfg.data.grad_accum_steps,
                    )
                    self.mlflow.log_metrics(
                        {"loss": loss.item(), "lr": lr}, step=self.iter_num
                    )

                self.iter_num += 1
                self.local_iter_num += 1

        except KeyboardInterrupt:
            should_save_checkpoint = False
            self.logger.info("Training loop interrupted")
            raise
        except BaseException:
            should_save_checkpoint = False
            raise
        finally:
            try:
                if should_save_checkpoint:
                    self.deps.save_checkpoint(
                        self.ckpt_mgr,
                        self.cfg,
                        model=raw_model,
                        optimizer=self.optimizer,
                        ema=self.ema,
                        iter_num=self.iter_num,
                        best_val_loss=self.best_val_loss,
                        logger=self.logger,
                        is_best=False,
                    )
            except (CheckpointError, RuntimeError, OSError) as exc:
                self.logger.warning(f"Failed to save final checkpoint: {exc}")

            try:
                self.deps.propagate_metadata(self.cfg, self.shared, logger=self.logger)
            except Exception as exc:
                self.logger.warning(f"Failed to propagate meta file: {exc}")

            self.mlflow.finish()

        return self.iter_num, self.best_val_loss

    def _train_step(self, X: torch.Tensor, Y: torch.Tensor) -> torch.Tensor:
        """Perform a gradient accumulation step and update EMA if configured."""

        grad_steps = int(self.cfg.data.grad_accum_steps)
        if grad_steps <= 0:
            raise ValueError("grad_accum_steps must be a positive integer")

        loss_tensor: torch.Tensor
        if grad_steps == 1:
            loss_tensor = self._train_step_single(X, Y)
        else:
            loss_tensor = self._train_step_accum(X, Y, grad_steps)

        if self.cfg.optim.grad_clip != 0.0:
            self.scaler.unscale_(self.optimizer)
            torch.nn.utils.clip_grad_norm_(
                self.model.parameters(), self.cfg.optim.grad_clip
            )

        self.scaler.step(self.optimizer)
        self.scaler.update()
        self.optimizer.zero_grad(set_to_none=True)

        if self.ema:
            self.ema.update(cast(GPT, getattr(self.model, "_orig_mod", self.model)))
        return loss_tensor.detach()

    def _train_step_single(self, X: torch.Tensor, Y: torch.Tensor) -> torch.Tensor:
        with self.ctx:
            _, loss_tensor = self.model(X, Y)
        self.scaler.scale(loss_tensor).backward()
        return loss_tensor

    def _train_step_accum(
        self, X: torch.Tensor, Y: torch.Tensor, grad_steps: int
    ) -> torch.Tensor:
        if grad_steps > 1 and self._vmap is not None:
            try:
                return self._train_step_vmap(X, Y, grad_steps)
            except RuntimeError:
                # Fallback if vmap cannot be applied (e.g., due to unsupported ops)
                pass
        return self._train_step_python(X, Y, grad_steps)

    def _train_step_python(
        self, X: torch.Tensor, Y: torch.Tensor, grad_steps: int
    ) -> torch.Tensor:
        loss_tensor = torch.tensor(0.0, device=X.device)
        with self.ctx:
            for _ in range(grad_steps):
                _, loss_tensor = self.model(X, Y)
                loss_tensor = loss_tensor / grad_steps
                self.scaler.scale(loss_tensor).backward()
        return loss_tensor

    def _train_step_vmap(
        self, X: torch.Tensor, Y: torch.Tensor, grad_steps: int
    ) -> torch.Tensor:
        if self._vmap is None:
            raise RuntimeError("vmap is unavailable for this trainer instance")
        x_expanded = X.unsqueeze(0).expand(grad_steps, *X.shape)
        y_expanded = Y.unsqueeze(0).expand(grad_steps, *Y.shape)

        with self.ctx:

            def _forward(x_batch: torch.Tensor, y_batch: torch.Tensor) -> torch.Tensor:
                _, loss_val = self.model(x_batch, y_batch)
                return loss_val

            vmap_fn = self._vmap(_forward)
            losses = vmap_fn(x_expanded, y_expanded)

        loss_tensor = losses.mean()
        self.scaler.scale(loss_tensor).backward()
        return loss_tensor


def train(
    cfg: TrainerConfig,
    shared: SharedConfig | None = None,
    *,
    deps: TrainerDependencies | None = None,
) -> Tuple[int, float]:
    """Run the strict trainer with optional shared metadata fallback."""
    if shared is None:
        out_dir = cfg.runtime.out_dir
        shared = SharedConfig(
            experiment="unknown",
            config_path=out_dir / "cfg.toml",
            project_home=out_dir.parent if out_dir.parent else out_dir,
            dataset_dir=out_dir,
            train_out_dir=out_dir,
            sample_out_dir=out_dir,
        )

    trainer = Trainer(cfg, shared, deps=deps)
    return trainer.run()
