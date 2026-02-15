"""Primary training loop orchestration."""

from __future__ import annotations

import time
from contextlib import AbstractContextManager
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Protocol, cast

import torch
from torch.amp.grad_scaler import GradScaler
from torch.optim import Optimizer

from ml_playground.framework.configuration.models import (
    TrainerConfig,
    MetadataConfig,
    LRSchedule,
    OptimConfig,
)
from ml_playground.framework.training.ema import EMA
from ml_playground.framework.models.core.model import GPT

from ml_playground.framework.core.error_handling import CheckpointError
from ml_playground.framework.core.logging_protocol import LoggerLike
from ml_playground.framework.training.checkpointing.checkpoint_manager import (
    Checkpoint,
    CheckpointManager,
)
from ml_playground.framework.training.checkpointing.service import (
    apply_checkpoint,
    create_manager,
    load_checkpoint,
    propagate_metadata,
    save_checkpoint,
)
from ml_playground.framework.training.hooks.components import initialize_components
from ml_playground.framework.training.hooks.data import initialize_batches
from ml_playground.framework.training.hooks.evaluation import run_evaluation
from ml_playground.framework.training.hooks.logging import log_training_step
from ml_playground.framework.training.hooks.model import initialize_model
from ml_playground.framework.training.hooks.runtime import RuntimeContext, setup_runtime
from ml_playground.framework.training.loop.scheduler import get_lr
from ml_playground.framework.training.types import (
    BatchProvider,
    OptimizerLike,
    ScaledLoss,
    TensorboardWriter,
    VectorizeFn,
)


__all__ = ["Trainer", "get_lr"]


TTrainStep = Callable[["Trainer", torch.Tensor, torch.Tensor], torch.Tensor]
VectorizeProvider = Callable[[], VectorizeFn | None]


class InitializeComponentsFn(Protocol):
    def __call__(
        self,
        model: GPT,
        cfg: TrainerConfig,
        runtime: RuntimeContext,
        *,
        log_dir: str,
    ) -> tuple[GPT, GradScaler, EMA | None, TensorboardWriter | None]: ...


@dataclass(frozen=True)
class TrainerDependencies:
    initialize_batches: Callable[[TrainerConfig, MetadataConfig], BatchProvider]
    initialize_model: Callable[[TrainerConfig, LoggerLike], tuple[GPT, OptimizerLike]]
    initialize_components: InitializeComponentsFn
    create_manager: Callable[[TrainerConfig, MetadataConfig], CheckpointManager]
    load_checkpoint: Callable[..., Checkpoint | None]
    apply_checkpoint: Callable[..., tuple[int, float]]
    save_checkpoint: Callable[..., None]
    propagate_metadata: Callable[..., None]
    run_evaluation: Callable[..., dict[str, float]]
    get_lr: Callable[[int, LRSchedule, OptimConfig], float]
    vectorize: VectorizeProvider


def default_trainer_dependencies(
    *,
    initialize_components_fn: InitializeComponentsFn | None = None,
    vectorize: VectorizeFn | None = None,
) -> TrainerDependencies:
    def _init_components(
        model: GPT,
        cfg: TrainerConfig,
        runtime: RuntimeContext,
        *,
        log_dir: str,
    ) -> tuple[GPT, GradScaler, EMA | None, TensorboardWriter | None]:
        target = initialize_components_fn or initialize_components
        return target(model, cfg, runtime, log_dir=log_dir)

    def _vectorize() -> VectorizeFn | None:
        if vectorize is not None:
            return vectorize
        return cast(VectorizeFn | None, getattr(torch, "vmap", None))

    return TrainerDependencies(
        initialize_batches=initialize_batches,
        initialize_model=initialize_model,
        initialize_components=_init_components,
        create_manager=create_manager,
        load_checkpoint=load_checkpoint,
        apply_checkpoint=apply_checkpoint,
        save_checkpoint=save_checkpoint,
        propagate_metadata=propagate_metadata,
        run_evaluation=run_evaluation,
        get_lr=get_lr,
        vectorize=_vectorize,
    )


class Trainer:
    """Coordinate the end-to-end training loop for a configured experiment."""

    def __init__(
        self,
        cfg: TrainerConfig,
        metadata: MetadataConfig,
        deps: TrainerDependencies | None = None,
    ):
        self.cfg: TrainerConfig = cfg
        self.metadata: MetadataConfig = metadata
        self.logger: LoggerLike = cfg.logger

        self.deps: TrainerDependencies = deps or default_trainer_dependencies()

        self.runtime: RuntimeContext = setup_runtime(cfg)
        self.ctx: AbstractContextManager[object] = self.runtime.autocast_context
        self.device_type: str = self.runtime.device_type

        self.batches: BatchProvider = self.deps.initialize_batches(cfg, metadata)
        model, optimizer_like = self.deps.initialize_model(cfg, self.logger)
        self.model: GPT = model
        self.optimizer: OptimizerLike = optimizer_like

        self.out_dir: Path = metadata.train_out_dir
        self.out_dir.mkdir(parents=True, exist_ok=True)
        self.ckpt_mgr: CheckpointManager = self.deps.create_manager(cfg, metadata)

        (
            self.model,
            scaler,
            ema,
            writer,
        ) = self.deps.initialize_components(
            self.model,
            cfg,
            self.runtime,
            log_dir=str(self.out_dir),
        )
        self._scaler: GradScaler = scaler
        self._ema: EMA | None = ema
        self._writer: TensorboardWriter | None = writer

        self.iter_num: int = 0
        self.best_val_loss: float = getattr(cfg.runtime, "initial_best_val_loss", 1e9)
        self._train_step_override: TTrainStep | None = None
        self._vectorize_impl: VectorizeFn | None = self.deps.vectorize()

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
    def ema(self) -> EMA | None:
        return self._ema

    @ema.setter
    def ema(self, value: EMA | None) -> None:
        self._ema = value

    @property
    def writer(self) -> TensorboardWriter | None:
        return self._writer

    @writer.setter
    def writer(self, value: TensorboardWriter | None) -> None:
        self._writer = value

    def set_train_step_override(self, override: TTrainStep | None) -> None:
        self._train_step_override = override

    def set_vectorize_impl(self, vectorize_impl: VectorizeFn | None) -> None:
        self._vectorize_impl = vectorize_impl

    def run(self) -> tuple[int, float]:
        """Execute the main training loop until reaching the maximum iteration count."""
        self.logger.info("Starting training loop")
        inputs, targets = self.batches.get_batch("train")
        t0 = time.time()
        local_iter_num = 0
        raw_model = cast(GPT, getattr(self.model, "_orig_mod", self.model))
        running_mfu = -1.0

        should_save_checkpoint = True

        try:
            while True:
                lr = self.deps.get_lr(self.iter_num, self.cfg.schedule, self.cfg.optim)
                for param_group in self.optimizer.param_groups:
                    param_group["lr"] = lr

                if (
                    self.iter_num % self.cfg.runtime.eval_interval == 0
                    and self.cfg.runtime.eval_iters > 0
                ):
                    losses = self.deps.run_evaluation(
                        self.cfg,
                        logger=self.logger,
                        iter_num=self.iter_num,
                        lr=lr,
                        raw_model=raw_model,
                        batches=self.batches,
                        ctx=self.ctx,
                        writer=self.writer,
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

                loss = self._train_step(inputs, targets)
                inputs, targets = self.batches.get_batch("train")

                t1 = time.time()
                dt = t1 - t0
                t0 = t1
                if self.iter_num % self.cfg.runtime.log_interval == 0:
                    running_mfu = log_training_step(
                        self.logger,
                        iter_num=self.iter_num,
                        loss_value=loss.item(),
                        dt=dt,
                        local_iter_num=local_iter_num,
                        raw_model=raw_model,
                        running_mfu=running_mfu,
                        batch_size=self.cfg.data.batch_size,
                        grad_accum_steps=self.cfg.data.grad_accum_steps,
                    )
                    # TensorBoard logging if update mode is 'log'
                    try:
                        if (
                            self.writer
                            and getattr(
                                self.cfg.runtime, "tensorboard_update_mode", "eval"
                            )
                            == "log"
                        ):
                            scaled_loss = loss.item() * self.cfg.data.grad_accum_steps
                            self.writer.add_scalar(
                                "Loss/train", scaled_loss, self.iter_num
                            )
                            self.writer.add_scalar("LR", lr, self.iter_num)
                    except (ValueError, RuntimeError, OSError) as exc:
                        self.logger.debug(
                            "TensorBoard logging skipped due to writer error: %s", exc
                        )

                self.iter_num += 1
                local_iter_num += 1

                if self.iter_num > self.cfg.runtime.max_iters:
                    break

        except KeyboardInterrupt:
            should_save_checkpoint = False
            self.logger.info(
                "Training loop interrupted; skipping final checkpoint save"
            )
            raise
        except BaseException:
            should_save_checkpoint = False
            raise
        finally:
            try:
                if should_save_checkpoint:
                    # iter_num tracks the next loop position after a completed step.
                    # Use a counter one behind for final last-checkpoint naming so
                    # ckpt_last_* reflects the most recently completed iteration.
                    final_counter = max(self.iter_num - 1, 0)
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
                        counter_value=final_counter,
                    )
            except (CheckpointError, RuntimeError, OSError) as exc:
                self.logger.warning(f"Failed to save final checkpoint: {exc}")

            try:
                self.deps.propagate_metadata(
                    self.cfg, self.metadata, logger=self.logger
                )
            except Exception as exc:
                self.logger.warning(f"Failed to propagate meta file: {exc}")

            if self.writer:
                self.writer.close()

        return self.iter_num, self.best_val_loss

    def _train_step(self, inputs: torch.Tensor, targets: torch.Tensor) -> torch.Tensor:
        """Perform a gradient accumulation step and update EMA if configured."""

        override = self._train_step_override
        if override is not None:
            return override(self, inputs, targets)

        grad_steps = int(self.cfg.data.grad_accum_steps)
        if grad_steps <= 0:
            raise ValueError("grad_accum_steps must be a positive integer")

        loss_tensor: torch.Tensor
        if grad_steps == 1:
            loss_tensor = self._train_step_single(inputs, targets)
        else:
            loss_tensor = self._train_step_accum(inputs, targets, grad_steps)

        optimizer = cast(Optimizer, self.optimizer)

        if self.cfg.optim.grad_clip != 0.0:
            self.scaler.unscale_(optimizer)
            _ = torch.nn.utils.clip_grad_norm_(
                self.model.parameters(), self.cfg.optim.grad_clip
            )

        _ = self.scaler.step(optimizer)
        self.scaler.update()
        optimizer.zero_grad(set_to_none=True)

        if self.ema:
            self.ema.update(cast(GPT, getattr(self.model, "_orig_mod", self.model)))
        return loss_tensor.detach()

    def _train_step_single(
        self, inputs: torch.Tensor, targets: torch.Tensor
    ) -> torch.Tensor:
        with self.ctx:
            _logits, loss_tensor = cast(
                tuple[torch.Tensor, torch.Tensor], self.model(inputs, targets)
            )
            scaled_loss = cast(ScaledLoss, self.scaler.scale(loss_tensor))
            scaled_loss.backward()
            return loss_tensor

    def _train_step_accum(
        self, inputs: torch.Tensor, targets: torch.Tensor, grad_steps: int
    ) -> torch.Tensor:
        if grad_steps > 1 and hasattr(torch, "vmap"):
            try:
                return self._train_step_vmap(inputs, targets, grad_steps)
            except RuntimeError:
                # Fallback if vmap cannot be applied (e.g., due to unsupported ops)
                pass
        return self._train_step_python(inputs, targets, grad_steps)

    def _train_step_python(
        self, inputs: torch.Tensor, targets: torch.Tensor, grad_steps: int
    ) -> torch.Tensor:
        loss_tensor = torch.tensor(0.0, device=inputs.device)
        with self.ctx:
            for _ in range(grad_steps):
                _logits, raw_loss = cast(
                    tuple[torch.Tensor, torch.Tensor],
                    self.model(inputs, targets),
                )
                loss_tensor = raw_loss / grad_steps
                scaled_loss = cast(ScaledLoss, self.scaler.scale(loss_tensor))
                scaled_loss.backward()
        return loss_tensor

    def _train_step_vmap(
        self, inputs: torch.Tensor, targets: torch.Tensor, grad_steps: int
    ) -> torch.Tensor:
        x_expanded = inputs.unsqueeze(0).expand(grad_steps, *inputs.shape)
        y_expanded = targets.unsqueeze(0).expand(grad_steps, *targets.shape)

        with self.ctx:

            def _forward(x_batch: torch.Tensor, y_batch: torch.Tensor) -> torch.Tensor:
                _logits, loss_val = cast(
                    tuple[torch.Tensor, torch.Tensor],
                    self.model(x_batch, y_batch),
                )
                return loss_val

            vmap_fn = self._vectorize_impl
            if vmap_fn is None:
                raise RuntimeError("Vectorization requested but unavailable")
            vectorized = vmap_fn(_forward)
            losses = vectorized(x_expanded, y_expanded)

        loss_tensor = losses.mean()
        scaled_loss = cast(ScaledLoss, self.scaler.scale(loss_tensor))
        scaled_loss.backward()
        return loss_tensor
