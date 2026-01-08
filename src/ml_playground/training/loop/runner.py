"""Primary training loop orchestration."""

from __future__ import annotations

import time
from contextlib import AbstractContextManager
from dataclasses import dataclass, field
from typing import Callable, Dict, Optional, Protocol, Tuple, cast

import torch
from torch.amp.grad_scaler import GradScaler

from ml_playground.configuration.models import (
    TrainerConfig,
    SharedConfig,
    LRSchedule,
    OptimConfig,
)
from ml_playground.training.ema import EMA
from ml_playground.models.core.model import GPT

from ml_playground.core.error_handling import CheckpointError
from ml_playground.core.logging_protocol import LoggerLike
from ml_playground.training.checkpointing.service import (
    apply_checkpoint,
    create_manager,
    load_checkpoint,
    propagate_metadata,
    save_checkpoint,
)
from ml_playground.training.checkpointing.checkpoint_manager import (
    Checkpoint,
    CheckpointManager,
)
from ml_playground.training.hooks.components import initialize_components
from ml_playground.training.hooks.data import initialize_batches
from ml_playground.training.hooks.evaluation import run_evaluation
from ml_playground.training.hooks.logging import log_training_step
from ml_playground.training.hooks.model import initialize_model
from ml_playground.training.hooks.runtime import RuntimeContext, setup_runtime
from ml_playground.training.loop.scheduler import get_lr
from ml_playground.training.types import (
    BatchProvider,
    OptimizerLike,
    ScaledLoss,
    TensorboardWriter,
    VectorizeFn,
)


__all__ = ["Trainer", "train", "get_lr"]


class LoadCheckpointFn(Protocol):
    def __call__(
        self, manager: CheckpointManager, cfg: TrainerConfig, *, logger: LoggerLike
    ) -> Checkpoint | None: ...


class ApplyCheckpointFn(Protocol):
    def __call__(
        self,
        checkpoint: Checkpoint,
        *,
        model: GPT,
        optimizer: OptimizerLike,
        ema: EMA | None,
    ) -> tuple[int, float]: ...


class SaveCheckpointFn(Protocol):
    def __call__(
        self,
        manager: CheckpointManager,
        cfg: TrainerConfig,
        *,
        model: GPT,
        optimizer: OptimizerLike,
        ema: EMA | None,
        iter_num: int,
        best_val_loss: float,
        logger: LoggerLike,
        is_best: bool,
    ) -> None: ...


class PropagateMetadataFn(Protocol):
    def __call__(
        self, cfg: TrainerConfig, shared: SharedConfig, *, logger: LoggerLike
    ) -> None: ...


class RunEvaluationFn(Protocol):
    def __call__(
        self,
        cfg: TrainerConfig,
        *,
        logger: LoggerLike,
        iter_num: int,
        lr: float,
        raw_model: GPT,
        batches: BatchProvider,
        ctx: AbstractContextManager[object],
        writer: TensorboardWriter | None,
    ) -> Dict[str, float]: ...


class InitializeComponentsFn(Protocol):
    def __call__(
        self,
        model: GPT,
        cfg: TrainerConfig,
        runtime: RuntimeContext,
        *,
        log_dir: str,
    ) -> Tuple[GPT, GradScaler, Optional[EMA], Optional[TensorboardWriter]]: ...


@dataclass(frozen=True)
class TrainerDependencies:
    initialize_batches: Callable[[TrainerConfig, SharedConfig], BatchProvider]
    initialize_model: Callable[[TrainerConfig, LoggerLike], Tuple[GPT, OptimizerLike]]
    initialize_components: InitializeComponentsFn
    create_manager: Callable[[TrainerConfig, SharedConfig], CheckpointManager]
    load_checkpoint: LoadCheckpointFn
    apply_checkpoint: ApplyCheckpointFn
    save_checkpoint: SaveCheckpointFn
    propagate_metadata: PropagateMetadataFn
    run_evaluation: RunEvaluationFn
    get_lr: Callable[[int, LRSchedule, OptimConfig], float]
    vmap: VectorizeFn | None = None
    vectorize: Callable[[], VectorizeFn | None] = field(default=lambda: None)


def default_trainer_dependencies(
    *,
    initialize_components_fn: InitializeComponentsFn | None = None,
) -> TrainerDependencies:
    def _init_components(
        model: GPT,
        cfg: TrainerConfig,
        runtime: RuntimeContext,
        *,
        log_dir: str,
    ) -> Tuple[GPT, GradScaler, Optional[EMA], Optional[TensorboardWriter]]:
        return initialize_components(model, cfg, runtime, log_dir=log_dir)

    init_components = initialize_components_fn or _init_components

    return TrainerDependencies(
        initialize_batches=initialize_batches,
        initialize_model=initialize_model,
        initialize_components=init_components,
        create_manager=create_manager,
        load_checkpoint=load_checkpoint,
        apply_checkpoint=apply_checkpoint,
        save_checkpoint=save_checkpoint,
        propagate_metadata=propagate_metadata,
        run_evaluation=run_evaluation,
        get_lr=get_lr,
        vmap=cast(VectorizeFn | None, getattr(torch, "vmap", None)),
        vectorize=lambda: cast(VectorizeFn | None, getattr(torch, "vmap", None)),
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
        self._train_step_override: (
            Callable[[Trainer, torch.Tensor, torch.Tensor], torch.Tensor] | None
        ) = None

        self.runtime: RuntimeContext = setup_runtime(cfg)
        self.ctx: AbstractContextManager[object] = self.runtime.autocast_context
        self.device_type = self.runtime.device_type
        self._vectorize_impl = self.deps.vectorize()

        self.batches: BatchProvider = self.deps.initialize_batches(cfg, shared)
        self.model, self.optimizer = self.deps.initialize_model(cfg, self.logger)

        self.out_dir = shared.train_out_dir
        self.out_dir.mkdir(parents=True, exist_ok=True)
        self.ckpt_mgr = self.deps.create_manager(cfg, shared)

        (
            self.model,
            self.scaler,
            self.ema,
            self.writer,
        ) = self.deps.initialize_components(
            self.model,
            cfg,
            self.runtime,
            log_dir=str(self.out_dir),
        )

        self.iter_num = 0
        self.best_val_loss = 1e9

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

    @property
    def writer(self) -> Optional[TensorboardWriter]:
        return self._writer

    @writer.setter
    def writer(self, value: Optional[TensorboardWriter]) -> None:
        self._writer = value

    def set_train_step_override(
        self,
        override: Callable[[Trainer, torch.Tensor, torch.Tensor], torch.Tensor] | None,
    ) -> None:
        self._train_step_override = override

    def run(self) -> Tuple[int, float]:
        """Execute the main training loop until reaching the maximum iteration count."""
        self.logger.info("Starting training loop")
        x_batch, y_batch = self.batches.get_batch("train")
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

                if self._train_step_override is None:
                    loss = self._train_step(x_batch, y_batch)
                else:
                    loss = self._train_step_override(self, x_batch, y_batch)
                x_batch, y_batch = self.batches.get_batch("train")

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
            except (
                CheckpointError,
                RuntimeError,
                OSError,
                ValueError,
                TypeError,
            ) as exc:
                self.logger.warning(f"Failed to propagate meta file: {exc}")

            if self.writer:
                self.writer.close()

        return self.iter_num, self.best_val_loss

    def _train_step(self, x_batch: torch.Tensor, y_batch: torch.Tensor) -> torch.Tensor:
        """Perform a gradient accumulation step and update EMA if configured."""

        grad_steps = int(self.cfg.data.grad_accum_steps)
        if grad_steps <= 0:
            raise ValueError("grad_accum_steps must be a positive integer")

        loss_tensor: torch.Tensor
        if grad_steps == 1:
            loss_tensor = self._train_step_single(x_batch, y_batch)
        else:
            loss_tensor = self._train_step_accum(x_batch, y_batch, grad_steps)

        optimizer = cast(torch.optim.Optimizer, self.optimizer)
        if self.cfg.optim.grad_clip != 0.0:
            self.scaler.unscale_(optimizer)
            torch.nn.utils.clip_grad_norm_(
                self.model.parameters(), self.cfg.optim.grad_clip
            )

        self.scaler.step(optimizer)
        self.scaler.update()
        self.optimizer.zero_grad(set_to_none=True)

        if self.ema:
            self.ema.update(cast(GPT, getattr(self.model, "_orig_mod", self.model)))
        return loss_tensor.detach()

    def _train_step_single(
        self, x_batch: torch.Tensor, y_batch: torch.Tensor
    ) -> torch.Tensor:
        with self.ctx:
            _, loss_tensor = self.model(x_batch, y_batch)
        scaled_loss = cast(ScaledLoss, self.scaler.scale(loss_tensor))
        scaled_loss.backward()
        return loss_tensor

    def _train_step_accum(
        self, x_batch: torch.Tensor, y_batch: torch.Tensor, grad_steps: int
    ) -> torch.Tensor:
        if grad_steps > 1 and self._vectorize_impl is not None:
            try:
                return self._train_step_vmap(x_batch, y_batch, grad_steps)
            except RuntimeError:
                # Fallback if vmap cannot be applied (e.g., due to unsupported ops)
                pass
        return self._train_step_python(x_batch, y_batch, grad_steps)

    def _train_step_python(
        self, x_batch: torch.Tensor, y_batch: torch.Tensor, grad_steps: int
    ) -> torch.Tensor:
        loss_tensor = torch.tensor(0.0, device=x_batch.device)
        with self.ctx:
            for _ in range(grad_steps):
                _, loss_tensor = self.model(x_batch, y_batch)
                loss_tensor = loss_tensor / grad_steps
                scaled_loss = cast(ScaledLoss, self.scaler.scale(loss_tensor))
                scaled_loss.backward()
        return loss_tensor

    def _train_step_vmap(
        self, x_batch: torch.Tensor, y_batch: torch.Tensor, grad_steps: int
    ) -> torch.Tensor:
        x_expanded = x_batch.unsqueeze(0).expand(grad_steps, *x_batch.shape)
        y_expanded = y_batch.unsqueeze(0).expand(grad_steps, *y_batch.shape)

        with self.ctx:

            def _forward(x_inner: torch.Tensor, y_inner: torch.Tensor) -> torch.Tensor:
                _, loss_val = self.model(x_inner, y_inner)
                return loss_val

            vmap_impl = self._vectorize_impl
            if vmap_impl is None:
                raise RuntimeError("Vectorization requested but unavailable")
            vectorized = vmap_impl(_forward)
            losses = vectorized(x_expanded, y_expanded)

        loss_tensor = losses.mean()
        scaled_loss = cast(ScaledLoss, self.scaler.scale(loss_tensor))
        scaled_loss.backward()
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
