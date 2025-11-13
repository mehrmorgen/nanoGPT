from __future__ import annotations

from contextlib import AbstractContextManager, nullcontext
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Literal, Optional, Tuple, cast, no_type_check
import math

import pytest
import torch
import torch.nn as nn
from torch import Tensor
from torch.optim import Optimizer

import ml_playground.training.loop.runner as runner_mod
from ml_playground.core.error_handling import CheckpointError
from ml_playground.core.logging_protocol import LoggerLike
from ml_playground.training.loop.runner import TrainerDependencies
from ml_playground.configuration.models import (
    DataConfig,
    LRSchedule,
    ModelConfig,
    OptimConfig,
    RuntimeConfig,
    SharedConfig,
    TrainerConfig,
)
from ml_playground.training.checkpointing.checkpoint_manager import (
    Checkpoint,
    CheckpointManager,
    CheckpointPayload,
)
from ml_playground.training.ema import EMA
from ml_playground.training.types import (
    BatchProvider,
    OptimizerLike,
    TensorboardWriter,
    VectorizeFn,
)
from ml_playground.models.core.model import GPT
from ml_playground.training.hooks.runtime import RuntimeContext

from tests.unit.training._helpers import (
    LoggerStub,
    SimpleBatchesStub,
    TensorboardWriterStub,
    make_optimizer,
)


SavePayload = dict[str, float | int | bool]
EvaluationMap = dict[int, dict[str, float]]


DEFAULT_EVALUATION: EvaluationMap = {0: {"train": 0.5, "val": 0.4}}


def default_evaluation() -> EvaluationMap:
    return {step: metrics.copy() for step, metrics in DEFAULT_EVALUATION.items()}


def _assert_close(
    actual: float,
    expected: float,
    *,
    rel_tol: float = 1e-9,
    abs_tol: float = 0.0,
) -> None:
    assert math.isclose(actual, expected, rel_tol=rel_tol, abs_tol=abs_tol)


class _FakeModel(nn.Module):
    @no_type_check
    def __init__(self) -> None:
        super().__init__()
        weight_param = nn.Parameter(torch.ones(1))
        self.register_parameter("weight", weight_param)
        self.weight: Tensor = weight_param

    def forward(self, inputs: Tensor, targets: Tensor) -> tuple[Tensor, Tensor]:
        del targets
        logits = inputs * self.weight
        loss = logits.mean() + self.weight.square().sum()
        return logits, loss

    def estimate_mfu(self, *_args: object, **_kwargs: object) -> float:
        return 42.0


@dataclass
class _Saved:
    is_best: bool
    iter_num: int


class _FakeCkptMgr:
    def __init__(self) -> None:
        self.saved: list[_Saved] = []
        self.out_dir = Path("/tmp")


class _CountingEMA(EMA):
    def __init__(self, model: GPT) -> None:
        super().__init__(model, decay=0.5, device="cpu")
        self.calls = 0

    def update(self, model: GPT) -> None:
        self.calls += 1
        super().update(model)


def _make_cfg(
    tmp_path: Path,
    *,
    eval_only: bool = False,
    max_iters: int = 2,
    tensorboard_mode: Literal["eval", "log"] = "eval",
    logger: LoggerStub | None = None,
    grad_accum_steps: int = 1,
    grad_clip: float = 0.0,
    ema_decay: float = 0.0,
) -> TrainerConfig:
    out_dir = tmp_path / "out"
    out_dir.mkdir(parents=True, exist_ok=True)
    return TrainerConfig(
        model=ModelConfig(n_layer=1, n_head=1, n_embd=8, block_size=4, dropout=0.0),
        data=DataConfig(batch_size=2, block_size=4, grad_accum_steps=grad_accum_steps),
        optim=OptimConfig(
            learning_rate=0.01,
            weight_decay=0.0,
            beta1=0.9,
            beta2=0.95,
            grad_clip=grad_clip,
        ),
        schedule=LRSchedule(
            decay_lr=True,
            warmup_iters=1,
            lr_decay_iters=10,
            min_lr=0.001,
        ),
        runtime=RuntimeConfig(
            out_dir=out_dir,
            max_iters=max_iters,
            eval_interval=1,
            eval_iters=1,
            log_interval=1,
            eval_only=eval_only,
            seed=42,
            device="cpu",
            dtype="float32",
            compile=False,
            tensorboard_enabled=True,
            tensorboard_update_mode=tensorboard_mode,
            ema_decay=ema_decay,
        ),
        hf_model=TrainerConfig.HFModelConfig(
            model_name="hf/model",
            gradient_checkpointing=False,
            block_size=128,
        ),
        peft=TrainerConfig.PeftConfig(enabled=False),
        logger=logger or LoggerStub(),
    )


def _shared(tmp_path: Path, cfg: TrainerConfig) -> SharedConfig:
    return SharedConfig(
        experiment="unit",
        config_path=tmp_path / "cfg.toml",
        project_home=tmp_path,
        dataset_dir=tmp_path / "data",
        train_out_dir=cfg.runtime.out_dir,
        sample_out_dir=cfg.runtime.out_dir,
    )


def _build_deps(
    *,
    evaluation: EvaluationMap | None = None,
    saved_hook: Callable[[SavePayload], None] | None = None,
    load_checkpoint_result: Optional[Checkpoint] = None,
    get_lr_override: Callable[[int, LRSchedule, OptimConfig], float] | None = None,
    writer: TensorboardWriter | None = None,
    raise_on_final_save: Optional[BaseException] = None,
    propagate_exception: Optional[BaseException] = None,
    vectorize_override: VectorizeFn | None = None,
) -> Tuple[TrainerDependencies, _FakeCkptMgr]:
    evaluation = evaluation or {}
    batches = SimpleBatchesStub()
    model = _FakeModel()
    optimizer_like = make_optimizer(model.parameters())
    writer_instance = writer or TensorboardWriterStub()
    manager = _FakeCkptMgr()

    model_obj: GPT = cast(GPT, model)
    optimizer_obj: OptimizerLike = cast(OptimizerLike, optimizer_like)
    writer_obj: TensorboardWriter = cast(TensorboardWriter, writer_instance)

    def init_batches(cfg: TrainerConfig, shared: SharedConfig) -> BatchProvider:
        del cfg, shared
        return cast(BatchProvider, batches)

    def init_model(cfg: TrainerConfig, logger: LoggerLike) -> Tuple[GPT, OptimizerLike]:
        del cfg, logger
        return model_obj, optimizer_obj

    def init_components(
        model: GPT,
        cfg: TrainerConfig,
        runtime: RuntimeContext,
        *,
        log_dir: str,
    ) -> Tuple[GPT, runner_mod.GradScaler, None, TensorboardWriter]:
        del cfg, runtime, log_dir
        return (
            model,
            runner_mod.GradScaler(device="cpu", enabled=False),
            None,
            writer_obj,
        )

    def create_manager(cfg: TrainerConfig, shared: SharedConfig) -> CheckpointManager:
        del cfg, shared
        return cast(CheckpointManager, manager)

    def load_checkpoint(
        manager_param: CheckpointManager,
        cfg: TrainerConfig,
        *,
        logger: LoggerLike,
    ) -> Optional[Checkpoint]:
        del cfg, logger
        _ = cast(_FakeCkptMgr, manager_param)
        return load_checkpoint_result

    def apply_checkpoint(
        checkpoint: Checkpoint,
        *,
        model: torch.nn.Module,
        optimizer: Optimizer,
        ema: Optional[EMA],
    ) -> Tuple[int, float]:
        del model, optimizer, ema
        return checkpoint.iter_num, checkpoint.best_val_loss

    def save_checkpoint(
        manager_param: CheckpointManager,
        cfg: TrainerConfig,
        *,
        model: torch.nn.Module,
        optimizer: Optimizer,
        ema: Optional[EMA],
        iter_num: int,
        best_val_loss: float,
        logger: LoggerLike,
        is_best: bool,
    ) -> None:
        del cfg, model, optimizer, ema, logger
        fake_mgr = cast(_FakeCkptMgr, manager_param)
        if not is_best and raise_on_final_save is not None:
            raise raise_on_final_save
        fake_mgr.saved.append(_Saved(is_best=is_best, iter_num=iter_num))
        if saved_hook is not None:
            saved_hook(
                {
                    "iter_num": iter_num,
                    "best": is_best,
                    "best_val_loss": best_val_loss,
                }
            )

    def propagate_metadata(
        cfg: TrainerConfig,
        shared: SharedConfig,
        *,
        logger: LoggerLike,
    ) -> None:
        del cfg, shared
        if propagate_exception is not None:
            raise propagate_exception
        del logger

    def run_evaluation(
        cfg: TrainerConfig,
        *,
        logger: LoggerLike,
        iter_num: int,
        lr: float,
        raw_model: torch.nn.Module,
        batches: BatchProvider,
        ctx: AbstractContextManager[object] | None,
        writer: Optional[TensorboardWriter],
    ) -> dict[str, float]:
        del cfg, logger, lr, raw_model, batches, ctx, writer
        return evaluation.get(iter_num, evaluation.get(-1, {"train": 0.5, "val": 0.5}))

    def get_lr(iteration: int, schedule: LRSchedule, optim: OptimConfig) -> float:
        if get_lr_override is not None:
            return get_lr_override(iteration, schedule, optim)
        return runner_mod.get_lr(iteration, schedule, optim)

    def vectorize_provider() -> VectorizeFn | None:
        if vectorize_override is not None:
            return vectorize_override
        return cast(VectorizeFn | None, getattr(torch, "vmap", None))

    deps = TrainerDependencies(
        initialize_batches=init_batches,
        initialize_model=init_model,
        initialize_components=init_components,
        create_manager=create_manager,
        load_checkpoint=load_checkpoint,
        apply_checkpoint=apply_checkpoint,
        save_checkpoint=save_checkpoint,
        propagate_metadata=propagate_metadata,
        run_evaluation=run_evaluation,
        get_lr=get_lr,
        vectorize=vectorize_provider,
    )
    return deps, manager


@dataclass
class TrainerFixture:
    trainer: runner_mod.Trainer
    cfg: TrainerConfig
    shared: SharedConfig
    deps: TrainerDependencies
    manager: _FakeCkptMgr

    def run(self) -> tuple[int, float]:
        return self.trainer.run()

    @property
    def logger(self) -> LoggerStub:
        return cast(LoggerStub, self.cfg.logger)


@dataclass
class TrainerHarness:
    tmp_path: Path

    def build(
        self,
        *,
        evaluation: EvaluationMap | None = None,
        saved_hook: Callable[[SavePayload], None] | None = None,
        load_checkpoint: Optional[Checkpoint] = None,
        get_lr_override: Callable[[int, LRSchedule, OptimConfig], float] | None = None,
        writer: TensorboardWriter | None = None,
        raise_on_final_save: Optional[BaseException] = None,
        propagate_exception: Optional[BaseException] = None,
        eval_only: bool = False,
        max_iters: int = 2,
        tensorboard_mode: Literal["eval", "log"] = "eval",
        logger: LoggerStub | None = None,
        grad_accum_steps: int = 1,
        grad_clip: float = 0.0,
        ema_decay: float = 0.0,
        train_step_override: Callable[
            [runner_mod.Trainer, torch.Tensor, torch.Tensor], torch.Tensor
        ]
        | None = None,
        vectorize_override: VectorizeFn | None = None,
    ) -> TrainerFixture:
        deps, manager = _build_deps(
            evaluation=evaluation,
            saved_hook=saved_hook,
            load_checkpoint_result=load_checkpoint,
            get_lr_override=get_lr_override,
            writer=writer,
            raise_on_final_save=raise_on_final_save,
            propagate_exception=propagate_exception,
            vectorize_override=vectorize_override,
        )
        cfg = _make_cfg(
            self.tmp_path,
            eval_only=eval_only,
            max_iters=max_iters,
            tensorboard_mode=tensorboard_mode,
            logger=logger,
            grad_accum_steps=grad_accum_steps,
            grad_clip=grad_clip,
            ema_decay=ema_decay,
        )
        shared = _shared(self.tmp_path, cfg)
        trainer = runner_mod.Trainer(cfg, shared, deps=deps)
        if train_step_override is not None:
            trainer.set_train_step_override(train_step_override)
        return TrainerFixture(
            trainer=trainer,
            cfg=cfg,
            shared=shared,
            deps=deps,
            manager=manager,
        )


@pytest.fixture
def trainer_harness(tmp_path: Path) -> TrainerHarness:
    return TrainerHarness(tmp_path)


def test_train_eval_only_breaks_early_and_returns(
    trainer_harness: TrainerHarness,
) -> None:
    """Test train eval only breaks early and returns."""
    saved_calls: list[SavePayload] = []
    fixture = trainer_harness.build(
        evaluation=default_evaluation(),
        saved_hook=saved_calls.append,
        eval_only=True,
        max_iters=0,
    )

    it, best = fixture.run()

    assert it == 0
    _assert_close(best, 0.4)
    assert any(not call["best"] for call in saved_calls)


def test_train_writes_best_checkpoint_on_improvement_after_first_iter(
    trainer_harness: TrainerHarness,
) -> None:
    """Test train writes best checkpoint on improvement after first iter."""
    calls: EvaluationMap = {
        0: {"train": 0.6, "val": 0.5},
        1: {"train": 0.5, "val": 0.2},
    }
    saved_calls: list[SavePayload] = []
    fixture = trainer_harness.build(
        evaluation=calls,
        saved_hook=saved_calls.append,
        max_iters=2,
    )

    it, best = fixture.run()

    assert it >= 1
    assert any(call["best"] and call["iter_num"] == 1 for call in saved_calls)
    assert any(not call["best"] for call in saved_calls)
    _assert_close(best, 0.2)
    assert fixture.manager.saved, "checkpoints should be recorded"


def test_trainer_updates_optimizer_lr_via_get_lr(
    trainer_harness: TrainerHarness,
) -> None:
    """Test trainer updates optimizer lr via get lr."""
    lr_calls: list[Tuple[int, float]] = []

    def track_lr(iteration: int, schedule: LRSchedule, optim: OptimConfig) -> float:
        value = runner_mod.get_lr(iteration, schedule, optim)
        lr_calls.append((iteration, value))
        return value

    fixture = trainer_harness.build(
        evaluation=default_evaluation(),
        get_lr_override=track_lr,
        max_iters=3,
    )

    fixture.run()
    trainer = fixture.trainer
    cfg = fixture.cfg

    assert lr_calls, "get_lr must be invoked at least once"
    for _, lr in lr_calls:
        assert 0.0 <= lr <= cfg.optim.learning_rate + 1e-6
    assert any(lr > 0 for _, lr in lr_calls)
    for group in trainer.optimizer.param_groups:
        lr_value = group.get("lr")
        assert isinstance(lr_value, float)
        assert 0.0 <= lr_value <= cfg.optim.learning_rate + 1e-6


def test_trainer_tensorboard_logging_handles_writer_errors(
    trainer_harness: TrainerHarness,
) -> None:
    """Test trainer tensorboard logging handles writer errors."""
    logger = LoggerStub()

    class ExplodingWriter:
        def add_scalar(
            self,
            tag: str,
            scalar_value: float,
            global_step: int | None = None,
            *,
            walltime: float | None = None,
            new_style: bool = False,
            double_precision: bool = False,
        ) -> None:
            del tag, scalar_value, global_step, walltime, new_style, double_precision
            raise RuntimeError("tensorboard disconnected")

        def close(self) -> None:
            pass

    trainer_harness.build(
        evaluation=default_evaluation(),
        writer=ExplodingWriter(),
        max_iters=1,
        tensorboard_mode="log",
        logger=logger,
    ).run()

    assert any(
        "TensorBoard logging skipped due to writer error" in msg
        for msg in logger.debugs
    )


def test_trainer_warns_when_final_checkpoint_fails(
    trainer_harness: TrainerHarness,
) -> None:
    """Test trainer warns when final checkpoint fails."""
    logger = LoggerStub()
    trainer_harness.build(
        evaluation=default_evaluation(),
        raise_on_final_save=CheckpointError(
            "disk full",
            reason="Test failure",
            rationale="Ensure warning branch is exercised",
        ),
        max_iters=0,
        logger=logger,
    ).run()

    assert any("Failed to save final checkpoint" in msg for msg in logger.warnings)


def test_trainer_warns_when_metadata_propagation_fails(
    trainer_harness: TrainerHarness,
) -> None:
    """Test trainer warns when metadata propagation fails."""
    logger = LoggerStub()
    trainer_harness.build(
        evaluation=default_evaluation(),
        propagate_exception=RuntimeError("fs mismatch"),
        max_iters=0,
        logger=logger,
    ).run()

    assert any("Failed to propagate meta file" in msg for msg in logger.warnings)


def test_trainer_applies_checkpoint_state_on_init(
    trainer_harness: TrainerHarness,
) -> None:
    """Test trainer applies checkpoint state on init."""
    checkpoint = Checkpoint(
        model={},
        optimizer={},
        model_args={"n_layer": 1},
        iter_num=5,
        best_val_loss=0.42,
        config={},
    )
    fixture = trainer_harness.build(
        evaluation=default_evaluation(),
        load_checkpoint=checkpoint,
        max_iters=0,
    )
    trainer = fixture.trainer

    assert trainer.iter_num == 5
    _assert_close(trainer.best_val_loss, 0.42)


def test_trainer_tensorboard_logging_success(
    trainer_harness: TrainerHarness,
) -> None:
    """Test trainer tensorboard logging success."""
    class CapturingWriter:
        def __init__(self) -> None:
            self.scalars: list[tuple[str, float, int | None]] = []
            self.closed = False

        def add_scalar(
            self,
            tag: str,
            scalar_value: float,
            global_step: int | None = None,
            *,
            walltime: float | None = None,
            new_style: bool = False,
            double_precision: bool = False,
        ) -> None:
            del walltime, new_style, double_precision
            self.scalars.append((tag, float(scalar_value), global_step))

        def close(self) -> None:
            self.closed = True

    writer = CapturingWriter()
    trainer_harness.build(
        evaluation=default_evaluation(),
        writer=writer,
        max_iters=1,
        tensorboard_mode="log",
    ).run()

    tags = {tag for tag, _, _ in writer.scalars}
    assert {"Loss/train", "LR"} <= tags
    assert writer.closed is True


def test_trainer_keyboard_interrupt_skips_final_save(
    trainer_harness: TrainerHarness,
) -> None:
    """Test trainer keyboard interrupt skips final save."""
    saved_calls: list[SavePayload] = []

    def _interrupt(
        _trainer: runner_mod.Trainer, X: torch.Tensor, Y: torch.Tensor
    ) -> torch.Tensor:
        del _trainer, X, Y
        raise KeyboardInterrupt()

    fixture = trainer_harness.build(
        evaluation=default_evaluation(),
        saved_hook=saved_calls.append,
        max_iters=5,
        logger=LoggerStub(),
        train_step_override=_interrupt,
    )

    with pytest.raises(KeyboardInterrupt):
        fixture.trainer.run()

    assert fixture.manager.saved == []
    assert any("Training loop interrupted" in msg for msg in fixture.logger.infos)


def test_train_step_accum_with_grad_clip_and_ema(
    trainer_harness: TrainerHarness,
) -> None:
    """Test train step accum with grad clip and ema."""
    def fake_vmap(
        func: Callable[[torch.Tensor, torch.Tensor], torch.Tensor],
    ) -> Callable[[torch.Tensor, torch.Tensor], torch.Tensor]:
        def wrapped(x_batch: torch.Tensor, y_batch: torch.Tensor) -> torch.Tensor:
            losses = [func(xb, yb) for xb, yb in zip(x_batch, y_batch)]
            return torch.stack(losses)

        return wrapped

    fixture = trainer_harness.build(
        evaluation={-1: {"train": 0.5, "val": 0.4}},
        grad_accum_steps=2,
        grad_clip=0.25,
        max_iters=1,
        ema_decay=0.5,
        vectorize_override=fake_vmap,
    )
    trainer = fixture.trainer
    counting_ema = _CountingEMA(trainer.model)
    trainer.ema = counting_ema

    train_step_accum = cast(
        Callable[[torch.Tensor, torch.Tensor, int], torch.Tensor],
        getattr(trainer, "_train_step_accum"),
    )

    def _override(
        _trainer: runner_mod.Trainer, x: torch.Tensor, y: torch.Tensor
    ) -> torch.Tensor:
        result = train_step_accum(x, y, trainer.cfg.data.grad_accum_steps)
        counting_ema.update(trainer.model)
        return result

    trainer.set_train_step_override(_override)
    trainer.run()
    assert counting_ema.calls >= 1


def test_train_step_accum_fallback_without_vmap(
    trainer_harness: TrainerHarness,
) -> None:
    """Test train step accum fallback without vmap."""
    def raising_vmap(
        func: Callable[[torch.Tensor, torch.Tensor], torch.Tensor],
    ) -> Callable[[torch.Tensor, torch.Tensor], torch.Tensor]:
        del func

        def wrapped(*_args: object, **_kwargs: object) -> torch.Tensor:
            raise RuntimeError("no vmap")

        return wrapped

    fixture = trainer_harness.build(
        evaluation={-1: {"train": 0.5, "val": 0.4}},
        grad_accum_steps=2,
        max_iters=1,
        vectorize_override=raising_vmap,
    )
    trainer = fixture.trainer

    train_step_accum = cast(
        Callable[[torch.Tensor, torch.Tensor, int], torch.Tensor],
        getattr(trainer, "_train_step_accum"),
    )

    def _override_train_step(
        _trainer: runner_mod.Trainer,
        x: torch.Tensor,
        y: torch.Tensor,
    ) -> torch.Tensor:
        return train_step_accum(x, y, trainer.cfg.data.grad_accum_steps)

    trainer.set_train_step_override(_override_train_step)
    trainer.run()


def test_default_trainer_dependencies_returns_callables(
    trainer_harness: TrainerHarness,
) -> None:
    """Test default trainer dependencies returns callables."""
    captured: dict[str, object] = {}

    def stub_initialize_components(
        model: GPT,
        cfg: TrainerConfig,
        runtime: RuntimeContext,
        *,
        log_dir: str,
    ) -> tuple[GPT, runner_mod.GradScaler, None, None]:
        captured["model"] = model
        captured["cfg"] = cfg
        captured["runtime"] = runtime
        captured["log_dir"] = log_dir
        return (
            model,
            runner_mod.GradScaler(device="cpu", enabled=False),
            None,
            None,
        )

    deps = runner_mod.default_trainer_dependencies(
        initialize_components_fn=stub_initialize_components
    )
    assert callable(deps.initialize_batches)
    assert callable(deps.create_manager)
    assert callable(deps.run_evaluation)

    cfg = _make_cfg(trainer_harness.tmp_path, max_iters=0)
    runtime = runner_mod.RuntimeContext(
        device_type="cpu", autocast_context=nullcontext()
    )
    model = cast(GPT, _FakeModel())
    compiled, scaler, ema, writer = deps.initialize_components(
        model, cfg, runtime, log_dir=str(trainer_harness.tmp_path)
    )
    assert compiled is model
    assert isinstance(scaler, runner_mod.GradScaler)
    assert ema is None and writer is None
    assert captured["log_dir"] == str(trainer_harness.tmp_path)


def test_trainer_propagates_non_keyboard_exception(
    trainer_harness: TrainerHarness,
) -> None:
    """Test trainer propagates non keyboard exception."""
    def _boom(
        _trainer: runner_mod.Trainer, *_args: object, **_kwargs: object
    ) -> torch.Tensor:
        del _trainer, _args, _kwargs
        raise RuntimeError("boom")

    fixture = trainer_harness.build(
        evaluation=default_evaluation(),
        max_iters=3,
        logger=LoggerStub(),
        train_step_override=_boom,
    )

    with pytest.raises(RuntimeError):
        fixture.trainer.run()

    assert fixture.manager.saved == []


def test_trainer_train_step_validates_grad_accum(
    trainer_harness: TrainerHarness,
) -> None:
    """Test trainer train step validates grad accum."""
    fixture = trainer_harness.build(
        evaluation={-1: {"train": 0.5, "val": 0.4}},
        max_iters=0,
    )
    trainer = fixture.trainer

    bad_data = trainer.cfg.data.model_copy(update={"grad_accum_steps": 0})
    bad_cfg = trainer.cfg.model_copy(update={"data": bad_data})
    object.__setattr__(trainer, "cfg", bad_cfg)

    inputs = torch.zeros((2, 2))
    targets = torch.zeros((2, 2))
    train_step = cast(
        Callable[[torch.Tensor, torch.Tensor], torch.Tensor],
        getattr(trainer, "_train_step"),
    )
    with pytest.raises(ValueError, match="grad_accum_steps must be a positive integer"):
        train_step(inputs, targets)


def test_train_entrypoint_uses_dependencies(
    trainer_harness: TrainerHarness,
) -> None:
    """Test train entrypoint uses dependencies."""
    deps, manager = _build_deps(evaluation=default_evaluation())

    cfg = _make_cfg(trainer_harness.tmp_path, max_iters=0)
    iters, best = runner_mod.train(cfg, deps=deps)

    assert iters == 1
    _assert_close(best, 0.4)
    assert manager.saved


def test_get_lr_variants() -> None:
    """Test get lr variants."""
    schedule = LRSchedule(decay_lr=False)
    optim = OptimConfig(learning_rate=0.1)
    assert runner_mod.get_lr(0, schedule, optim) == 0.1

    schedule = LRSchedule(warmup_iters=10, lr_decay_iters=20, min_lr=0.01)
    optim = OptimConfig(learning_rate=0.1)
    _assert_close(runner_mod.get_lr(5, schedule, optim), 0.05)
    assert runner_mod.get_lr(10, schedule, optim) == 0.1
    assert runner_mod.get_lr(20, schedule, optim) == 0.01
    assert runner_mod.get_lr(25, schedule, optim) == 0.01


def test_checkpoint_model_args() -> None:
    """Test checkpoint model args."""
    checkpoint_data: CheckpointPayload = {
        "model_args": {"n_layer": 1},
        "config": {"model_args": {"n_layer": 2}},
        "optimizer": {},
        "iter_num": 0,
        "best_val_loss": 0.0,
        "model": {},
    }
    ckpt = Checkpoint(**checkpoint_data)
    assert ckpt.model_args == {"n_layer": 1}

    fallback_model_args: dict[str, int] = {"n_layer": 2}
    fallback_data: CheckpointPayload = {
        "config": {"model_args": fallback_model_args},
        "optimizer": {},
        "iter_num": 0,
        "best_val_loss": 0.0,
        "model": {},
        "model_args": fallback_model_args,
    }
    ckpt = Checkpoint(**fallback_data)
    assert ckpt.model_args == {"n_layer": 2}

    invalid_data: CheckpointPayload = {
        "config": {},
        "optimizer": {},
        "iter_num": 0,
        "best_val_loss": 0.0,
        "model": {},
    }
    with pytest.raises(TypeError):
        _ = Checkpoint(**invalid_data)
