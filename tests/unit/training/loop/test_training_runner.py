from __future__ import annotations

from contextlib import nullcontext
from dataclasses import dataclass
from pathlib import Path
from types import MethodType
from typing import Any, Callable, Dict, Literal, Optional, Tuple

import pytest
import torch

import ml_playground.training.loop.runner as runner_mod
from ml_playground.core.error_handling import CheckpointError
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
from ml_playground.training.checkpointing.checkpoint_manager import Checkpoint


class _FakeBatches:
    def __init__(self, device: str) -> None:
        self.device = device

    def get_batch(self, split: str) -> Tuple[torch.Tensor, torch.Tensor]:
        del split
        X = torch.zeros((2, 2), device=self.device)
        Y = torch.zeros((2, 2), device=self.device)
        return X, Y


class _FakeModel(torch.nn.Module):
    def __init__(self) -> None:
        super().__init__()
        self.lin = torch.nn.Linear(2, 2)

    def forward(self, X: torch.Tensor, Y: torch.Tensor):  # type: ignore[override]
        del Y
        loss = torch.ones((), device=X.device, requires_grad=True)
        return self.lin(X), loss

    def estimate_mfu(self, *args: Any, **kwargs: Any) -> float:
        del args, kwargs
        return 42.0


class _FakeOptimizer:
    def __init__(self) -> None:
        self.param_groups = [{"lr": 0.0}]

    def state_dict(self) -> Dict[str, Any]:
        return {}

    def load_state_dict(self, state: Dict[str, Any]) -> None:
        del state

    def zero_grad(self, *, set_to_none: bool = True) -> None:
        del set_to_none

    def step(self) -> None:
        pass


class _FakeWriter:
    def add_scalar(
        self,
        tag: str,
        scalar_value: float,
        global_step: int,
        *,
        walltime: float | None = None,
        new_style: bool = False,
        double_precision: bool = False,
    ) -> None:
        del tag, scalar_value, global_step, walltime, new_style, double_precision

    def close(self) -> None:
        pass


class _ListLogger:
    def __init__(self) -> None:
        self.debugs: list[str] = []
        self.infos: list[str] = []
        self.warnings: list[str] = []
        self.errors: list[str] = []

    def info(self, message: str, *args: Any, **kwargs: Any) -> None:
        del kwargs
        self.infos.append(message % args if args else message)

    def debug(self, message: str, *args: Any, **kwargs: Any) -> None:
        del kwargs
        self.debugs.append(message % args if args else message)

    def warning(self, message: str, *args: Any, **kwargs: Any) -> None:
        del kwargs
        self.warnings.append(message % args if args else message)

    def error(self, message: str, *args: Any, **kwargs: Any) -> None:
        del kwargs
        self.errors.append(message % args if args else message)


@dataclass
class _Saved:
    is_best: bool
    iter_num: int


class _FakeCkptMgr:
    def __init__(self) -> None:
        self.saved: list[_Saved] = []
        self.out_dir = Path("/tmp")


def _make_cfg(
    tmp_path: Path,
    *,
    eval_only: bool = False,
    max_iters: int = 2,
    tensorboard_mode: Literal["eval", "log"] = "eval",
    logger: Any | None = None,
    grad_accum_steps: int = 1,
    grad_clip: float = 0.0,
    ema_decay: float = 0.0,
) -> TrainerConfig:
    out_dir = tmp_path / "out"
    out_dir.mkdir(parents=True, exist_ok=True)
    cfg_kwargs: dict[str, Any] = {}
    if logger is not None:
        cfg_kwargs["logger"] = logger
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
        **cfg_kwargs,
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
    evaluation: Dict[int, Dict[str, float]] | None = None,
    saved_hook: Callable[[Dict[str, Any]], None] | None = None,
    load_checkpoint_result: Optional[Checkpoint] = None,
    get_lr_override: Callable[[int, LRSchedule, OptimConfig], float] | None = None,
    writer: Any | None = None,
    raise_on_final_save: Optional[BaseException] = None,
    propagate_exception: Optional[BaseException] = None,
) -> Tuple[TrainerDependencies, _FakeCkptMgr]:
    evaluation = evaluation or {}
    batches = _FakeBatches(device="cpu")
    model = _FakeModel()
    optimizer = _FakeOptimizer()
    writer = writer or _FakeWriter()
    manager = _FakeCkptMgr()

    def init_batches(cfg: TrainerConfig, shared: SharedConfig) -> _FakeBatches:
        del cfg, shared
        return batches

    def init_model(
        cfg: TrainerConfig, logger: Any
    ) -> Tuple[_FakeModel, _FakeOptimizer]:
        del cfg, logger
        return model, optimizer

    def init_components(
        model_param: torch.nn.Module,
        cfg: TrainerConfig,
        runtime: runner_mod.RuntimeContext,
        log_dir: str,
    ) -> Tuple[torch.nn.Module, runner_mod.GradScaler, None, _FakeWriter]:
        del cfg, runtime, log_dir
        return (
            model_param,
            runner_mod.GradScaler(device="cpu", enabled=False),
            None,
            writer,
        )

    def create_manager(cfg: TrainerConfig, shared: SharedConfig) -> _FakeCkptMgr:
        del cfg, shared
        return manager

    def load_checkpoint(
        manager_param: _FakeCkptMgr,
        cfg: TrainerConfig,
        *,
        logger: Any,
    ) -> Optional[Checkpoint]:
        del manager_param, cfg, logger
        return load_checkpoint_result

    def apply_checkpoint(
        checkpoint: Checkpoint,
        *,
        model: torch.nn.Module,
        optimizer: Any,
        ema: Optional[Any],
    ) -> Tuple[int, float]:
        del model, optimizer, ema
        return checkpoint.iter_num, checkpoint.best_val_loss

    def save_checkpoint(
        manager_param: _FakeCkptMgr,
        cfg: TrainerConfig,
        *,
        model: torch.nn.Module,
        optimizer: Any,
        ema: Optional[Any],
        iter_num: int,
        best_val_loss: float,
        logger: Any,
        is_best: bool,
    ) -> None:
        del cfg, model, optimizer, ema, logger
        if not is_best and raise_on_final_save is not None:
            raise raise_on_final_save
        manager_param.saved.append(_Saved(is_best=is_best, iter_num=iter_num))
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
        logger: Any,
    ) -> None:
        del cfg, shared
        if propagate_exception is not None:
            raise propagate_exception
        del logger

    def run_evaluation(
        cfg: TrainerConfig,
        *,
        logger: Any,
        iter_num: int,
        lr: float,
        raw_model: torch.nn.Module,
        batches: Any,
        ctx: Any,
        writer: Optional[_FakeWriter],
    ) -> Dict[str, float]:
        del cfg, logger, lr, raw_model, batches, ctx, writer
        return evaluation.get(iter_num, evaluation.get(-1, {"train": 0.5, "val": 0.5}))

    def get_lr(iteration: int, schedule: LRSchedule, optim: OptimConfig) -> float:
        if get_lr_override is not None:
            return get_lr_override(iteration, schedule, optim)
        return runner_mod.get_lr(iteration, schedule, optim)

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
    )
    return deps, manager


def test_train_eval_only_breaks_early_and_returns(tmp_path: Path) -> None:
    saved_calls: list[Dict[str, Any]] = []
    deps, _manager = _build_deps(
        evaluation={0: {"train": 0.5, "val": 0.4}},
        saved_hook=saved_calls.append,
    )

    cfg = _make_cfg(tmp_path, eval_only=True, max_iters=0)
    shared = _shared(tmp_path, cfg)

    it, best = runner_mod.Trainer(cfg, shared, deps=deps).run()

    assert it == 0
    assert best == pytest.approx(0.4)
    assert any(not call["best"] for call in saved_calls)


def test_train_writes_best_checkpoint_on_improvement_after_first_iter(
    tmp_path: Path,
) -> None:
    calls: Dict[int, Dict[str, float]] = {
        0: {"train": 0.6, "val": 0.5},
        1: {"train": 0.5, "val": 0.2},
    }
    saved_calls: list[Dict[str, Any]] = []
    deps, manager = _build_deps(
        evaluation=calls,
        saved_hook=saved_calls.append,
    )

    cfg = _make_cfg(tmp_path, eval_only=False, max_iters=2)
    shared = _shared(tmp_path, cfg)

    it, best = runner_mod.Trainer(cfg, shared, deps=deps).run()

    assert it >= 1
    assert any(call["best"] and call["iter_num"] == 1 for call in saved_calls)
    assert any(not call["best"] for call in saved_calls)
    assert best == pytest.approx(0.2)
    assert manager.saved, "checkpoints should be recorded"


def test_trainer_updates_optimizer_lr_via_get_lr(tmp_path: Path) -> None:
    lr_calls: list[Tuple[int, float]] = []

    def track_lr(iteration: int, schedule: LRSchedule, optim: OptimConfig) -> float:
        value = runner_mod.get_lr(iteration, schedule, optim)
        lr_calls.append((iteration, value))
        return value

    deps, _manager = _build_deps(
        evaluation={0: {"train": 0.5, "val": 0.4}},
        get_lr_override=track_lr,
    )

    cfg = _make_cfg(tmp_path, eval_only=False, max_iters=3)
    shared = _shared(tmp_path, cfg)

    trainer = runner_mod.Trainer(cfg, shared, deps=deps)
    trainer.run()

    assert lr_calls, "get_lr must be invoked at least once"
    for _, lr in lr_calls:
        assert 0.0 <= lr <= cfg.optim.learning_rate + 1e-6
    assert any(lr > 0 for _, lr in lr_calls)
    for group in trainer.optimizer.param_groups:
        assert 0.0 <= group["lr"] <= cfg.optim.learning_rate + 1e-6


def test_trainer_tensorboard_logging_handles_writer_errors(tmp_path: Path) -> None:
    logger = _ListLogger()

    class ExplodingWriter:
        def add_scalar(self, *args: Any, **kwargs: Any) -> None:
            del args, kwargs
            raise RuntimeError("tensorboard disconnected")

        def close(self) -> None:
            pass

    deps, _manager = _build_deps(
        evaluation={0: {"train": 0.5, "val": 0.4}},
        writer=ExplodingWriter(),
    )

    cfg = _make_cfg(
        tmp_path,
        eval_only=False,
        max_iters=1,
        tensorboard_mode="log",
        logger=logger,
    )
    shared = _shared(tmp_path, cfg)

    runner_mod.Trainer(cfg, shared, deps=deps).run()

    assert any(
        "TensorBoard logging skipped due to writer error" in msg
        for msg in logger.debugs
    )


def test_trainer_warns_when_final_checkpoint_fails(tmp_path: Path) -> None:
    logger = _ListLogger()
    deps, _manager = _build_deps(
        evaluation={0: {"train": 0.5, "val": 0.4}},
        raise_on_final_save=CheckpointError(
            "disk full",
            reason="Test failure",
            rationale="Ensure warning branch is exercised",
        ),
    )

    cfg = _make_cfg(tmp_path, max_iters=0, logger=logger)
    shared = _shared(tmp_path, cfg)

    runner_mod.Trainer(cfg, shared, deps=deps).run()

    assert any("Failed to save final checkpoint" in msg for msg in logger.warnings)


def test_trainer_warns_when_metadata_propagation_fails(tmp_path: Path) -> None:
    logger = _ListLogger()
    deps, _manager = _build_deps(
        evaluation={0: {"train": 0.5, "val": 0.4}},
        propagate_exception=RuntimeError("fs mismatch"),
    )

    cfg = _make_cfg(tmp_path, max_iters=0, logger=logger)
    shared = _shared(tmp_path, cfg)

    runner_mod.Trainer(cfg, shared, deps=deps).run()

    assert any("Failed to propagate meta file" in msg for msg in logger.warnings)


def test_trainer_applies_checkpoint_state_on_init(tmp_path: Path) -> None:
    checkpoint = Checkpoint(
        model={},
        optimizer={},
        model_args={"n_layer": 1},
        iter_num=5,
        best_val_loss=0.42,
        config={},
    )
    deps, _manager = _build_deps(
        evaluation={0: {"train": 0.5, "val": 0.4}},
        load_checkpoint_result=checkpoint,
    )

    cfg = _make_cfg(tmp_path, max_iters=0)
    shared = _shared(tmp_path, cfg)

    trainer = runner_mod.Trainer(cfg, shared, deps=deps)

    assert trainer.iter_num == 5
    assert trainer.best_val_loss == pytest.approx(0.42)


def test_trainer_tensorboard_logging_success(tmp_path: Path) -> None:
    class CapturingWriter:
        def __init__(self) -> None:
            self.scalars: list[tuple[str, float, int]] = []
            self.closed = False

        def add_scalar(self, tag: str, value: float, step: int) -> None:
            self.scalars.append((tag, float(value), int(step)))

        def close(self) -> None:
            self.closed = True

    writer = CapturingWriter()
    deps, _manager = _build_deps(
        evaluation={0: {"train": 0.5, "val": 0.4}},
        writer=writer,
    )
    cfg = _make_cfg(
        tmp_path,
        max_iters=1,
        tensorboard_mode="log",
    )
    shared = _shared(tmp_path, cfg)

    runner_mod.Trainer(cfg, shared, deps=deps).run()

    tags = {tag for tag, _, _ in writer.scalars}
    assert {"Loss/train", "LR"} <= tags
    assert writer.closed is True


def test_trainer_keyboard_interrupt_skips_final_save(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    logger = _ListLogger()
    saved_calls: list[Dict[str, Any]] = []
    deps, manager = _build_deps(
        evaluation={0: {"train": 0.5, "val": 0.4}},
        saved_hook=saved_calls.append,
    )
    cfg = _make_cfg(tmp_path, max_iters=5, logger=logger)
    shared = _shared(tmp_path, cfg)
    trainer = runner_mod.Trainer(cfg, shared, deps=deps)

    def _interrupt(self, X: torch.Tensor, Y: torch.Tensor) -> torch.Tensor:
        del self, X, Y
        raise KeyboardInterrupt()

    monkeypatch.setattr(
        trainer,
        "_train_step",
        MethodType(_interrupt, trainer),
    )

    with pytest.raises(KeyboardInterrupt):
        trainer.run()

    assert manager.saved == []
    assert any("Training loop interrupted" in msg for msg in logger.infos)


def test_train_step_accum_with_grad_clip_and_ema(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    deps, _manager = _build_deps(evaluation={-1: {"train": 0.5, "val": 0.4}})
    cfg = _make_cfg(
        tmp_path,
        max_iters=0,
        grad_accum_steps=2,
        grad_clip=0.25,
    )
    shared = _shared(tmp_path, cfg)
    trainer = runner_mod.Trainer(cfg, shared, deps=deps)

    class StubEMA:
        def __init__(self) -> None:
            self.calls = 0

        def update(self, model: torch.nn.Module) -> None:
            del model
            self.calls += 1

    trainer.ema = StubEMA()

    def fake_vmap(func: Callable[[torch.Tensor, torch.Tensor], torch.Tensor]):
        def wrapped(x_batch: torch.Tensor, y_batch: torch.Tensor) -> torch.Tensor:
            losses = [func(xb, yb) for xb, yb in zip(x_batch, y_batch)]
            return torch.stack(losses)

        return wrapped

    monkeypatch.setattr(torch, "vmap", fake_vmap)

    X = torch.zeros((2, 2))
    Y = torch.zeros((2, 2))
    loss = trainer._train_step(X, Y)

    assert isinstance(loss, torch.Tensor)
    assert trainer.ema.calls == 1


def test_train_step_accum_fallback_without_vmap(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    deps, _manager = _build_deps(evaluation={-1: {"train": 0.5, "val": 0.4}})
    cfg = _make_cfg(
        tmp_path,
        max_iters=0,
        grad_accum_steps=2,
    )
    shared = _shared(tmp_path, cfg)
    trainer = runner_mod.Trainer(cfg, shared, deps=deps)

    def raising_vmap(func: Callable[[torch.Tensor, torch.Tensor], torch.Tensor]):
        del func

        def wrapped(*_args: Any, **_kwargs: Any) -> torch.Tensor:
            raise RuntimeError("no vmap")

        return wrapped

    monkeypatch.setattr(torch, "vmap", raising_vmap)

    X = torch.zeros((2, 2))
    Y = torch.zeros((2, 2))
    loss = trainer._train_step(X, Y)

    assert isinstance(loss, torch.Tensor)


def test_default_trainer_dependencies_returns_callables(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    captured: dict[str, Any] = {}

    def stub_initialize_components(
        model: Any,
        cfg: TrainerConfig,
        runtime: runner_mod.RuntimeContext,
        log_dir: str,
    ) -> tuple[Any, str, None, None]:
        captured["model"] = model
        captured["cfg"] = cfg
        captured["runtime"] = runtime
        captured["log_dir"] = log_dir
        return model, "scaler", None, None

    monkeypatch.setattr(runner_mod, "initialize_components", stub_initialize_components)

    deps = runner_mod.default_trainer_dependencies()
    assert callable(deps.initialize_batches)
    assert callable(deps.create_manager)
    assert callable(deps.run_evaluation)

    cfg = _make_cfg(tmp_path, max_iters=0)
    runtime = runner_mod.RuntimeContext(
        device_type="cpu", autocast_context=nullcontext()
    )
    model = _FakeModel()
    compiled, scaler, ema, writer = deps.initialize_components(
        model, cfg, runtime, log_dir=str(tmp_path)
    )
    assert compiled is model
    assert scaler == "scaler"
    assert ema is None and writer is None
    assert captured["log_dir"] == str(tmp_path)


def test_trainer_propagates_non_keyboard_exception(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    logger = _ListLogger()
    deps, manager = _build_deps(evaluation={0: {"train": 0.5, "val": 0.4}})
    cfg = _make_cfg(tmp_path, max_iters=3, logger=logger)
    shared = _shared(tmp_path, cfg)
    trainer = runner_mod.Trainer(cfg, shared, deps=deps)

    def _boom(self, *_args: Any, **_kwargs: Any) -> torch.Tensor:
        raise RuntimeError("boom")

    monkeypatch.setattr(trainer, "_train_step", MethodType(_boom, trainer))

    with pytest.raises(RuntimeError):
        trainer.run()

    assert manager.saved == []


def test_trainer_train_step_validates_grad_accum(tmp_path: Path) -> None:
    deps, _manager = _build_deps(evaluation={-1: {"train": 0.5, "val": 0.4}})
    cfg = _make_cfg(tmp_path, max_iters=0)
    shared = _shared(tmp_path, cfg)
    trainer = runner_mod.Trainer(cfg, shared, deps=deps)

    bad_data = trainer.cfg.data.model_copy(update={"grad_accum_steps": 0})
    bad_cfg = trainer.cfg.model_copy(update={"data": bad_data})
    object.__setattr__(trainer, "cfg", bad_cfg)

    with pytest.raises(ValueError, match="grad_accum_steps must be a positive integer"):
        trainer._train_step(torch.zeros((2, 2)), torch.zeros((2, 2)))


def test_train_entrypoint_uses_dependencies(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    deps, manager = _build_deps(evaluation={0: {"train": 0.5, "val": 0.4}})

    monkeypatch.setattr(
        runner_mod,
        "default_trainer_dependencies",
        lambda: deps,
    )

    cfg = _make_cfg(tmp_path, max_iters=0)
    iters, best = runner_mod.train(cfg)

    assert iters == 1
    assert best == pytest.approx(0.4)
    assert manager.saved


def test_get_lr_variants() -> None:
    schedule = LRSchedule(decay_lr=False)
    optim = OptimConfig(learning_rate=0.1)
    assert runner_mod.get_lr(0, schedule, optim) == 0.1

    schedule = LRSchedule(warmup_iters=10, lr_decay_iters=20, min_lr=0.01)
    optim = OptimConfig(learning_rate=0.1)
    assert runner_mod.get_lr(5, schedule, optim) == pytest.approx(0.05)
    assert runner_mod.get_lr(10, schedule, optim) == 0.1
    assert runner_mod.get_lr(20, schedule, optim) == 0.01
    assert runner_mod.get_lr(25, schedule, optim) == 0.01


def test_checkpoint_model_args() -> None:
    checkpoint_data: Dict[str, Any] = {
        "model_args": {"n_layer": 1},
        "config": {"model_args": {"n_layer": 2}},
        "optimizer": {},
        "iter_num": 0,
        "best_val_loss": 0.0,
        "model": {},
    }
    ckpt = Checkpoint(**checkpoint_data)
    assert ckpt.model_args == {"n_layer": 1}

    checkpoint_data = {
        "config": {"model_args": {"n_layer": 2}},
        "optimizer": {},
        "iter_num": 0,
        "best_val_loss": 0.0,
        "model": {},
    }
    model_args = checkpoint_data.get("model_args") or checkpoint_data["config"].get(
        "model_args"
    )
    checkpoint_data["model_args"] = model_args
    ckpt = Checkpoint(**checkpoint_data)
    assert ckpt.model_args == {"n_layer": 2}

    checkpoint_data = {
        "config": {},
        "optimizer": {},
        "iter_num": 0,
        "best_val_loss": 0.0,
        "model": {},
    }
    with pytest.raises(TypeError):
        Checkpoint(**checkpoint_data)
