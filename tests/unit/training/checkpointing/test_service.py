from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

import pytest
import torch

from ml_playground.configuration.models import (
    DataConfig,
    LRSchedule,
    ModelConfig,
    OptimConfig,
    RuntimeConfig,
    SharedConfig,
    TrainerConfig,
    READ_POLICY_BEST,
    READ_POLICY_LATEST,
)
from ml_playground.core.error_handling import CheckpointError
from ml_playground.training.checkpointing.checkpoint_manager import Checkpoint
from ml_playground.training.checkpointing import service


class _StubModel(torch.nn.Module):
    def __init__(self) -> None:
        super().__init__()
        self.linear = torch.nn.Linear(1, 1)


class _StubOptimizer:
    def __init__(self) -> None:
        self.param_groups = [{"lr": 0.0}]

    def state_dict(self) -> dict[str, Any]:
        return {}

    def load_state_dict(self, state: dict[str, Any]) -> None:
        del state

    def zero_grad(self, *, set_to_none: bool = True) -> None:
        del set_to_none


class _StubLogger:
    def __init__(self) -> None:
        self.warnings: list[str] = []

    def warning(self, message: str) -> None:
        self.warnings.append(message)


class _StubEMA:
    def __init__(self) -> None:
        self.shadow: dict[str, Any] | None = {}


def _make_cfg(tmp_path: Path, *, read_policy: str = READ_POLICY_BEST) -> TrainerConfig:
    out_dir = tmp_path / "out"
    out_dir.mkdir(parents=True, exist_ok=True)
    return TrainerConfig(
        model=ModelConfig(n_layer=1, n_head=1, n_embd=4, block_size=4, dropout=0.0),
        data=DataConfig(batch_size=2, block_size=4, grad_accum_steps=1),
        optim=OptimConfig(learning_rate=0.01),
        schedule=LRSchedule(
            decay_lr=False,
            warmup_iters=0,
            lr_decay_iters=1,
            min_lr=0.0,
        ),
        runtime=RuntimeConfig(
            out_dir=out_dir,
            max_iters=1,
            eval_interval=1,
            eval_iters=1,
            log_interval=1,
            eval_only=False,
            seed=1,
            device="cpu",
            dtype="float32",
            compile=False,
            ema_decay=0.0,
            checkpointing=RuntimeConfig.Checkpointing(read_policy=read_policy),
        ),
        hf_model=TrainerConfig.HFModelConfig(
            model_name="hf/model",
            gradient_checkpointing=False,
            block_size=128,
        ),
        peft=TrainerConfig.PeftConfig(enabled=False),
    )


def _make_shared(tmp_path: Path, cfg: TrainerConfig) -> SharedConfig:
    return SharedConfig(
        experiment="unit",
        config_path=tmp_path / "cfg.toml",
        project_home=tmp_path,
        dataset_dir=tmp_path / "data",
        train_out_dir=cfg.runtime.out_dir,
        sample_out_dir=cfg.runtime.out_dir,
    )


def _with_checkpoint_load_fn(cfg: TrainerConfig, fn) -> TrainerConfig:
    return cfg.model_copy(update={"checkpoint_load_fn": fn})


def _with_checkpoint_save_fn(cfg: TrainerConfig, fn) -> TrainerConfig:
    return cfg.model_copy(update={"checkpoint_save_fn": fn})


def _with_sample_out_dir(shared: SharedConfig, sample_out_dir: Path) -> SharedConfig:
    return shared.model_copy(update={"sample_out_dir": sample_out_dir})


class _TrackingLogger:
    def __init__(self) -> None:
        self.warnings: list[str] = []
        self.infos: list[str] = []

    def warning(self, message: str, *args: object, **kwargs: object) -> None:
        del args, kwargs
        self.warnings.append(message)

    def info(self, message: str, *args: object, **kwargs: object) -> None:
        del args, kwargs
        self.infos.append(message)


class _ExtendedTrackingModel(torch.nn.Module):
    def __init__(self) -> None:
        super().__init__()
        self.linear = torch.nn.Linear(1, 1)
        self.state_updates: list[dict[str, Any]] = []

    def load_state_dict(self, state_dict: dict[str, Any], strict: bool = True) -> None:
        del strict
        self.state_updates.append(state_dict)


class _ExtendedTrackingOptimizer:
    def __init__(self) -> None:
        self.param_groups = [{"lr": 0.01}]
        self.state_updates: list[dict[str, Any]] = []

    def state_dict(self) -> dict[str, Any]:
        return {"param_groups": self.param_groups}

    def load_state_dict(self, state_dict: dict[str, Any]) -> None:
        self.state_updates.append(state_dict)


class _ExtendedTrackingEMA:
    def __init__(self) -> None:
        self.shadow: dict[str, Any] | None = None
        self.updates: list[dict[str, Any]] = []

    def __bool__(self) -> bool:
        return True


def _make_cfg_with_all_options(tmp_path: Path) -> TrainerConfig:
    out_dir = tmp_path / "out"
    out_dir.mkdir(parents=True, exist_ok=True)
    return TrainerConfig(
        model=ModelConfig(n_layer=1, n_head=1, n_embd=4, block_size=4, dropout=0.0),
        data=DataConfig(batch_size=2, block_size=4, grad_accum_steps=1),
        optim=OptimConfig(learning_rate=0.01),
        schedule=LRSchedule(
            decay_lr=False,
            warmup_iters=0,
            lr_decay_iters=1,
            min_lr=0.0,
        ),
        runtime=RuntimeConfig(
            out_dir=out_dir,
            max_iters=1,
            eval_interval=1,
            eval_iters=1,
            log_interval=1,
            eval_only=False,
            seed=1,
            device="cpu",
            dtype="float32",
            compile=False,
            ema_decay=0.999,
            checkpointing=RuntimeConfig.Checkpointing(
                read_policy=READ_POLICY_LATEST,
                keep=RuntimeConfig.Checkpointing.Keep(last=3, best=2),
            ),
            ckpt_atomic=True,
            ckpt_naming_policy="steps",
            ckpt_domain_label="test",
            ckpt_naming_strict=True,
        ),
        hf_model=TrainerConfig.HFModelConfig(
            model_name="hf/model",
            gradient_checkpointing=False,
            block_size=128,
        ),
        peft=TrainerConfig.PeftConfig(enabled=False),
    )


def _make_shared_with_sample(tmp_path: Path, cfg: TrainerConfig) -> SharedConfig:
    return SharedConfig(
        experiment="coverage",
        config_path=tmp_path / "cfg.toml",
        project_home=tmp_path,
        dataset_dir=tmp_path / "data",
        train_out_dir=cfg.runtime.out_dir,
        sample_out_dir=cfg.runtime.out_dir / "sample",
    )


def test_create_manager_respects_retention(tmp_path: Path) -> None:
    cfg = _make_cfg(tmp_path)
    shared = _make_shared(tmp_path, cfg)

    manager = service.create_manager(cfg, shared)

    assert manager.keep_last == cfg.runtime.checkpointing.keep.last
    assert manager.keep_best == cfg.runtime.checkpointing.keep.best


def test_save_checkpoint_invokes_manager(tmp_path: Path) -> None:
    cfg_latest = _make_cfg(tmp_path, read_policy=READ_POLICY_LATEST)
    shared = _make_shared(tmp_path, cfg_latest)
    model = _StubModel()
    optimizer = _StubOptimizer()

    calls: list[dict[str, Any]] = []

    class _Manager:
        def __init__(self) -> None:
            self.out_dir = shared.train_out_dir

        def save_checkpoint(
            self,
            checkpoint,
            base_filename,
            metric,
            iter_num,
            logger,
            is_best,
            counter_value=None,
        ):
            del base_filename, counter_value
            calls.append(
                {
                    "metric": metric,
                    "iter_num": iter_num,
                    "is_best": is_best,
                    "model": checkpoint.model,
                }
            )
            return tmp_path / "ckpt.pt"

    mgr = _Manager()
    service.save_checkpoint(
        mgr,
        cfg_latest,
        model=model,
        optimizer=optimizer,
        ema=None,
        iter_num=1,
        best_val_loss=0.123,
        logger=None,
        is_best=True,
    )

    assert calls
    payload = calls[0]
    assert payload["metric"] == pytest.approx(0.123)
    assert payload["iter_num"] == 1
    assert payload["is_best"] is True


def test_save_checkpoint_defaults_domain_counter(tmp_path: Path) -> None:
    cfg = _make_cfg(tmp_path)
    runtime = cfg.runtime.model_copy(
        update={"ckpt_naming_policy": "domain", "ckpt_domain_label": "games"}
    )
    cfg = cfg.model_copy(update={"runtime": runtime})
    shared = _make_shared(tmp_path, cfg)
    manager = service.create_manager(cfg, shared)

    service.save_checkpoint(
        manager,
        cfg,
        model=_StubModel(),
        optimizer=_StubOptimizer(),
        ema=None,
        iter_num=1,
        best_val_loss=0.5,
        logger=logging.getLogger("test"),
        is_best=False,
    )
    expected = cfg.runtime.out_dir / "ckpt_last_games_00000001.pt"
    assert expected.exists()


def test_load_checkpoint_respects_policy(tmp_path: Path) -> None:
    cfg_latest = _make_cfg(tmp_path, read_policy=READ_POLICY_LATEST)
    shared = _make_shared(tmp_path, cfg_latest)

    class _Manager:
        def __init__(self) -> None:
            self.out_dir = shared.train_out_dir
            self.best_called = False
            self.last_called = False

        def load_best_checkpoint(self, *, device, logger):
            del device, logger
            self.best_called = True
            return "best"

        def load_latest_checkpoint(self, *, device, logger):
            del device, logger
            self.last_called = True
            return "latest"

    mgr = _Manager()
    result = service.load_checkpoint(mgr, cfg_latest, logger=None)
    assert result == "latest"
    assert mgr.last_called is True

    cfg_best = _make_cfg(tmp_path, read_policy=READ_POLICY_BEST)
    result = service.load_checkpoint(mgr, cfg_best, logger=None)
    assert result == "best"
    assert mgr.best_called is True


def test_load_checkpoint_override_exception(tmp_path: Path) -> None:
    cfg = _with_checkpoint_load_fn(
        _make_cfg(tmp_path),
        lambda **_kwargs: (_ for _ in ()).throw(RuntimeError("boom")),
    )
    shared = _make_shared(tmp_path, cfg)
    logger = _StubLogger()

    class _Manager:
        def __init__(self) -> None:
            self.out_dir = shared.train_out_dir

    result = service.load_checkpoint(_Manager(), cfg, logger=logger)
    assert result is None
    assert logger.warnings == ["checkpoint_load_fn failed: boom"]


def test_load_checkpoint_missing_out_dir(tmp_path: Path) -> None:
    cfg = _with_checkpoint_load_fn(_make_cfg(tmp_path), None)
    logger = _StubLogger()

    class _Manager:
        def __init__(self) -> None:
            self.out_dir = tmp_path / "missing"

    result = service.load_checkpoint(_Manager(), cfg, logger=logger)
    assert result is None
    assert not logger.warnings


def test_load_checkpoint_handles_checkpoint_error(tmp_path: Path) -> None:
    cfg = _make_cfg(tmp_path, read_policy=READ_POLICY_LATEST)
    logger = _StubLogger()

    class _Manager:
        def __init__(self) -> None:
            self.out_dir = tmp_path / "out_err"
            self.out_dir.mkdir(parents=True, exist_ok=True)

        def load_latest_checkpoint(self, *, device, logger):  # type: ignore[no-untyped-def]
            del device, logger
            raise CheckpointError(
                "bad checkpoint",
                reason="Stubbed manager signalled load failure",
                rationale="Service must propagate checkpoint errors so callers can react",
            )

    result = service.load_checkpoint(_Manager(), cfg, logger=logger)
    assert result is None
    assert len(logger.warnings) == 1
    warning_lines = logger.warnings[0].splitlines()
    assert warning_lines[0] == "Could not load checkpoint (latest): bad checkpoint"
    assert warning_lines[1] == "Reason: Stubbed manager signalled load failure"
    assert warning_lines[2] == (
        "Rationale: Service must propagate checkpoint errors so callers can react"
    )


def test_load_checkpoint_override_success(tmp_path: Path) -> None:
    sentinel = object()
    cfg = _with_checkpoint_load_fn(_make_cfg(tmp_path), lambda **_kwargs: sentinel)
    shared = _make_shared(tmp_path, cfg)

    class _Manager:
        def __init__(self) -> None:
            self.out_dir = shared.train_out_dir

    result = service.load_checkpoint(_Manager(), cfg, logger=_StubLogger())
    assert result is sentinel


class _TrackingModel:
    def __init__(self) -> None:
        self.state: dict[str, Any] | None = None
        self.strict: bool | None = None

    def state_dict(self) -> dict[str, Any]:
        return {"model": 1}

    def load_state_dict(self, state: dict[str, Any], strict: bool = False) -> None:
        self.state = state
        self.strict = strict


class _TrackingOptimizer:
    def __init__(self) -> None:
        self.state: dict[str, Any] | None = None

    def state_dict(self) -> dict[str, Any]:
        return {"opt": 1}

    def load_state_dict(self, state: dict[str, Any]) -> None:
        self.state = state


def test_apply_checkpoint_restores_state_and_ema() -> None:
    checkpoint = Checkpoint(
        model={"weights": [1, 2, 3]},
        optimizer={"moments": [0.1]},
        model_args={"hidden": 4},
        iter_num=42,
        best_val_loss=0.123,
        config={"cfg": True},
        ema={"shadow": {"weights": [0.9]}},
    )
    model = _TrackingModel()
    optimizer = _TrackingOptimizer()
    ema = _StubEMA()

    iter_num, best_val_loss = service.apply_checkpoint(
        checkpoint,
        model=model,  # type: ignore[arg-type]
        optimizer=optimizer,
        ema=ema,
    )

    assert iter_num == 42
    assert best_val_loss == pytest.approx(0.123)
    assert model.state == {"weights": [1, 2, 3]}
    assert model.strict is False
    assert optimizer.state == {"moments": [0.1]}
    assert ema.shadow == {"shadow": {"weights": [0.9]}}


def test_propagate_metadata_copies_file(tmp_path: Path) -> None:
    cfg = _make_cfg(tmp_path)
    shared = _make_shared(tmp_path, cfg)
    ds_dir = shared.dataset_dir
    ds_dir.mkdir(parents=True, exist_ok=True)
    meta_src = ds_dir / "meta.pkl"
    meta_src.write_bytes(b"meta")

    shared = _with_sample_out_dir(shared, tmp_path / "sample-out")

    expanded_shared = shared
    meta_dst = expanded_shared.train_out_dir / meta_src.name

    service.propagate_metadata(cfg, expanded_shared, logger=None)

    assert meta_dst.exists()
    assert meta_dst.read_bytes() == b"meta"


def test_save_checkpoint_uses_override(tmp_path: Path) -> None:
    calls: list[dict[str, Any]] = []

    def override(**kwargs: Any) -> None:
        calls.append(kwargs)

    cfg = _with_checkpoint_save_fn(_make_cfg(tmp_path), override)
    shared = _make_shared(tmp_path, cfg)

    class _Manager:
        def __init__(self) -> None:
            self.out_dir = shared.train_out_dir

        def save_checkpoint(
            self, *args: Any, **kwargs: Any
        ) -> None:  # pragma: no cover
            raise AssertionError("manager.save_checkpoint should not be called")

    ema = _StubEMA()
    ema.shadow = {"ema": True}

    service.save_checkpoint(
        _Manager(),
        cfg,
        model=_TrackingModel(),
        optimizer=_TrackingOptimizer(),
        ema=ema,
        iter_num=3,
        best_val_loss=0.4,
        logger=None,
        is_best=False,
    )

    assert calls
    payload = calls[0]
    assert payload["is_best"] is False
    assert payload["checkpoint"].ema == {"ema": True}


def test_save_checkpoint_fallbacks_after_override_failure(tmp_path: Path) -> None:
    messages: list[str] = []

    def override(**_kwargs: Any) -> None:
        raise RuntimeError("boom")

    cfg = _with_checkpoint_save_fn(_make_cfg(tmp_path), override)

    class _Logger:
        def warning(self, message: str) -> None:
            messages.append(message)

    class _Manager:
        def __init__(self) -> None:
            self.out_dir = tmp_path
            self.calls: list[dict[str, Any]] = []

        def save_checkpoint(
            self,
            checkpoint,
            *,
            base_filename,
            metric,
            iter_num,
            logger,
            is_best,
            counter_value=None,
        ):
            del counter_value
            self.calls.append(
                {
                    "checkpoint": checkpoint,
                    "base_filename": base_filename,
                    "metric": metric,
                    "iter_num": iter_num,
                    "logger": logger,
                    "is_best": is_best,
                }
            )

    manager = _Manager()
    optimizer = _TrackingOptimizer()
    service.save_checkpoint(
        manager,
        cfg,
        model=_TrackingModel(),
        optimizer=optimizer,
        ema=None,
        iter_num=10,
        best_val_loss=0.99,
        logger=_Logger(),
        is_best=True,
    )

    assert manager.calls
    call = manager.calls[0]
    assert call["base_filename"] == "ckpt_best.pt"
    assert call["metric"] == pytest.approx(0.99)
    assert messages == ["checkpoint_save_fn failed, falling back to default save: boom"]


def test_propagate_metadata_ignores_meta_resolution_error(tmp_path: Path) -> None:
    cfg = _make_cfg(tmp_path)
    shared = _make_shared(tmp_path, cfg)
    logger = _StubLogger()

    def failing_meta_path(_dataset_dir: Path) -> Path:
        raise RuntimeError("nope")

    object.__setattr__(
        cfg.data, "meta_path", failing_meta_path
    )  # bypass frozen model guard

    service.propagate_metadata(cfg, shared, logger=logger)

    assert logger.warnings == ["Failed to resolve meta source path: nope"]


def test_propagate_metadata_logs_copy_failure(tmp_path: Path) -> None:
    cfg = _make_cfg(tmp_path)
    shared = _make_shared(tmp_path, cfg)
    shared = _with_sample_out_dir(shared, tmp_path / "sample-out")
    ds_dir = shared.dataset_dir
    ds_dir.mkdir(parents=True, exist_ok=True)
    meta_src = ds_dir / "meta.pkl"
    meta_src.write_bytes(b"meta")

    logger = _StubLogger()

    def failing_copy(src: Path, dst: Path) -> None:
        raise OSError(f"cannot copy to {dst}")

    service.propagate_metadata(cfg, shared, logger=logger, copy_fn=failing_copy)

    assert any("cannot copy" in msg for msg in logger.warnings)


def test_checkpoint_manager_handles_non_mapping_payload(tmp_path: Path) -> None:
    """CheckpointManager should raise error when checkpoint file doesn't contain a mapping."""
    from ml_playground.training.checkpointing.checkpoint_manager import (
        CheckpointManager,
    )

    out_dir = tmp_path / "out"
    out_dir.mkdir()

    # Create a checkpoint file with non-mapping payload
    ckpt_path = out_dir / "ckpt_best_00000001_1.0.pt"
    torch.save([1, 2, 3], ckpt_path)  # Save a list instead of dict

    mgr = CheckpointManager(out_dir, atomic=False, keep_last=1, keep_best=1)
    logger = _StubLogger()

    with pytest.raises(CheckpointError, match="does not contain a mapping payload"):
        mgr.load_best_checkpoint("cpu", logger)


def test_apply_checkpoint_with_ema() -> None:
    """apply_checkpoint should apply EMA shadow weights when available."""
    from ml_playground.training.ema import EMA
    from ml_playground.models.core.model import GPT
    import logging

    # Create a minimal model
    cfg = ModelConfig(
        n_layer=1, n_head=1, n_embd=4, block_size=4, dropout=0.0, vocab_size=50
    )
    logger = logging.getLogger(__name__)
    model = GPT(cfg, logger)

    # Create EMA
    ema = EMA(model, decay=0.999, device="cpu")

    # Create checkpoint with EMA shadow
    checkpoint = Checkpoint(
        model=model.state_dict(),
        optimizer={},
        model_args=cfg.model_dump(),
        iter_num=100,
        best_val_loss=0.5,
        config={},
        ema={"test_param": torch.tensor([1.0, 2.0, 3.0])},
    )

    optimizer = _StubOptimizer()

    # Apply checkpoint
    iter_num, best_val_loss = service.apply_checkpoint(
        checkpoint, model=model, optimizer=optimizer, ema=ema
    )

    # EMA shadow should be updated
    assert "test_param" in ema.shadow
    assert iter_num == 100
    assert best_val_loss == 0.5


def test_propagate_metadata_with_nonexistent_meta(tmp_path: Path) -> None:
    """propagate_metadata should handle nonexistent meta file gracefully."""
    cfg = _make_cfg(tmp_path)
    shared = _make_shared(tmp_path, cfg)
    logger = _StubLogger()

    # Call with nonexistent meta file
    service.propagate_metadata(cfg, shared, logger=logger)

    # Should not raise, just return
    assert len(logger.warnings) == 0  # No warnings for missing meta


def test_propagate_metadata_copies_to_multiple_dirs(tmp_path: Path) -> None:
    """propagate_metadata should copy meta to train and sample dirs."""
    dataset_dir = tmp_path / "dataset"
    dataset_dir.mkdir()
    meta_src = dataset_dir / "meta.pkl"
    meta_src.write_bytes(b"meta content")

    train_dir = tmp_path / "train"
    train_dir.mkdir()
    sample_dir = tmp_path / "sample"

    cfg = _make_cfg(tmp_path)
    shared = SharedConfig(
        experiment="unit",
        config_path=tmp_path / "cfg.toml",
        project_home=tmp_path,
        dataset_dir=dataset_dir,
        train_out_dir=train_dir,
        sample_out_dir=sample_dir,
    )
    logger = _StubLogger()

    service.propagate_metadata(cfg, shared, logger=logger)

    # Both directories should have the meta file
    assert (train_dir / "meta.pkl").exists()
    assert (sample_dir / "meta.pkl").exists()


def test_create_manager_with_all_options(tmp_path: Path) -> None:
    """Test create_manager with all configuration options to cover line 37."""
    cfg = _make_cfg_with_all_options(tmp_path)
    shared = _make_shared_with_sample(tmp_path, cfg)

    manager = service.create_manager(cfg, shared)

    assert manager.keep_last == 3
    assert manager.keep_best == 2
    assert manager.out_dir == shared.train_out_dir


def test_load_checkpoint_with_di_override_success(tmp_path: Path) -> None:
    """Test load_checkpoint with successful DI override (lines 56-64)."""
    cfg = _make_cfg_with_all_options(tmp_path)
    shared = _make_shared_with_sample(tmp_path, cfg)
    logger = _TrackingLogger()

    expected_checkpoint = Checkpoint(
        model={"test": "data"},
        optimizer={"opt": "state"},
        model_args={},
        iter_num=42,
        best_val_loss=0.123,
        config={},
        ema=None,
    )

    def override_load_fn(**_kwargs: Any) -> Checkpoint:
        return expected_checkpoint

    cfg = cfg.model_copy(update={"checkpoint_load_fn": override_load_fn})

    class _MockManager:
        def __init__(self) -> None:
            self.out_dir = shared.train_out_dir

    result = service.load_checkpoint(_MockManager(), cfg, logger=logger)

    assert result is expected_checkpoint
    assert len(logger.warnings) == 0


def test_load_checkpoint_with_di_override_checkpoint_error(tmp_path: Path) -> None:
    """Test load_checkpoint with DI override that raises CheckpointError (lines 59-64)."""
    cfg = _make_cfg_with_all_options(tmp_path)
    shared = _make_shared_with_sample(tmp_path, cfg)
    logger = _TrackingLogger()

    def failing_override(**_kwargs: Any) -> Checkpoint:
        raise CheckpointError("DI override failed", reason="test", rationale="coverage")

    cfg = cfg.model_copy(update={"checkpoint_load_fn": failing_override})

    class _MockManager:
        def __init__(self) -> None:
            self.out_dir = shared.train_out_dir

    result = service.load_checkpoint(_MockManager(), cfg, logger=logger)

    assert result is None
    assert len(logger.warnings) == 1
    assert "DI override failed" in logger.warnings[0]


def test_load_checkpoint_with_di_override_runtime_error(tmp_path: Path) -> None:
    """Test load_checkpoint with DI override that raises RuntimeError (lines 59-64)."""
    cfg = _make_cfg_with_all_options(tmp_path)
    shared = _make_shared_with_sample(tmp_path, cfg)
    logger = _TrackingLogger()

    def failing_override(**_kwargs: Any) -> Checkpoint:
        raise RuntimeError("DI override runtime error")

    cfg = cfg.model_copy(update={"checkpoint_load_fn": failing_override})

    class _MockManager:
        def __init__(self) -> None:
            self.out_dir = shared.train_out_dir

    result = service.load_checkpoint(_MockManager(), cfg, logger=logger)

    assert result is None
    assert len(logger.warnings) == 1
    assert "DI override runtime error" in logger.warnings[0]


def test_apply_checkpoint_with_ema_none(tmp_path: Path) -> None:
    """Test apply_checkpoint with ema=None (lines 94-96)."""
    checkpoint = Checkpoint(
        model={"test": "model"},
        optimizer={"test": "opt"},
        model_args={},
        iter_num=100,
        best_val_loss=0.5,
        config={},
        ema=None,
    )

    model = _ExtendedTrackingModel()
    optimizer = _ExtendedTrackingOptimizer()

    iter_num, best_val_loss = service.apply_checkpoint(
        checkpoint,
        model=model,
        optimizer=optimizer,
        ema=None,
    )

    assert iter_num == 100
    assert best_val_loss == 0.5
    assert len(model.state_updates) == 1
    assert len(optimizer.state_updates) == 1


def test_apply_checkpoint_with_ema_empty_shadow(tmp_path: Path) -> None:
    """Test apply_checkpoint with ema having empty shadow (lines 94-96)."""
    checkpoint = Checkpoint(
        model={"test": "model"},
        optimizer={"test": "opt"},
        model_args={},
        iter_num=200,
        best_val_loss=0.25,
        config={},
        ema={"shadow": {}},
    )

    model = _ExtendedTrackingModel()
    optimizer = _ExtendedTrackingOptimizer()
    ema = _ExtendedTrackingEMA()

    iter_num, best_val_loss = service.apply_checkpoint(
        checkpoint,
        model=model,
        optimizer=optimizer,
        ema=ema,
    )

    assert iter_num == 200
    assert best_val_loss == 0.25
    assert ema.shadow == {"shadow": {}}


def test_save_checkpoint_with_di_override_success(tmp_path: Path) -> None:
    """Test save_checkpoint with successful DI override (lines 123-132)."""
    cfg = _make_cfg_with_all_options(tmp_path)
    shared = _make_shared_with_sample(tmp_path, cfg)
    logger = _TrackingLogger()

    override_calls: list[dict[str, Any]] = []

    def override_save_fn(**kwargs: Any) -> None:
        override_calls.append(kwargs)

    cfg = cfg.model_copy(update={"checkpoint_save_fn": override_save_fn})

    class _MockManager:
        def __init__(self) -> None:
            self.out_dir = shared.train_out_dir

        def save_checkpoint(
            self, *args: Any, **kwargs: Any
        ) -> None:  # pragma: no cover
            del args, kwargs
            raise AssertionError("Manager save should not be called")

    model = _ExtendedTrackingModel()
    optimizer = _ExtendedTrackingOptimizer()
    ema = _ExtendedTrackingEMA()

    service.save_checkpoint(
        _MockManager(),
        cfg,
        model=model,
        optimizer=optimizer,
        ema=ema,
        iter_num=50,
        best_val_loss=0.75,
        logger=logger,
        is_best=True,
    )

    assert len(override_calls) == 1
    call = override_calls[0]
    assert call["is_best"] is True
    assert call["checkpoint"].iter_num == 50
    assert call["checkpoint"].best_val_loss == 0.75
    assert len(logger.warnings) == 0


def test_save_checkpoint_with_di_override_checkpoint_error(tmp_path: Path) -> None:
    """Test save_checkpoint with DI override that raises CheckpointError (lines 133-140)."""
    cfg = _make_cfg_with_all_options(tmp_path)
    shared = _make_shared_with_sample(tmp_path, cfg)
    logger = _TrackingLogger()

    def failing_override(**_kwargs: Any) -> None:
        raise CheckpointError(
            "DI override save failed", reason="test", rationale="coverage"
        )

    cfg = cfg.model_copy(update={"checkpoint_save_fn": failing_override})

    manager_calls: list[dict[str, Any]] = []

    class _MockManager:
        def __init__(self) -> None:
            self.out_dir = shared.train_out_dir

        def save_checkpoint(self, *args: Any, **kwargs: Any) -> None:
            manager_calls.append(kwargs)

    model = _ExtendedTrackingModel()
    optimizer = _ExtendedTrackingOptimizer()
    ema = _ExtendedTrackingEMA()

    service.save_checkpoint(
        _MockManager(),
        cfg,
        model=model,
        optimizer=optimizer,
        ema=ema,
        iter_num=75,
        best_val_loss=0.6,
        logger=logger,
        is_best=False,
    )

    assert len(manager_calls) == 1
    assert len(logger.warnings) == 1
    assert "DI override save failed" in logger.warnings[0]


def test_save_checkpoint_with_di_override_runtime_error(tmp_path: Path) -> None:
    """Test save_checkpoint with DI override that raises RuntimeError (lines 133-140)."""
    cfg = _make_cfg_with_all_options(tmp_path)
    shared = _make_shared_with_sample(tmp_path, cfg)
    logger = _TrackingLogger()

    def failing_override(**_kwargs: Any) -> None:
        raise RuntimeError("DI override runtime error")

    cfg = cfg.model_copy(update={"checkpoint_save_fn": failing_override})

    manager_calls: list[dict[str, Any]] = []

    class _MockManager:
        def __init__(self) -> None:
            self.out_dir = shared.train_out_dir

        def save_checkpoint(self, *args: Any, **kwargs: Any) -> None:
            manager_calls.append(kwargs)

    model = _ExtendedTrackingModel()
    optimizer = _ExtendedTrackingOptimizer()
    ema = _ExtendedTrackingEMA()

    service.save_checkpoint(
        _MockManager(),
        cfg,
        model=model,
        optimizer=optimizer,
        ema=ema,
        iter_num=25,
        best_val_loss=0.8,
        logger=logger,
        is_best=True,
    )

    assert len(manager_calls) == 1
    assert len(logger.warnings) == 1
    assert "DI override runtime error" in logger.warnings[0]


def test_propagate_metadata_with_oserror_in_meta_path(tmp_path: Path) -> None:
    """Test propagate_metadata with OSError in meta_path resolution (lines 169-171)."""
    cfg = _make_cfg_with_all_options(tmp_path)
    shared = _make_shared_with_sample(tmp_path, cfg)
    logger = _TrackingLogger()

    class _BadDataConfig(DataConfig):
        def meta_path(self, base_dir: Path) -> Path:
            raise OSError("Cannot access dataset directory")

    cfg = cfg.model_copy(update={"data": _BadDataConfig(batch_size=1, block_size=4)})

    service.propagate_metadata(cfg, shared, logger=logger)

    assert len(logger.warnings) == 1
    assert "Failed to resolve meta source path" in logger.warnings[0]


def test_propagate_metadata_with_typeerror_in_meta_path(tmp_path: Path) -> None:
    """Test propagate_metadata with TypeError in meta_path resolution (lines 163-167)."""
    cfg = _make_cfg_with_all_options(tmp_path)
    shared = _make_shared_with_sample(tmp_path, cfg)
    logger = _TrackingLogger()

    class _BadDataConfig(DataConfig):
        def meta_path(self, base_dir: Path) -> Path:
            raise TypeError("Invalid argument type")

    cfg = cfg.model_copy(update={"data": _BadDataConfig(batch_size=1, block_size=4)})

    service.propagate_metadata(cfg, shared, logger=logger)

    assert len(logger.warnings) == 1
    assert "Failed to resolve meta source path" in logger.warnings[0]


def test_propagate_metadata_with_ioerror_in_copy(tmp_path: Path) -> None:
    """Test propagate_metadata with IOError during file copy (lines 185-180)."""
    cfg = _make_cfg_with_all_options(tmp_path)
    shared = _make_shared_with_sample(tmp_path, cfg)
    logger = _TrackingLogger()

    meta_path = tmp_path / "data" / "meta.pkl"
    meta_path.parent.mkdir(parents=True, exist_ok=True)
    meta_path.write_text('{"test": "data"}')

    def failing_copy(src: Path, dst: Path) -> None:
        del src
        raise IOError(f"Cannot write to destination {dst}")

    service.propagate_metadata(cfg, shared, logger=logger, copy_fn=failing_copy)

    assert len(logger.warnings) >= 1
    assert any("Cannot write to destination" in warning for warning in logger.warnings)


def test_propagate_metadata_with_none_logger_and_exceptions(tmp_path: Path) -> None:
    """Test propagate_metadata with None logger and exceptions (lines 169-171, 185-180)."""
    cfg = _make_cfg_with_all_options(tmp_path)
    shared = _make_shared_with_sample(tmp_path, cfg)

    class _BadDataConfig(DataConfig):
        def meta_path(self, base_dir: Path) -> Path:
            raise RuntimeError("Meta path error")

    cfg = cfg.model_copy(update={"data": _BadDataConfig(batch_size=1, block_size=4)})

    service.propagate_metadata(cfg, shared, logger=None)


class _FakeLogger:
    def __init__(self) -> None:
        self.warnings: list[str] = []

    def warning(self, msg: str, *args: object, **kwargs: object) -> None:
        del args, kwargs
        self.warnings.append(msg)


class _BadDataConfig(DataConfig):
    """DataConfig that raises ValueError in meta_path to trigger exception branch."""

    def meta_path(self, base_dir: Path) -> Path:
        del base_dir
        raise ValueError("Simulated meta_path failure")


def test_propagate_metadata_warns_on_meta_path_resolution_failure(
    tmp_path: Path,
) -> None:
    cfg = TrainerConfig(
        model=ModelConfig(n_layer=1, n_head=1, n_embd=4, block_size=4),
        data=_BadDataConfig(batch_size=1, block_size=4),
        optim=OptimConfig(learning_rate=0.01),
        schedule=LRSchedule(),
        runtime=RuntimeConfig(out_dir=tmp_path / "out"),
    )
    shared = SharedConfig(
        experiment="unit",
        config_path=tmp_path / "cfg.toml",
        project_home=tmp_path,
        dataset_dir=tmp_path / "data",
        train_out_dir=tmp_path / "train_out",
        sample_out_dir=tmp_path / "sample_out",
    )
    logger = _FakeLogger()

    service.propagate_metadata(cfg, shared, logger=logger)
    assert any("Failed to resolve meta source path" in msg for msg in logger.warnings)


def test_propagate_metadata_warns_on_copy_failure(tmp_path: Path) -> None:
    meta_path = tmp_path / "data" / "meta.pkl"
    meta_path.parent.mkdir(parents=True, exist_ok=True)
    meta_path.write_text('{"meta_version": 1}')

    cfg = TrainerConfig(
        model=ModelConfig(n_layer=1, n_head=1, n_embd=4, block_size=4),
        data=DataConfig(batch_size=1, block_size=4),
        optim=OptimConfig(learning_rate=0.01),
        schedule=LRSchedule(),
        runtime=RuntimeConfig(out_dir=tmp_path / "out"),
    )
    shared = SharedConfig(
        experiment="unit",
        config_path=tmp_path / "cfg.toml",
        project_home=tmp_path,
        dataset_dir=tmp_path / "data",
        train_out_dir=tmp_path / "train_out",
        sample_out_dir=tmp_path / "sample_out",
    )
    logger = _FakeLogger()

    (tmp_path / "train_out").write_text("not a dir")

    service.propagate_metadata(cfg, shared, logger=logger)
    assert any("Failed to copy meta file" in msg for msg in logger.warnings)


def test_propagate_metadata_with_none_logger(tmp_path: Path) -> None:
    """Test that propagate_metadata handles None logger gracefully."""
    meta_path = tmp_path / "data" / "meta.pkl"
    meta_path.parent.mkdir(parents=True, exist_ok=True)
    meta_path.write_text('{"meta_version": 1}')

    cfg = TrainerConfig(
        model=ModelConfig(n_layer=1, n_head=1, n_embd=4, block_size=4),
        data=DataConfig(batch_size=1, block_size=4),
        optim=OptimConfig(learning_rate=0.01),
        schedule=LRSchedule(),
        runtime=RuntimeConfig(out_dir=tmp_path / "out"),
    )
    shared = SharedConfig(
        experiment="unit",
        config_path=tmp_path / "cfg.toml",
        project_home=tmp_path,
        dataset_dir=tmp_path / "data",
        train_out_dir=tmp_path / "train_out",
        sample_out_dir=tmp_path / "sample_out",
    )

    service.propagate_metadata(cfg, shared, logger=None)


def test_propagate_metadata_skips_duplicate_destination(tmp_path: Path) -> None:
    """Test that propagate_metadata skips sample_out_dir when it equals train_out_dir."""
    meta_path = tmp_path / "data" / "meta.pkl"
    meta_path.parent.mkdir(parents=True, exist_ok=True)
    meta_path.write_text('{"meta_version": 1}')

    cfg = TrainerConfig(
        model=ModelConfig(n_layer=1, n_head=1, n_embd=4, block_size=4),
        data=DataConfig(batch_size=1, block_size=4),
        optim=OptimConfig(learning_rate=0.01),
        schedule=LRSchedule(),
        runtime=RuntimeConfig(out_dir=tmp_path / "out"),
    )
    shared = SharedConfig(
        experiment="unit",
        config_path=tmp_path / "cfg.toml",
        project_home=tmp_path,
        dataset_dir=tmp_path / "data",
        train_out_dir=tmp_path / "train_out",
        sample_out_dir=tmp_path / "train_out",
    )

    service.propagate_metadata(cfg, shared, logger=None)


def test_apply_checkpoint_with_ema_and_checkpoint_ema() -> None:
    """Test apply_checkpoint when both ema and checkpoint.ema are truthy."""
    from ml_playground.models.core.config import build_gpt_config
    from ml_playground.models.core.model import GPT

    model_cfg = ModelConfig(n_layer=1, n_head=1, n_embd=4, block_size=4, vocab_size=50)
    gpt_cfg = build_gpt_config(model_cfg)
    model = GPT(gpt_cfg, logger=None)
    optimizer = torch.optim.AdamW(model.parameters(), lr=0.01)

    class _FakeEMA:
        def __init__(self) -> None:
            self.shadow: dict | None = None

    ema = _FakeEMA()

    checkpoint = Checkpoint(
        model=model.state_dict(),
        optimizer=optimizer.state_dict(),
        model_args={},
        iter_num=5,
        best_val_loss=0.3,
        config={},
        ema={"param": 1.0},
    )

    iter_num, best = service.apply_checkpoint(
        checkpoint, model=model, optimizer=optimizer, ema=ema
    )

    assert iter_num == 5
    assert best == 0.3
    assert ema.shadow == checkpoint.ema


class _LoadSaveFakeLogger:
    def __init__(self) -> None:
        self.warnings: list[str] = []
        self.infos: list[str] = []

    def warning(self, msg: str, *args: object, **kwargs: object) -> None:
        del args, kwargs
        self.warnings.append(msg)

    def info(self, msg: str, *args: object, **kwargs: object) -> None:
        del args, kwargs
        self.infos.append(msg)


class _LoadSaveFakeEMA:
    def __init__(self) -> None:
        self.shadow: dict | None = None


def test_load_checkpoint_uses_default_when_no_override(tmp_path: Path) -> None:
    """Test load_checkpoint uses default loading when checkpoint_load_fn is None."""
    from ml_playground.models.core.config import build_gpt_config
    from ml_playground.models.core.model import GPT

    model_cfg = ModelConfig(n_layer=1, n_head=1, n_embd=4, block_size=4, vocab_size=50)
    gpt_cfg = build_gpt_config(model_cfg)
    model = GPT(gpt_cfg, logger=None)
    assert model is not None

    cfg = TrainerConfig(
        model=model_cfg,
        data=DataConfig(batch_size=1, block_size=4),
        optim=OptimConfig(learning_rate=0.01),
        schedule=LRSchedule(),
        runtime=RuntimeConfig(out_dir=tmp_path / "out"),
    )
    shared = SharedConfig(
        experiment="unit",
        config_path=tmp_path / "cfg.toml",
        project_home=tmp_path,
        dataset_dir=tmp_path / "data",
        train_out_dir=tmp_path / "train_out",
        sample_out_dir=tmp_path / "sample_out",
    )
    manager = service.create_manager(cfg, shared)

    checkpoint = service.load_checkpoint(manager, cfg, logger=_LoadSaveFakeLogger())
    assert checkpoint is None


def test_save_checkpoint_uses_default_when_no_override(tmp_path: Path) -> None:
    """Test save_checkpoint uses default saving when checkpoint_save_fn is None."""
    from ml_playground.models.core.config import build_gpt_config
    from ml_playground.models.core.model import GPT

    model_cfg = ModelConfig(n_layer=1, n_head=1, n_embd=4, block_size=4, vocab_size=50)
    gpt_cfg = build_gpt_config(model_cfg)
    model = GPT(gpt_cfg, logger=None)
    optimizer = torch.optim.AdamW(model.parameters(), lr=0.01)

    train_out_dir = tmp_path / "train_out"
    train_out_dir.mkdir(parents=True, exist_ok=True)

    cfg = TrainerConfig(
        model=model_cfg,
        data=DataConfig(batch_size=1, block_size=4),
        optim=OptimConfig(learning_rate=0.01),
        schedule=LRSchedule(),
        runtime=RuntimeConfig(out_dir=tmp_path / "out"),
    )
    shared = SharedConfig(
        experiment="unit",
        config_path=tmp_path / "cfg.toml",
        project_home=tmp_path,
        dataset_dir=tmp_path / "data",
        train_out_dir=train_out_dir,
        sample_out_dir=tmp_path / "sample_out",
    )
    manager = service.create_manager(cfg, shared)

    service.save_checkpoint(
        manager,
        cfg,
        model=model,
        optimizer=optimizer,
        ema=None,
        iter_num=1,
        best_val_loss=0.5,
        logger=_LoadSaveFakeLogger(),
        is_best=True,
    )

    assert any(p.name.startswith("ckpt_best") for p in train_out_dir.glob("*.pt"))


def test_save_checkpoint_with_ema_shadow(tmp_path: Path) -> None:
    """Test save_checkpoint includes ema.shadow when ema is present."""
    from ml_playground.models.core.config import build_gpt_config
    from ml_playground.models.core.model import GPT

    model_cfg = ModelConfig(n_layer=1, n_head=1, n_embd=4, block_size=4, vocab_size=50)
    gpt_cfg = build_gpt_config(model_cfg)
    model = GPT(gpt_cfg, logger=None)
    optimizer = torch.optim.AdamW(model.parameters(), lr=0.01)
    ema = _LoadSaveFakeEMA()
    ema.shadow = {"param": 1.0}

    train_out_dir = tmp_path / "train_out"
    train_out_dir.mkdir(parents=True, exist_ok=True)

    cfg = TrainerConfig(
        model=model_cfg,
        data=DataConfig(batch_size=1, block_size=4),
        optim=OptimConfig(learning_rate=0.01),
        schedule=LRSchedule(),
        runtime=RuntimeConfig(out_dir=tmp_path / "out"),
    )
    shared = SharedConfig(
        experiment="unit",
        config_path=tmp_path / "cfg.toml",
        project_home=tmp_path,
        dataset_dir=tmp_path / "data",
        train_out_dir=train_out_dir,
        sample_out_dir=tmp_path / "sample_out",
    )
    manager = service.create_manager(cfg, shared)

    service.save_checkpoint(
        manager,
        cfg,
        model=model,
        optimizer=optimizer,
        ema=ema,
        iter_num=1,
        best_val_loss=0.5,
        logger=_LoadSaveFakeLogger(),
        is_best=False,
    )

    assert any(p.name.startswith("ckpt_last") for p in train_out_dir.glob("*.pt"))
