from __future__ import annotations

import copy
from pathlib import Path
from math import isclose
from typing import Any, Callable, Literal, NoReturn

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
from ml_playground.core.logging_protocol import LoggerLike
from ml_playground.training.checkpointing.checkpoint_manager import (
    Checkpoint,
    CheckpointManager,
)
from ml_playground.training.checkpointing import service

from tests.unit.training._helpers import (
    LoggerStub,
    make_ema,
    make_minimal_gpt,
    make_optimizer,
)

# Backwards-compatible alias used in legacy tests
_StubLogger = LoggerStub


def _make_cfg(
    tmp_path: Path,
    *,
    read_policy: Literal["latest", "best"] = READ_POLICY_BEST,
) -> TrainerConfig:
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
            tensorboard_enabled=False,
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


def _with_checkpoint_load_fn(
    cfg: TrainerConfig, fn: Callable[..., object] | None
) -> TrainerConfig:
    return cfg.model_copy(update={"checkpoint_load_fn": fn})


def _with_checkpoint_save_fn(
    cfg: TrainerConfig, fn: Callable[..., object] | None
) -> TrainerConfig:
    return cfg.model_copy(update={"checkpoint_save_fn": fn})


def _with_sample_out_dir(shared: SharedConfig, sample_out_dir: Path) -> SharedConfig:
    return shared.model_copy(update={"sample_out_dir": sample_out_dir})


def test_create_manager_respects_retention(tmp_path: Path) -> None:
    """Test create manager respects retention."""
    cfg = _make_cfg(tmp_path)
    shared = _make_shared(tmp_path, cfg)

    manager = service.create_manager(cfg, shared)

    assert manager.keep_last == cfg.runtime.checkpointing.keep.last
    assert manager.keep_best == cfg.runtime.checkpointing.keep.best


def test_save_checkpoint_invokes_manager(tmp_path: Path) -> None:
    """Test save checkpoint invokes manager."""
    cfg_latest = _make_cfg(tmp_path, read_policy=READ_POLICY_LATEST)
    shared = _make_shared(tmp_path, cfg_latest)
    model = make_minimal_gpt()
    optimizer = make_optimizer(model.parameters())

    calls: list[dict[str, Any]] = []

    class _StubManager(CheckpointManager):
        def __init__(self, keep_last: int, keep_best: int, atomic: bool, out_dir: Path):
            super().__init__(
                out_dir=out_dir, keep_last=keep_last, keep_best=keep_best, atomic=atomic
            )

        def save_checkpoint(
            self,
            checkpoint: Checkpoint,
            base_filename: str,
            metric: float,
            iter_num: int,
            logger: LoggerLike,
            counter_value: int | None = None,
            is_best: bool = False,
        ) -> Path:
            del checkpoint, logger
            calls.append(
                {
                    "base_filename": base_filename,
                    "metric": metric,
                    "iter_num": iter_num,
                    "counter_value": counter_value,
                    "is_best": is_best,
                }
            )
            return Path(base_filename)

    mgr = _StubManager(
        keep_last=cfg_latest.runtime.checkpointing.keep.last,
        keep_best=cfg_latest.runtime.checkpointing.keep.best,
        atomic=cfg_latest.runtime.ckpt_atomic,
        out_dir=shared.train_out_dir,
    )
    service.save_checkpoint(
        mgr,
        cfg_latest,
        model=model,
        optimizer=optimizer,
        ema=None,
        iter_num=1,
        best_val_loss=0.123,
        logger=LoggerStub(),
        is_best=True,
    )

    assert calls
    payload = calls[0]
    assert isclose(payload["metric"], 0.123, rel_tol=1e-9)
    assert payload["iter_num"] == 1
    assert payload["is_best"] is True


def test_load_checkpoint_respects_policy(tmp_path: Path) -> None:
    """Test load checkpoint respects policy."""
    cfg_latest = _make_cfg(tmp_path, read_policy=READ_POLICY_LATEST)
    shared = _make_shared(tmp_path, cfg_latest)

    class _SpyManager(CheckpointManager):
        def __init__(self) -> None:
            super().__init__(
                out_dir=shared.train_out_dir,
                atomic=cfg_latest.runtime.ckpt_atomic,
                keep_last=cfg_latest.runtime.checkpointing.keep.last,
                keep_best=cfg_latest.runtime.checkpointing.keep.best,
            )
            self.best_called = False
            self.last_called = False

        def load_latest_checkpoint(self, device: str, logger: LoggerLike) -> Checkpoint:
            del device, logger
            self.last_called = True
            return Checkpoint(
                model={},
                optimizer={},
                model_args={},
                iter_num=1,
                best_val_loss=0.0,
                config={},
            )

        def load_best_checkpoint(self, device: str, logger: LoggerLike) -> Checkpoint:
            del device, logger
            self.best_called = True
            return Checkpoint(
                model={},
                optimizer={},
                model_args={},
                iter_num=0,
                best_val_loss=0.0,
                config={},
            )

    manager = _SpyManager()

    result = service.load_checkpoint(manager, cfg_latest, logger=LoggerStub())
    assert result is not None
    assert result.iter_num == 1
    assert manager.last_called is True

    cfg_best = _make_cfg(tmp_path, read_policy=READ_POLICY_BEST)
    result = service.load_checkpoint(manager, cfg_best, logger=LoggerStub())
    assert result is not None
    assert result.iter_num == 0
    assert manager.best_called is True


def test_load_checkpoint_override_exception(tmp_path: Path) -> None:
    """Test load checkpoint override exception."""

    def _raise_runtime_error(**kwargs: object) -> NoReturn:
        del kwargs
        raise RuntimeError("boom")

    cfg = _with_checkpoint_load_fn(
        _make_cfg(tmp_path),
        _raise_runtime_error,
    )
    shared = _make_shared(tmp_path, cfg)
    logger = LoggerStub()

    result = service.load_checkpoint(
        CheckpointManager(
            out_dir=shared.train_out_dir,
            atomic=cfg.runtime.ckpt_atomic,
            keep_last=cfg.runtime.checkpointing.keep.last,
            keep_best=cfg.runtime.checkpointing.keep.best,
        ),
        cfg,
        logger=logger,
    )
    assert result is None
    assert logger.warnings == ["checkpoint_load_fn failed: boom"]


def test_load_checkpoint_missing_out_dir(tmp_path: Path) -> None:
    """Test load checkpoint missing out dir."""
    cfg = _with_checkpoint_load_fn(_make_cfg(tmp_path), None)
    logger = LoggerStub()

    manager = CheckpointManager(
        out_dir=tmp_path / "missing",
        atomic=cfg.runtime.ckpt_atomic,
        keep_last=cfg.runtime.checkpointing.keep.last,
        keep_best=cfg.runtime.checkpointing.keep.best,
    )
    result = service.load_checkpoint(manager, cfg, logger=logger)
    assert result is None
    assert not logger.warnings


def test_load_checkpoint_handles_checkpoint_error(tmp_path: Path) -> None:
    """Test load checkpoint handles checkpoint error."""
    cfg = _make_cfg(tmp_path, read_policy=READ_POLICY_LATEST)
    logger = LoggerStub()

    class _ErrorManager(CheckpointManager):
        def __init__(self) -> None:
            out_dir = tmp_path / "out_err"
            out_dir.mkdir(parents=True, exist_ok=True)
            super().__init__(
                out_dir=out_dir,
                atomic=cfg.runtime.ckpt_atomic,
                keep_last=cfg.runtime.checkpointing.keep.last,
                keep_best=cfg.runtime.checkpointing.keep.best,
            )

        def load_latest_checkpoint(self, device: str, logger: LoggerLike) -> Checkpoint:
            del device, logger
            raise CheckpointError(
                "bad checkpoint",
                reason="Stubbed manager signalled load failure",
                rationale="Service must propagate checkpoint errors so callers can react",
            )

    result = service.load_checkpoint(_ErrorManager(), cfg, logger=logger)
    assert result is None
    assert len(logger.warnings) == 1
    warning_lines = logger.warnings[0].splitlines()
    assert warning_lines[0] == "Could not load checkpoint (latest): bad checkpoint"
    assert warning_lines[1] == "Reason: Stubbed manager signalled load failure"
    assert warning_lines[2] == (
        "Rationale: Service must propagate checkpoint errors so callers can react"
    )


def test_load_checkpoint_override_success(tmp_path: Path) -> None:
    """Test load checkpoint override success."""
    sentinel = object()

    def _return_sentinel(**kwargs: object) -> object:
        del kwargs
        return sentinel

    cfg = _with_checkpoint_load_fn(_make_cfg(tmp_path), _return_sentinel)
    shared = _make_shared(tmp_path, cfg)

    manager = CheckpointManager(
        out_dir=shared.train_out_dir,
        atomic=cfg.runtime.ckpt_atomic,
        keep_last=cfg.runtime.checkpointing.keep.last,
        keep_best=cfg.runtime.checkpointing.keep.best,
    )
    result = service.load_checkpoint(manager, cfg, logger=LoggerStub())
    assert result is sentinel


def test_apply_checkpoint_restores_state_and_ema() -> None:
    """Test apply checkpoint restores state and ema."""
    model = make_minimal_gpt()
    optimizer = make_optimizer(model.parameters())
    ema = make_ema(model)

    model_checkpoint: dict[str, torch.Tensor] = {}
    for key, value in model.state_dict().items():
        if not isinstance(value, torch.Tensor):
            raise AssertionError(f"Unexpected non-tensor value in state dict for {key}")
        model_checkpoint[key] = torch.full_like(value, 7.0)

    optimizer_checkpoint = copy.deepcopy(optimizer.state_dict())
    for group in optimizer_checkpoint["param_groups"]:
        group["lr"] = 0.123

    checkpoint = Checkpoint(
        model=model_checkpoint,
        optimizer=optimizer_checkpoint,
        model_args={"hidden": 4},
        iter_num=42,
        best_val_loss=0.123,
        config={"cfg": True},
        ema={"shadow": {"weights": [0.9]}},
    )

    iter_num, best_val_loss = service.apply_checkpoint(
        checkpoint,
        model=model,
        optimizer=optimizer,
        ema=ema,
    )

    assert iter_num == 42
    assert isclose(best_val_loss, 0.123, rel_tol=1e-9)
    updated_state = model.state_dict()
    for key, expected in model_checkpoint.items():
        actual = updated_state[key]
        assert isinstance(actual, torch.Tensor)
        assert torch.equal(actual, expected)
    assert (
        optimizer.state_dict()["param_groups"] == optimizer_checkpoint["param_groups"]
    )
    assert ema.shadow == {"shadow": {"weights": [0.9]}}


def test_propagate_metadata_copies_file(tmp_path: Path) -> None:
    """Test propagate metadata copies file."""
    cfg = _make_cfg(tmp_path)
    shared = _make_shared(tmp_path, cfg)
    ds_dir = shared.dataset_dir
    ds_dir.mkdir(parents=True, exist_ok=True)
    meta_src = ds_dir / "meta.pkl"
    meta_src.write_bytes(b"meta")

    shared = _with_sample_out_dir(shared, tmp_path / "sample-out")

    expanded_shared = shared
    meta_dst = expanded_shared.train_out_dir / meta_src.name

    service.propagate_metadata(cfg, expanded_shared, logger=LoggerStub())

    assert meta_dst.exists()
    assert meta_dst.read_bytes() == b"meta"


def test_save_checkpoint_uses_override(tmp_path: Path) -> None:
    """Test save checkpoint uses override."""
    calls: list[dict[str, Any]] = []

    def override(**kwargs: Any) -> None:
        calls.append(kwargs)

    cfg = _with_checkpoint_save_fn(_make_cfg(tmp_path), override)
    shared = _make_shared(tmp_path, cfg)

    class _SpyManager(CheckpointManager):
        def __init__(self) -> None:
            super().__init__(
                out_dir=shared.train_out_dir,
                atomic=cfg.runtime.ckpt_atomic,
                keep_last=cfg.runtime.checkpointing.keep.last,
                keep_best=cfg.runtime.checkpointing.keep.best,
            )
            self.calls: list[dict[str, Any]] = []

        def save_checkpoint(
            self,
            checkpoint: Checkpoint,
            base_filename: str,
            metric: float,
            iter_num: int,
            logger: LoggerLike,
            counter_value: int | None = None,
            is_best: bool = False,
        ) -> Path:  # pragma: no cover
            self.calls.append(
                {
                    "checkpoint": checkpoint,
                    "base_filename": base_filename,
                    "metric": metric,
                    "iter_num": iter_num,
                    "logger": logger,
                    "counter_value": counter_value,
                    "is_best": is_best,
                }
            )
            raise AssertionError("manager.save_checkpoint should not be called")

    ema = make_ema()
    ema.shadow = {"ema": True}

    service.save_checkpoint(
        _SpyManager(),
        cfg,
        model=make_minimal_gpt(),
        optimizer=make_optimizer(),
        ema=ema,
        iter_num=3,
        best_val_loss=0.4,
        logger=LoggerStub(),
        is_best=False,
    )

    assert calls
    payload = calls[0]
    assert payload["is_best"] is False
    assert payload["checkpoint"].ema == {"ema": True}


def test_save_checkpoint_fallbacks_after_override_failure(tmp_path: Path) -> None:
    """Test save checkpoint fallbacks after override failure."""

    def override(**_kwargs: Any) -> None:
        raise RuntimeError("boom")

    cfg = _with_checkpoint_save_fn(_make_cfg(tmp_path), override)
    logger = LoggerStub()

    class _SpyManager(CheckpointManager):
        def __init__(self) -> None:
            super().__init__(
                out_dir=tmp_path,
                atomic=cfg.runtime.ckpt_atomic,
                keep_last=cfg.runtime.checkpointing.keep.last,
                keep_best=cfg.runtime.checkpointing.keep.best,
            )
            self.calls: list[dict[str, Any]] = []

        def save_checkpoint(
            self,
            checkpoint: Checkpoint,
            base_filename: str,
            metric: float,
            iter_num: int,
            logger: LoggerLike,
            counter_value: int | None = None,
            is_best: bool = False,
        ) -> Path:
            self.calls.append(
                {
                    "checkpoint": checkpoint,
                    "base_filename": base_filename,
                    "metric": metric,
                    "iter_num": iter_num,
                    "logger": logger,
                    "counter_value": counter_value,
                    "is_best": is_best,
                }
            )
            return self.out_dir / base_filename

    manager = _SpyManager()
    service.save_checkpoint(
        manager,
        cfg,
        model=make_minimal_gpt(),
        optimizer=make_optimizer(),
        ema=make_ema(),
        iter_num=5,
        best_val_loss=0.2,
        logger=logger,
        is_best=True,
    )

    assert manager.calls
    call = manager.calls[0]
    assert call["base_filename"] == "ckpt_best.pt"
    assert isclose(call["metric"], 0.2, rel_tol=1e-9)
    assert logger.warnings == [
        "checkpoint_save_fn failed, falling back to default save: boom"
    ]


def test_propagate_metadata_ignores_meta_resolution_error(tmp_path: Path) -> None:
    """Test propagate metadata ignores meta resolution error."""
    cfg = _make_cfg(tmp_path)
    shared = _make_shared(tmp_path, cfg)
    logger = LoggerStub()

    def failing_meta_path(_dataset_dir: Path) -> Path:
        raise RuntimeError("nope")

    object.__setattr__(
        cfg.data, "meta_path", failing_meta_path
    )  # bypass frozen model guard

    service.propagate_metadata(cfg, shared, logger=logger)

    assert logger.warnings == ["Failed to resolve meta source path: nope"]


def test_propagate_metadata_logs_copy_failure(tmp_path: Path) -> None:
    """Test propagate metadata logs copy failure."""
    cfg = _make_cfg(tmp_path)
    shared = _make_shared(tmp_path, cfg)
    shared = _with_sample_out_dir(shared, tmp_path / "sample-out")
    ds_dir = shared.dataset_dir
    ds_dir.mkdir(parents=True, exist_ok=True)
    meta_src = ds_dir / "meta.pkl"
    meta_src.write_bytes(b"meta")

    logger = LoggerStub()

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
    logger = LoggerStub()

    with pytest.raises(CheckpointError, match="does not contain a mapping payload"):
        mgr.load_best_checkpoint("cpu", logger)


def test_apply_checkpoint_with_ema() -> None:
    """apply_checkpoint should apply EMA shadow weights when available."""
    model = make_minimal_gpt()
    optimizer = make_optimizer(model.parameters())
    ema = make_ema(model)

    # Create checkpoint with EMA shadow
    checkpoint = Checkpoint(
        model=model.state_dict(),
        optimizer=optimizer.state_dict(),
        model_args={},
        iter_num=100,
        best_val_loss=0.5,
        config={},
        ema={"test_param": torch.tensor([1.0, 2.0, 3.0])},
    )

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
    logger = LoggerStub()

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
    logger = LoggerStub()

    service.propagate_metadata(cfg, shared, logger=logger)

    # Both directories should have the meta file
    assert (train_dir / "meta.pkl").exists()
    assert (sample_dir / "meta.pkl").exists()


def test_propagate_metadata_handles_meta_path_exception(tmp_path: Path) -> None:
    """propagate_metadata logs and returns when meta path resolution fails."""

    class _BadData(DataConfig):
        def meta_path(self, dataset_dir: Path) -> Path:
            raise TypeError("bad path")

    cfg = _make_cfg(tmp_path).model_copy(update={"data": _BadData()})
    logger = _StubLogger()
    shared = _make_shared(tmp_path, cfg)

    service.propagate_metadata(cfg, shared, logger=logger)

    assert any("Failed to resolve meta source path" in msg for msg in logger.warnings)


def test_propagate_metadata_skips_duplicate_sample_dir(tmp_path: Path) -> None:
    """propagate_metadata avoids duplicate copies for shared output dirs."""
    dataset_dir = tmp_path / "dataset"
    dataset_dir.mkdir()
    meta_src = dataset_dir / "meta.pkl"
    meta_src.write_bytes(b"meta content")

    cfg = _make_cfg(tmp_path)
    shared = _make_shared(tmp_path, cfg).model_copy(
        update={
            "dataset_dir": dataset_dir,
            "sample_out_dir": cfg.runtime.out_dir,
        }
    )

    copies: list[Path] = []

    def _copy(src: Path, dst: Path) -> None:
        copies.append(dst)

    service.propagate_metadata(cfg, shared, logger=_StubLogger(), copy_fn=_copy)

    assert len(copies) == 1
