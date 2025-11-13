from __future__ import annotations

from pathlib import Path

import pytest
import torch
from contextlib import AbstractContextManager, nullcontext
from types import SimpleNamespace

from ml_playground.configuration.models import (
    DataConfig,
    DeviceKind,
    LRSchedule,
    ModelConfig,
    OptimConfig,
    RuntimeConfig,
    TrainerConfig,
    DTypeKind,
)
from ml_playground.training.hooks.runtime import setup_runtime


def _make_config(
    device: DeviceKind = "cpu", dtype: DTypeKind = "float32", seed: int = 42
) -> TrainerConfig:
    """Create a TrainerConfig for testing."""
    return TrainerConfig(
        model=ModelConfig(
            n_layer=1, n_head=1, n_embd=4, block_size=4, dropout=0.0, vocab_size=50
        ),
        data=DataConfig(batch_size=2, block_size=4, grad_accum_steps=1),
        optim=OptimConfig(learning_rate=0.01),
        schedule=LRSchedule(
            decay_lr=False, warmup_iters=0, lr_decay_iters=0, min_lr=0.0
        ),
        runtime=RuntimeConfig(
            out_dir=Path("."),
            max_iters=1,
            eval_interval=1,
            eval_iters=1,
            log_interval=1,
            eval_only=False,
            seed=seed,
            device=device,
            dtype=dtype,
            compile=False,
            tensorboard_enabled=False,
            ema_decay=0.0,
        ),
        hf_model=TrainerConfig.HFModelConfig(
            model_name="hf/model",
            gradient_checkpointing=False,
            block_size=128,
        ),
        peft=TrainerConfig.PeftConfig(enabled=False),
    )


def test_setup_runtime_cpu() -> None:
    """setup_runtime should configure CPU runtime context."""
    cfg = _make_config(device="cpu", dtype="float32")

    runtime = setup_runtime(cfg)

    assert runtime.device_type == "cpu"
    assert runtime.autocast_context is not None


def test_setup_runtime_seeds_torch() -> None:
    """setup_runtime should seed torch RNG."""
    cfg = _make_config(seed=123)

    setup_runtime(cfg)

    # Verify seed was set by generating a random number
    val1 = torch.rand(1).item()

    # Reset and verify reproducibility
    setup_runtime(cfg)
    val2 = torch.rand(1).item()

    assert val1 == val2


@pytest.mark.skipif(not torch.cuda.is_available(), reason="CUDA not available")
def test_setup_runtime_cuda() -> None:
    """setup_runtime should configure CUDA runtime context."""
    cfg = _make_config(device="cuda", dtype="float16")

    runtime = setup_runtime(cfg)

    assert runtime.device_type == "cuda"
    assert runtime.autocast_context is not None


def test_setup_runtime_bfloat16() -> None:
    """setup_runtime should handle bfloat16 dtype."""
    cfg = _make_config(device="cpu", dtype="bfloat16")

    runtime = setup_runtime(cfg)

    assert runtime.device_type == "cpu"
    assert runtime.autocast_context is not None


def test_setup_runtime_float16() -> None:
    """setup_runtime should handle float16 dtype."""
    cfg = _make_config(device="cpu", dtype="float16")

    runtime = setup_runtime(cfg)

    assert runtime.device_type == "cpu"
    assert runtime.autocast_context is not None


def test_setup_runtime_injected_cuda_available_true() -> None:
    """setup_runtime should use injected cuda_available_func returning True."""
    cfg = _make_config(device="cuda", dtype="float32")

    cuda_called = False
    seed_calls: list[tuple[str, int]] = []

    def fake_cuda():
        nonlocal cuda_called
        cuda_called = True
        return True

    seed_called = False

    def fake_seed(seed: int) -> None:
        nonlocal seed_called
        seed_called = True
        seed_calls.append(("cuda", seed))

    def fake_autocast(
        device_type: str, dtype: torch.dtype
    ) -> AbstractContextManager[None]:
        del device_type, dtype
        return nullcontext()

    def _seed_cpu(seed: int) -> None:
        seed_calls.append(("cpu", seed))

    def _seed_cuda(seed: int) -> None:
        seed_calls.append(("cuda", seed))

    fake_torch = SimpleNamespace(
        manual_seed=_seed_cpu,
        cuda=SimpleNamespace(
            manual_seed=_seed_cuda,
            is_available=lambda: True,
        ),
        backends=SimpleNamespace(
            cuda=SimpleNamespace(matmul=SimpleNamespace(fp32_precision="highest")),
            cudnn=SimpleNamespace(fp32_precision="highest"),
        ),
    )

    runtime = setup_runtime(
        cfg,
        cuda_available_func=fake_cuda,
        cuda_seed_func=fake_seed,
        autocast_func=fake_autocast,
        torch_module=fake_torch,
    )

    assert cuda_called
    assert seed_called
    assert runtime.device_type == "cuda"
    assert ("cpu", int(cfg.runtime.seed)) in seed_calls
    assert ("cuda", int(cfg.runtime.seed)) in seed_calls


def test_setup_runtime_injected_cuda_available_false() -> None:
    """setup_runtime should use injected cuda_available_func returning False."""
    cfg = _make_config(device="cpu", dtype="float32")

    cuda_called = False

    def fake_cuda():
        nonlocal cuda_called
        cuda_called = True
        return False

    runtime = setup_runtime(cfg, cuda_available_func=fake_cuda)

    assert cuda_called
    assert runtime.device_type == "cpu"


def test_setup_runtime_cuda_error_handling() -> None:
    """setup_runtime should handle RuntimeError in CUDA setup."""
    cfg = _make_config(device="cuda", dtype="float32")

    cuda_called = False

    def fake_cuda():
        nonlocal cuda_called
        cuda_called = True
        return True

    seed_called = False

    def fake_seed(seed: int) -> None:
        nonlocal seed_called
        seed_called = True
        raise RuntimeError("CUDA error")

    def fake_autocast(
        device_type: str, dtype: torch.dtype
    ) -> AbstractContextManager[None]:
        del device_type, dtype
        return nullcontext()

    runtime = setup_runtime(
        cfg,
        cuda_available_func=fake_cuda,
        cuda_seed_func=fake_seed,
        autocast_func=fake_autocast,
    )

    assert cuda_called
    assert seed_called
    assert runtime.device_type == "cuda"


def test_setup_runtime_injected_autocast_func() -> None:
    """setup_runtime should use injected autocast_func for GPU."""
    cfg = _make_config(device="cuda", dtype="float16")

    autocast_called = False

    def fake_autocast(device_type: str, dtype: torch.dtype) -> nullcontext[None]:
        nonlocal autocast_called
        autocast_called = True
        return nullcontext()

    runtime = setup_runtime(cfg, autocast_func=fake_autocast)

    assert autocast_called
    assert runtime.device_type == "cuda"


def test_setup_runtime_cuda_backends_without_fp32_precision() -> None:
    """setup_runtime should handle CUDA backends without fp32_precision attribute."""
    cfg = _make_config(device="cuda", dtype="float32")

    def fake_cuda():
        return True

    def fake_seed(seed: int) -> None:
        pass

    def fake_autocast(
        device_type: str, dtype: torch.dtype
    ) -> AbstractContextManager[None]:
        return nullcontext()

    def _seed_cpu_noop(seed: int) -> None:
        del seed

    def _seed_cuda_noop(seed: int) -> None:
        del seed

    fake_torch = SimpleNamespace(
        manual_seed=_seed_cpu_noop,
        cuda=SimpleNamespace(
            manual_seed=_seed_cuda_noop,
            is_available=lambda: True,
        ),
        backends=SimpleNamespace(
            cuda=SimpleNamespace(matmul=SimpleNamespace()),  # No fp32_precision
            cudnn=SimpleNamespace(),  # No fp32_precision
        ),
    )

    runtime = setup_runtime(
        cfg,
        cuda_available_func=fake_cuda,
        cuda_seed_func=fake_seed,
        autocast_func=fake_autocast,
        torch_module=fake_torch,
    )

    assert runtime.device_type == "cuda"


def test_setup_runtime_cuda_exception_handling_attribute_error() -> None:
    """setup_runtime should handle AttributeError in CUDA setup."""
    cfg = _make_config(device="cuda", dtype="float32")

    def fake_cuda():
        return True

    def fake_seed(seed: int) -> None:
        raise AttributeError("Missing CUDA attribute")

    def fake_autocast(
        device_type: str, dtype: torch.dtype
    ) -> AbstractContextManager[None]:
        return nullcontext()

    runtime = setup_runtime(
        cfg,
        cuda_available_func=fake_cuda,
        cuda_seed_func=fake_seed,
        autocast_func=fake_autocast,
    )

    # Should not raise, should handle the exception gracefully
    assert runtime.device_type == "cuda"


def test_setup_runtime_cuda_exception_handling_assertion_error() -> None:
    """setup_runtime should handle AssertionError in CUDA setup."""
    cfg = _make_config(device="cuda", dtype="float32")

    def fake_cuda():
        return True

    def fake_seed(seed: int) -> None:
        raise AssertionError("CUDA assertion failed")

    def fake_autocast(
        device_type: str, dtype: torch.dtype
    ) -> AbstractContextManager[None]:
        return nullcontext()

    runtime = setup_runtime(
        cfg,
        cuda_available_func=fake_cuda,
        cuda_seed_func=fake_seed,
        autocast_func=fake_autocast,
    )

    # Should not raise, should handle the exception gracefully
    assert runtime.device_type == "cuda"
