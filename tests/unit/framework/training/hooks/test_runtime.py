from __future__ import annotations

from pathlib import Path

import pytest
import torch
from contextlib import AbstractContextManager, nullcontext
from typing import cast

from ml_playground.framework.configuration.models import (
    DataConfig,
    DeviceKind,
    DTypeKind,
    LRSchedule,
    ModelConfig,
    OptimConfig,
    RuntimeConfig,
    TrainerConfig,
)
from ml_playground.framework.training.hooks.runtime import setup_runtime, TorchModule


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


@pytest.mark.filterwarnings("ignore::UserWarning")  # type: ignore[reportAttributeAccessIssue]
def test_setup_runtime_cuda() -> None:
    """setup_runtime should configure CUDA runtime context."""
    cfg = _make_config(device="cuda", dtype="float16")

    # Inject successful CUDA checks
    def fake_cuda():
        return True

    def fake_seed(seed: int) -> None:
        pass

    def fake_autocast(
        device_type: str, dtype: torch.dtype
    ) -> AbstractContextManager[None]:
        return nullcontext()

    # We also need to mock torch.cuda for the TorchModule.cuda.is_available/manual_seed calls
    # OR we can just inject a complete fake TorchModule as seen in other tests
    # But setup_runtime also calls torch.manual_seed, so we might need to mock that too
    # or rely on the fact that CPU torch operations work fine.

    # Let's inspect other tests like `test_setup_runtime_injected_cuda_available_true`
    # It mocks TorchModule. This is cleaner.

    class MockCudaBackendMatmul:
        def __init__(self, precision: str = "highest") -> None:
            self._precision = precision

        @property
        def fp32_precision(self) -> str:
            return self._precision

        @fp32_precision.setter
        def fp32_precision(self, value: str) -> None:
            self._precision = value

    class MockCudaBackends:
        def __init__(self) -> None:
            self.matmul = MockCudaBackendMatmul()

    class MockCudnnBackends:
        def __init__(self, precision: str = "highest") -> None:
            self._precision = precision

        @property
        def fp32_precision(self) -> str:
            return self._precision

        @fp32_precision.setter
        def fp32_precision(self, value: str) -> None:
            self._precision = value

    class MockBackends:
        def __init__(self) -> None:
            self.cuda = MockCudaBackends()
            self.cudnn = MockCudnnBackends()

    class MockCuda:
        def __init__(self) -> None:
            pass

        def manual_seed(self, seed: int) -> None:
            pass

        def is_available(self) -> bool:
            return True

    class MockTorchModule:
        def __init__(self) -> None:
            self.backends = MockBackends()
            self.cuda = MockCuda()

        def manual_seed(self, seed: int) -> object:
            return None

    fake_torch = MockTorchModule()

    runtime = setup_runtime(
        cfg,
        cuda_available_func=fake_cuda,
        cuda_seed_func=fake_seed,
        autocast_func=fake_autocast,
        torch_module=cast(TorchModule, fake_torch),
    )

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

    class MockCudaBackendMatmul:
        def __init__(self, precision: str = "highest") -> None:
            self._precision = precision

        @property
        def fp32_precision(self) -> str:
            return self._precision

        @fp32_precision.setter
        def fp32_precision(self, value: str) -> None:
            self._precision = value

    class MockCudaBackends:
        def __init__(self) -> None:
            self.matmul = MockCudaBackendMatmul()

    class MockCudnnBackends:
        def __init__(self, precision: str = "highest") -> None:
            self._precision = precision

        @property
        def fp32_precision(self) -> str:
            return self._precision

        @fp32_precision.setter
        def fp32_precision(self, value: str) -> None:
            self._precision = value

    class MockBackends:
        def __init__(self) -> None:
            self.cuda = MockCudaBackends()
            self.cudnn = MockCudnnBackends()

    class MockCuda:
        def __init__(self) -> None:
            pass

        def manual_seed(self, seed: int) -> None:
            _seed_cuda(seed)

        def is_available(self) -> bool:
            return True

    class MockTorchModule:
        def __init__(self) -> None:
            self.backends = MockBackends()
            self.cuda = MockCuda()

        def manual_seed(self, seed: int) -> object:
            _seed_cpu(seed)
            return None

    fake_torch = MockTorchModule()

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

    class MockCudaBackendMatmul:
        pass  # No fp32_precision

    class MockCudaBackends:
        def __init__(self) -> None:
            self.matmul = MockCudaBackendMatmul()

    class MockCudnnBackends:
        pass  # No fp32_precision

    class MockBackends:
        def __init__(self) -> None:
            self.cuda = MockCudaBackends()
            self.cudnn = MockCudnnBackends()

    class MockCuda:
        def __init__(self) -> None:
            pass

        def manual_seed(self, seed: int) -> None:
            _seed_cuda_noop(seed)

        def is_available(self) -> bool:
            return True

    class MockTorchModule:
        def __init__(self) -> None:
            self.backends = MockBackends()
            self.cuda = MockCuda()

        def manual_seed(self, seed: int) -> object:
            _seed_cpu_noop(seed)
            return None

    # cast to TorchModule to satisfy type checker while providing incomplete mock
    fake_torch = cast(TorchModule, MockTorchModule())

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
