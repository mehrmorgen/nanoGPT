"""Unit tests for device.py branch coverage.

Tests uncovered branches in global_device_setup for CUDA availability,
backend configuration, and edge cases with missing attributes.
Uses DI fakes instead of mocks per project policy.
"""

from __future__ import annotations


from ml_playground.framework.runtime.device import global_device_setup


class FakeTorchNoCuda:
    """Fake torch module without cuda attribute."""

    def __init__(self) -> None:
        self.manual_seed_called = False
        self.seed: int | None = None

    def manual_seed(self, seed: int) -> None:
        self.manual_seed_called = True
        self.seed = seed


class FakeTorchNoManualSeed:
    """Fake torch module without callable manual_seed."""

    manual_seed = "not_callable"


class FakeCudaNotAvailable:
    """Fake cuda module returning not available."""

    @staticmethod
    def is_available() -> bool:
        return False

    @staticmethod
    def manual_seed(seed: int) -> None:
        pass


class FakeTorchCudaNotAvailable:
    """Fake torch module with cuda that returns not available."""

    def __init__(self) -> None:
        self.manual_seed_called = False
        self.cuda_manual_seed_called = False
        self.seed: int | None = None

    def manual_seed(self, seed: int) -> None:
        self.manual_seed_called = True
        self.seed = seed

    cuda = FakeCudaNotAvailable()


class FakeCudaAvailableNoBackends:
    """Fake cuda module with no backends."""

    @staticmethod
    def is_available() -> bool:
        return True

    @staticmethod
    def manual_seed(seed: int) -> None:
        pass


class FakeTorchCudaAvailableNoBackends:
    """Fake torch with CUDA available but no backends attribute."""

    def __init__(self) -> None:
        self.manual_seed_called = False
        self.seed: int | None = None

    def manual_seed(self, seed: int) -> None:
        self.manual_seed_called = True
        self.seed = seed

    cuda = FakeCudaAvailableNoBackends()


class FakeCudaAvailableNoMatmul:
    """Fake cuda module without matmul."""

    @staticmethod
    def is_available() -> bool:
        return True

    @staticmethod
    def manual_seed(seed: int) -> None:
        pass


class FakeBackendCudaNoMatmul:
    """Fake cuda backend without matmul."""

    pass


class FakeBackendsNoMatmul:
    """Fake backends without matmul in cuda backend."""

    class cudnn:
        allow_tf32: bool = False

    def __init__(self) -> None:
        self.cuda = FakeBackendCudaNoMatmul()


class FakeTorchCudaAvailableNoMatmul:
    """Fake torch with CUDA available but no matmul in cuda backend."""

    def __init__(self) -> None:
        self.manual_seed_called = False
        self.seed: int | None = None

    def manual_seed(self, seed: int) -> None:
        self.manual_seed_called = True
        self.seed = seed

    cuda = FakeCudaAvailableNoMatmul()
    backends = FakeBackendsNoMatmul()


class FakeCudaManualSeedNotCallable:
    """Fake cuda where manual_seed is not callable."""

    @staticmethod
    def is_available() -> bool:
        return True

    manual_seed = "not_callable"


class FakeTorchCudaManualSeedNotCallable:
    """Fake torch where cuda.manual_seed is not callable."""

    def __init__(self) -> None:
        self.manual_seed_called = False
        self.seed: int | None = None

    def manual_seed(self, seed: int) -> None:
        self.manual_seed_called = True
        self.seed = seed

    cuda = FakeCudaManualSeedNotCallable()


class FakeMatmul:
    """Fake matmul with allow_tf32."""

    def __init__(self) -> None:
        self.allow_tf32 = False


class FakeBackendCuda:
    """Fake cuda backend with matmul."""

    def __init__(self) -> None:
        self.matmul = FakeMatmul()


class FakeBackendCudnn:
    """Fake cudnn backend."""

    def __init__(self) -> None:
        self.allow_tf32 = False


class FakeBackendsFull:
    """Fake backends with full CUDA support."""

    def __init__(self) -> None:
        self.cuda = FakeBackendCuda()
        self.cudnn = FakeBackendCudnn()


class FakeCudaFull:
    """Fake cuda with full support."""

    @staticmethod
    def is_available() -> bool:
        return True

    @staticmethod
    def manual_seed(seed: int) -> None:
        pass


class FakeTorchFullCuda:
    """Fake torch with full CUDA support including TF32."""

    def __init__(self) -> None:
        self.manual_seed_called = False
        self.cuda_manual_seed_called = False
        self.seed: int | None = None
        self.tf32_enabled = False

    def manual_seed(self, seed: int) -> None:
        self.manual_seed_called = True
        self.seed = seed

    cuda = FakeCudaFull()
    backends = FakeBackendsFull()


def test_global_device_setup_no_manual_seed() -> None:
    """Test when manual_seed is not callable (line 22->25)."""
    torch_mod = FakeTorchNoManualSeed()
    # Should not raise even though manual_seed is not callable
    global_device_setup("cpu", "float32", 42, torch_module=torch_mod)


def test_global_device_setup_cuda_mod_none() -> None:
    """Test when cuda_mod is None (line 39 else branch)."""
    torch_mod = FakeTorchNoCuda()
    global_device_setup("cpu", "float32", 42, torch_module=torch_mod)
    # Verify global manual_seed was still called
    assert torch_mod.manual_seed_called
    assert torch_mod.seed == 42


def test_global_device_setup_cuda_not_available() -> None:
    """Test when CUDA is not available."""
    torch_mod = FakeTorchCudaNotAvailable()
    global_device_setup("cpu", "float32", 42, torch_module=torch_mod)
    assert torch_mod.manual_seed_called
    assert torch_mod.seed == 42


def test_global_device_setup_no_backends() -> None:
    """Test when backends_obj is None (line 50->57)."""
    torch_mod = FakeTorchCudaAvailableNoBackends()
    global_device_setup("cpu", "float32", 42, torch_module=torch_mod)
    assert torch_mod.manual_seed_called


def test_global_device_setup_no_matmul() -> None:
    """Test when matmul_obj is None (line 54->exit)."""
    torch_mod = FakeTorchCudaAvailableNoMatmul()
    global_device_setup("cpu", "float32", 42, torch_module=torch_mod)
    assert torch_mod.manual_seed_called


def test_global_device_setup_cuda_manual_seed_not_callable() -> None:
    """Test when cuda.manual_seed is not callable (line 45->49)."""
    torch_mod = FakeTorchCudaManualSeedNotCallable()
    global_device_setup("cpu", "float32", 42, torch_module=torch_mod)
    assert torch_mod.manual_seed_called
    assert torch_mod.seed == 42


def test_global_device_setup_full_cuda_tf32() -> None:
    """Test full CUDA path with TF32 enabled using fakes."""
    torch_mod = FakeTorchFullCuda()

    global_device_setup("cuda", "float32", 42, torch_module=torch_mod)

    # Verify TF32 was enabled
    assert torch_mod.backends.cuda.matmul.allow_tf32 is True
    assert torch_mod.backends.cudnn.allow_tf32 is True
