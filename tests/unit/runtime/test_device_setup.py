from __future__ import annotations

from types import SimpleNamespace

from ml_playground.runtime.device import global_device_setup


def test_global_device_setup_enables_tf32_when_backends_present() -> None:
    calls: list[tuple[str, int]] = []

    fake_torch = SimpleNamespace(
        manual_seed=lambda seed: calls.append(("cpu", seed)),
        cuda=SimpleNamespace(manual_seed=lambda seed: calls.append(("cuda", seed))),
        backends=SimpleNamespace(
            cuda=SimpleNamespace(matmul=SimpleNamespace(allow_tf32=False)),
            cudnn=SimpleNamespace(allow_tf32=False),
        ),
    )

    global_device_setup(
        "cuda",
        "float16",
        123,
        cuda_is_available=lambda: True,
        torch_module=fake_torch,
    )

    assert ("cpu", 123) in calls
    assert ("cuda", 123) in calls
    assert fake_torch.backends.cuda.matmul.allow_tf32 is True
    assert fake_torch.backends.cudnn.allow_tf32 is True


def test_global_device_setup_skips_tf32_when_backends_missing() -> None:
    calls: list[tuple[str, int]] = []

    fake_torch = SimpleNamespace(
        manual_seed=lambda seed: calls.append(("cpu", seed)),
        cuda=SimpleNamespace(manual_seed=lambda seed: calls.append(("cuda", seed))),
        backends=SimpleNamespace(),
    )

    global_device_setup(
        "cuda",
        "float16",
        7,
        cuda_is_available=lambda: True,
        torch_module=fake_torch,
    )

    assert calls == [("cpu", 7), ("cuda", 7)]
