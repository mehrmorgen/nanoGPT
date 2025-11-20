from __future__ import annotations

from contextlib import contextmanager
from typing import Any, Dict, Iterator

import ml_playground.runtime.cli.device as device_module


@contextmanager
def override_attr(obj: object, name: str, value: Any) -> Iterator[None]:
    original = getattr(obj, name)
    setattr(obj, name, value)
    try:
        yield
    finally:
        setattr(obj, name, original)


def test_global_device_setup_passes_overrides() -> None:
    """Ensure optional torch/availability overrides are forwarded to runtime helper."""

    captured: Dict[str, Any] = {}

    def fake_setup(
        device: str,
        dtype: str,
        seed: int,
        *,
        cuda_is_available: Any,
        torch_module: Any,
    ) -> None:
        captured["device"] = device
        captured["dtype"] = dtype
        captured["seed"] = seed
        captured["cuda"] = cuda_is_available
        captured["torch_module"] = torch_module

    def fake_cuda() -> bool:
        return True
    fake_torch = object()

    with override_attr(device_module, "rt_global_device_setup", fake_setup):
        device_module.global_device_setup(
            "cuda",
            "float16",
            123,
            cuda_is_available=fake_cuda,
            torch_module=fake_torch,
        )

    assert captured["device"] == "cuda"
    assert captured["dtype"] == "float16"
    assert captured["seed"] == 123
    assert captured["cuda"] is fake_cuda
    assert captured["torch_module"] is fake_torch


def test_global_device_setup_defaults_to_module_torch() -> None:
    """When no torch override is provided, the module-level torch is used."""

    sentinel_torch = object()
    received: Dict[str, Any] = {}

    def fake_setup(
        *_args: Any, cuda_is_available: Any, torch_module: Any, **_kwargs: Any
    ) -> None:
        received["cuda"] = cuda_is_available
        received["torch_module"] = torch_module

    with override_attr(device_module, "torch", sentinel_torch):
        with override_attr(device_module, "rt_global_device_setup", fake_setup):
            device_module.global_device_setup("cpu", "float32", 0)

    assert received["cuda"] is None
    assert received["torch_module"] is sentinel_torch
