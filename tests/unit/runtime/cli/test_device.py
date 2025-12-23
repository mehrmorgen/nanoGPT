from __future__ import annotations

import ml_playground.runtime.cli.device as device


def test_global_device_setup_callable_exists() -> None:
    assert callable(device.global_device_setup)
