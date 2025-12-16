from __future__ import annotations

from typing import Any, cast

from ml_playground.runtime import protocols


def test_device_setup_protocol_placeholder_executes() -> None:
    sentinel: Any = object()
    primitive = cast(protocols.DeviceSetup, sentinel)

    assert (
        getattr(protocols.DeviceSetup, "__call__")(primitive, "cpu", "float32", 123)
        is None
    )
