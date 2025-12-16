from __future__ import annotations

from typing import cast

import ml_playground.runtime.cli.app as runtime_app


def test_runtime_cli_app_protocol_placeholders_execute() -> None:
    echo_primitive = cast(runtime_app.EchoFunc, object())
    logger_factory_primitive = cast(runtime_app.LoggerFactory, object())

    assert (
        getattr(runtime_app.EchoFunc, "__call__")(echo_primitive, "msg", err=True)
        is None
    )
    assert (
        getattr(runtime_app.LoggerFactory, "__call__")(logger_factory_primitive, "name")
        is None
    )
