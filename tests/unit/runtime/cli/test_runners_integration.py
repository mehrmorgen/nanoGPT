from __future__ import annotations

from pathlib import Path
from typing import Any

from ml_playground.runtime.cli import runners
from ml_playground.runtime.cli.device import global_device_setup as cli_device_setup
from ml_playground.runtime.core.results import ToolResult


class StubTrainConfig:
    def __init__(self) -> None:
        self.runtime = StubRuntime()
        self.logger = StubLogger()


class StubRuntime:
    def __init__(self) -> None:
        self.device = "cpu"
        self.dtype = "float32"
        self.seed = 42


class StubLogger:
    def info(self, msg: str) -> None:
        pass

    def error(self, msg: str) -> None:
        pass


class StubShared:
    pass


def test_run_train_uses_cli_device_setup() -> None:
    """Verify run_train uses the CLI's global_device_setup by default."""

    train_cfg = StubTrainConfig()
    shared = StubShared()

    # Capture hooks passed to run_train_impl
    captured_hooks = None

    import ml_playground.runtime.runners as runtime_runners_module

    def fake_run_train_impl(*args: Any, **kwargs: Any) -> ToolResult:
        nonlocal captured_hooks
        captured_hooks = kwargs.get("hooks")
        return ToolResult.create(
            success=True, exit_code=0, namespace="ml", category="train", command="exp"
        )

    original_impl = runtime_runners_module.run_train_impl
    runtime_runners_module.run_train_impl = fake_run_train_impl  # type: ignore

    try:
        runners.run_train(
            experiment="exp",
            train_cfg=train_cfg,  # type: ignore
            config_path=Path("config.toml"),
            shared=shared,
        )

        assert captured_hooks is not None
        # Verify the device_setup hook is indeed the CLI's global_device_setup
        assert captured_hooks.device_setup == cli_device_setup

    finally:
        runtime_runners_module.run_train_impl = original_impl
