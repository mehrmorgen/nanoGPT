from __future__ import annotations

from types import SimpleNamespace
from typing import Any, cast

import pytest

from ml_playground.framework.runtime import protocols
from ml_playground.framework.runtime.core import bootstrap


def test_main_complete_experiments_uses_deps() -> None:
    calls: list[tuple[str, ...]] = []

    def fake_list(prefix: str) -> list[str]:
        calls.append((prefix,))
        return ["demo"]

    deps = bootstrap.CLIDependencies(list_experiments=fake_list)
    with bootstrap.override_cli_dependencies(deps):
        from ml_playground.runtime_cli.main import complete_experiments

        result = complete_experiments("de")
        assert result == ["demo"]
        assert calls == [("de",)]


def test_main_global_options_merges_existing_ctx() -> None:
    from ml_playground.runtime_cli import main as cli_main
    import click

    # We use a real context
    dummy_command = click.Command("dummy")
    ctx = click.Context(dummy_command)
    ctx.obj = {"existing": 1}

    cli_main.apply_global_options(ctx, None, True, 2)

    assert ctx.obj["existing"] == 1
    assert ctx.obj["learning_mode"] is True
    assert ctx.obj["verbosity"] == cli_main.VerbosityLevel(2)


def test_bootstrap_guard_raises_when_unconfigured() -> None:
    # Use the reset/clear helpers instead of override_attr on private state
    bootstrap.clear_config_for_tests()
    with pytest.raises(RuntimeError, match="not been configured"):
        bootstrap.get_cli_dependencies()


def test_bootstrap_lazy_initializes_from_default() -> None:
    called: list[str] = []

    def factory() -> bootstrap.CLIDependencies:
        called.append("factory")
        return bootstrap.CLIDependencies()

    # Configure with our factory
    bootstrap.configure_cli_dependencies(factory)
    try:
        deps = bootstrap.get_cli_dependencies()
        assert called == ["factory"]
        assert isinstance(deps, bootstrap.CLIDependencies)
    finally:
        bootstrap.reset_cli_dependencies()


def test_protocol_device_setup_runtime_checkable() -> None:
    class Impl:
        def __call__(
            self,
            device: str,
            dtype: str,
            seed: int,
            *,
            cuda_is_available: Any | None = None,
            torch_module: Any | None = None,
        ) -> None:
            return None

    impl = Impl()
    assert isinstance(impl, protocols.DeviceSetup)
    assert not isinstance(object(), protocols.DeviceSetup)
    assert not isinstance(SimpleNamespace(), protocols.DeviceSetup)
    assert issubclass(Impl, protocols.DeviceSetup)
    assert not issubclass(SimpleNamespace, protocols.DeviceSetup)
    assert impl("cpu", "float32", 0) is None

    class NoCall:
        pass

    assert not isinstance(NoCall(), protocols.DeviceSetup)
    assert not issubclass(NoCall, protocols.DeviceSetup)
    assert not isinstance(None, protocols.DeviceSetup)
    # Testing the Protocol's __call__ itself for coverage/runtime check
    call_result = cast(Any, protocols.DeviceSetup).__call__(
        SimpleNamespace(),
        "cpu",
        "float32",
        0,
        cuda_is_available=None,
        torch_module=None,
    )
    assert call_result is None or call_result is Ellipsis
    with pytest.raises(TypeError):
        issubclass(123, protocols.DeviceSetup)  # type: ignore[arg-type]


def test_main_global_options_non_dict_ctx_obj() -> None:
    from ml_playground.runtime_cli import main as cli_main
    import click

    dummy_command = click.Command("dummy")
    ctx = click.Context(dummy_command)
    ctx.obj = "not-a-dict"
    cli_main.apply_global_options(ctx, None, False, 1)

    assert isinstance(ctx.obj, dict)


def test_main_global_options_preexisting_dict_preserved() -> None:
    from ml_playground.runtime_cli import main as cli_main
    import click

    # Use DI hook for click context
    class FakeClickCtx:
        invoked_subcommand = None

        def get_help(self) -> str:
            return "help"

    dummy_command = click.Command("dummy")
    ctx = click.Context(dummy_command)
    ctx.obj = {"exp_config": None}

    # This should no longer raise Exit as the "Welcome" message/exit logic was removed
    cli_main.apply_global_options(
        ctx,
        None,
        False,
        1,
    )

    assert isinstance(ctx.obj, dict)


def test_main_global_options_existing_dict_no_exit() -> None:
    from ml_playground.runtime_cli import main as cli_main

    class FakeClickCtx:
        invoked_subcommand = "prepare"

    class DummyCtx:
        def __init__(self) -> None:
            self.obj: dict[str, object] = {"exp_config": None}

        def ensure_object(self, _typ: Any) -> None:
            return None

    ctx = DummyCtx()
    # Call apply_global_options directly
    result = cli_main.apply_global_options(
        ctx,  # type: ignore
        None,
        True,
        2,
    )

    assert result is None
    assert ctx.obj["learning_mode"] is True
    assert ctx.obj["verbosity"] == cli_main.VerbosityLevel(2)
