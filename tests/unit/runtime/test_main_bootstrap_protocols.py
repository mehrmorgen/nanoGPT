from __future__ import annotations

import sys
from types import SimpleNamespace
from typing import Any, cast

import importlib
import pytest

from ml_playground.runtime import protocols
from ml_playground.runtime.core import bootstrap
from ml_playground.runtime.cli import device as cli_device


def _cli_main_module():
    return importlib.import_module("ml_playground.runtime.cli.main")


def test_main_complete_experiments_uses_config_loading(monkeypatch: Any) -> None:
    calls: list[tuple[str, ...]] = []
    cli_main_module = _cli_main_module()

    def fake_list(prefix: str) -> list[str]:
        calls.append((prefix,))
        return ["demo"]

    monkeypatch.setattr(
        cli_main_module.config_loading, "list_experiments_with_config", fake_list
    )
    complete_experiments = cli_main_module.__dict__["_complete_experiments"]
    result = complete_experiments("de")
    assert result == ["demo"]
    assert calls == [("de",)]


def test_main_cli_module_imports_when_missing(monkeypatch: Any) -> None:
    monkeypatch.delitem(sys.modules, "ml_playground.runtime.cli", raising=False)
    sentinel = SimpleNamespace(marker="loaded-main-cli")
    cli_main_module = _cli_main_module()

    def fake_import(name: str) -> SimpleNamespace:  # noqa: ARG001
        return sentinel

    monkeypatch.setattr(cli_main_module.importlib, "import_module", fake_import)
    cli_module = cli_main_module.__dict__["_cli_module"]
    mod = cli_module()
    assert mod is sentinel


def test_main_global_options_merges_existing_ctx(monkeypatch: Any) -> None:
    cli_main_module = _cli_main_module()

    class FakeClickCtx:
        invoked_subcommand = "prepare"

        def get_help(self) -> str:
            return "help"

    monkeypatch.setattr(
        cli_main_module.click, "get_current_context", lambda silent=True: FakeClickCtx()
    )  # noqa: ARG005

    dummy_command = cli_main_module.click.Command("dummy")
    ctx = cli_main_module.click.Context(dummy_command)
    ctx.obj = {"existing": 1}
    cli_main_module.global_options(ctx, None, True, 2)

    assert ctx.obj["existing"] == 1
    assert ctx.obj["learning_mode"] is True
    assert ctx.obj["verbosity"] == cli_main_module.VerbosityLevel(2)


def test_bootstrap_guard_raises_when_unconfigured(monkeypatch: Any) -> None:
    monkeypatch.setattr(bootstrap, "_default_factory", None)
    monkeypatch.setattr(bootstrap, "_current", None)
    with pytest.raises(RuntimeError):
        bootstrap.get_runtime_cli_dependencies()


def test_bootstrap_lazy_initializes_from_default(monkeypatch: Any) -> None:
    called: list[str] = []

    def factory() -> bootstrap.CLIDependencies:
        called.append("factory")
        return bootstrap.CLIDependencies(
            load_experiment=lambda e, p: None,  # noqa: ARG005
            ensure_train_prerequisites=lambda cfg: None,  # noqa: ARG005
            ensure_sample_prerequisites=lambda cfg: None,  # noqa: ARG005
            run_prepare=lambda a, b, c, d, e: None,  # noqa: ARG005
            run_train=lambda a, b, c, d, e: None,  # noqa: ARG005
            run_sample=lambda a, b, c, d, e: None,  # noqa: ARG005
        )

    monkeypatch.setattr(bootstrap, "_default_factory", factory)
    monkeypatch.setattr(bootstrap, "_current", None)

    deps = bootstrap.get_runtime_cli_dependencies()
    assert called == ["factory"]
    assert isinstance(deps, bootstrap.CLIDependencies)


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


def test_main_global_options_non_dict_ctx_obj(monkeypatch: Any) -> None:
    cli_main_module = _cli_main_module()

    class FakeClickCtx:
        invoked_subcommand = "prepare"

        def get_help(self) -> str:
            return "help"

    monkeypatch.setattr(
        cli_main_module.click, "get_current_context", lambda silent=True: FakeClickCtx()
    )  # noqa: ARG005

    dummy_command = cli_main_module.click.Command("dummy")
    ctx = cli_main_module.click.Context(dummy_command)
    ctx.obj = "not-a-dict"
    cli_main_module.global_options(ctx, None, False, 1)

    assert isinstance(ctx.obj, dict)


def test_main_global_options_preexisting_dict_preserved(
    monkeypatch: Any,
) -> None:
    cli_main_module = _cli_main_module()

    class FakeClickCtx:
        invoked_subcommand = None

        def get_help(self) -> str:
            return "help"

    monkeypatch.setattr(
        cli_main_module.click, "get_current_context", lambda silent=True: FakeClickCtx()
    )  # noqa: ARG005

    dummy_command = cli_main_module.click.Command("dummy")
    ctx = cli_main_module.click.Context(dummy_command)
    ctx.obj = {"exp_config": None}
    with pytest.raises(cli_main_module.typer.Exit):
        cli_main_module.global_options(ctx, None, False, 1)

    assert isinstance(ctx.obj, dict)
    assert "exp_config" in ctx.obj
    # With learning_mode default False, it should not be injected
    assert "learning_mode" not in ctx.obj
    # Verbosity should be left default (STANDARD) when unchanged
    assert "verbosity" not in ctx.obj


def test_main_global_options_existing_dict_no_exit(
    monkeypatch: Any,
) -> None:
    cli_main_module = _cli_main_module()

    class FakeClickCtx:
        invoked_subcommand = "prepare"

        def get_help(self) -> str:
            return "help"

    monkeypatch.setattr(
        cli_main_module.click, "get_current_context", lambda silent=True: FakeClickCtx()
    )  # noqa: ARG005

    class DummyCtx:
        def __init__(self) -> None:
            self.obj: dict[str, object] = {"exp_config": None}

        def ensure_object(self, typ: Any) -> None:  # noqa: ANN401
            return None

    ctx = DummyCtx()
    result = cli_main_module.global_options(ctx, None, True, 2)

    assert result is None
    assert ctx.obj["learning_mode"] is True
    assert ctx.obj["verbosity"] == cli_main_module.VerbosityLevel(2)


def test_main_global_options_obj_not_dict_path(monkeypatch: Any) -> None:
    cli_main_module = _cli_main_module()

    class FakeClickCtx:
        invoked_subcommand = "prepare"

        def get_help(self) -> str:
            return "help"

    monkeypatch.setattr(
        cli_main_module.click, "get_current_context", lambda silent=True: FakeClickCtx()
    )  # noqa: ARG005

    class DummyCtx:
        def __init__(self) -> None:
            self.obj: Any = "string"

        def ensure_object(self, typ: Any) -> None:  # noqa: ANN401
            return None

    ctx = DummyCtx()
    result = cli_main_module.global_options(ctx, None, False, 1)

    assert result is None
    assert isinstance(ctx.obj, dict)
    ctx_dict = cast(dict[str, object], ctx.obj)
    assert "verbosity" not in ctx_dict


def test_main_global_options_click_ctx_dict_path(
    monkeypatch: Any,
) -> None:
    cli_main_module = _cli_main_module()

    class FakeClickCtx:
        invoked_subcommand = "prepare"

        def get_help(self) -> str:
            return "help"

    monkeypatch.setattr(
        cli_main_module.click, "get_current_context", lambda silent=True: FakeClickCtx()
    )  # noqa: ARG005

    dummy_command = cli_main_module.click.Command("dummy")
    ctx = cli_main_module.click.Context(dummy_command, obj={"exp_config": None})
    result = cli_main_module.global_options(ctx, None, False, 1)

    assert result is None
    assert isinstance(ctx.obj, dict)
    ctx_dict = cast(dict[str, object], ctx.obj)
    assert "exp_config" in ctx_dict


def test_global_device_setup_defensive_path(monkeypatch: Any) -> None:
    called: list[str] = []

    def bad_setup(*_: object, **__: object) -> None:
        called.append("setup")
        raise RuntimeError("fail")

    dummy_module = SimpleNamespace(global_device_setup=bad_setup)

    def fake_import(name: str) -> SimpleNamespace:  # noqa: ARG001
        return dummy_module

    monkeypatch.setattr(cli_device.importlib, "import_module", fake_import)

    result = cli_device.global_device_setup("cpu", "float32", 0)

    assert result is None
    assert called == ["setup"]
