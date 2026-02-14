from __future__ import annotations

from types import SimpleNamespace
from typing import Any, cast

import importlib
import pytest
from contextlib import contextmanager

from ml_playground.framework.runtime import protocols
from ml_playground.framework.runtime.core import bootstrap


@contextmanager
def override_attr(target: object, name: str, value: object):
    missing = object()
    original = getattr(target, name, missing)
    object.__setattr__(target, name, value)
    try:
        yield
    finally:
        restore_attr(target, name, original, original is not missing)


def restore_attr(target: object, name: str, original: object, had: bool) -> None:
    if had:
        object.__setattr__(target, name, original)
    else:
        delattr(target, name)


@contextmanager
def override_mapping(
    mapping: dict, key: str, value: object | None = None, *, delete: bool = False
):
    had = key in mapping
    original = mapping.get(key)
    if delete:
        mapping.pop(key, None)
    else:
        mapping[key] = value
    try:
        yield
    finally:
        if had:
            mapping[key] = original
        else:
            mapping.pop(key, None)


def _cli_main_module():
    return importlib.import_module("ml_playground.runtime_cli.main")


def test_main_complete_experiments_uses_config_loading() -> None:
    calls: list[tuple[str, ...]] = []
    cli_main_module = _cli_main_module()

    def fake_list(prefix: str) -> list[str]:
        calls.append((prefix,))
        return ["demo"]

    with override_attr(
        cli_main_module.config_loading, "list_experiments_with_config", fake_list
    ):
        complete_experiments = cli_main_module.__dict__["_complete_experiments"]
        result = complete_experiments("de")
        assert result == ["demo"]
        assert calls == [("de",)]


def test_main_global_options_merges_existing_ctx() -> None:
    cli_main_module = _cli_main_module()

    class FakeClickCtx:
        invoked_subcommand = "prepare"

        def get_help(self) -> str:
            return "help"

    with override_attr(
        cli_main_module.click, "get_current_context", lambda silent=True: FakeClickCtx()
    ):  # noqa: ARG005
        dummy_command = cli_main_module.click.Command("dummy")
        ctx = cli_main_module.click.Context(dummy_command)
        ctx.obj = {"existing": 1}
        cli_main_module.global_options(ctx, None, True, 2)

    assert ctx.obj["existing"] == 1
    assert ctx.obj["learning_mode"] is True
    assert ctx.obj["verbosity"] == cli_main_module.VerbosityLevel(2)


def test_bootstrap_guard_raises_when_unconfigured() -> None:
    with (
        override_attr(bootstrap, "_default_factory", None),
        override_attr(bootstrap, "_current", None),
    ):
        with pytest.raises(RuntimeError):
            bootstrap.get_cli_dependencies()


def test_bootstrap_lazy_initializes_from_default() -> None:
    called: list[str] = []

    def factory() -> bootstrap.CLIDependencies:
        called.append("factory")
        return bootstrap.CLIDependencies(
            load_experiment=lambda e, p: None,  # noqa: ARG005
            ensure_train_prerequisites=lambda cfg: None,  # noqa: ARG005
            ensure_sample_prerequisites=lambda cfg: None,  # noqa: ARG005
            run_prepare=lambda a, b, c, d, e, f=None: None,  # noqa: ARG005
            run_train=lambda a, b, c, d, e, f=None: None,  # noqa: ARG005
            run_sample=lambda a, b, c, d, e, f=None: None,  # noqa: ARG005
        )

    with (
        override_attr(bootstrap, "_default_factory", factory),
        override_attr(bootstrap, "_current", None),
    ):
        deps = bootstrap.get_cli_dependencies()
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


def test_main_global_options_non_dict_ctx_obj() -> None:
    cli_main_module = _cli_main_module()

    class FakeClickCtx:
        invoked_subcommand = "prepare"

        def get_help(self) -> str:
            return "help"

    with override_attr(
        cli_main_module.click, "get_current_context", lambda silent=True: FakeClickCtx()
    ):  # noqa: ARG005
        dummy_command = cli_main_module.click.Command("dummy")
        ctx = cli_main_module.click.Context(dummy_command)
        ctx.obj = "not-a-dict"
        cli_main_module.global_options(ctx, None, False, 1)

    assert isinstance(ctx.obj, dict)


def test_main_global_options_preexisting_dict_preserved() -> None:
    cli_main_module = _cli_main_module()

    class FakeClickCtx:
        invoked_subcommand = None

        def get_help(self) -> str:
            return "help"

    with override_attr(
        cli_main_module.click, "get_current_context", lambda silent=True: FakeClickCtx()
    ):  # noqa: ARG005
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


def test_main_global_options_existing_dict_no_exit() -> None:
    cli_main_module = _cli_main_module()

    class FakeClickCtx:
        invoked_subcommand = "prepare"

        def get_help(self) -> str:
            return "help"

    with override_attr(
        cli_main_module.click, "get_current_context", lambda silent=True: FakeClickCtx()
    ):  # noqa: ARG005

        class DummyCtx:
            def __init__(self) -> None:
                self.obj: dict[str, object] = {"exp_config": None}

            def ensure_object(self, _typ: Any) -> None:  # noqa: ANN401
                return None

        ctx = DummyCtx()
        result = cli_main_module.global_options(ctx, None, True, 2)

    assert result is None
    assert ctx.obj["learning_mode"] is True
    assert ctx.obj["verbosity"] == cli_main_module.VerbosityLevel(2)


def test_main_global_options_obj_not_dict_path() -> None:
    cli_main_module = _cli_main_module()

    class FakeClickCtx:
        invoked_subcommand = "prepare"

        def get_help(self) -> str:
            return "help"

    with override_attr(
        cli_main_module.click, "get_current_context", lambda silent=True: FakeClickCtx()
    ):  # noqa: ARG005

        class DummyCtx:
            def __init__(self) -> None:
                self.obj: Any = "string"

            def ensure_object(self, _typ: Any) -> None:  # noqa: ANN401
                return None

        ctx = DummyCtx()
        result = cli_main_module.global_options(ctx, None, False, 1)

    assert result is None
    assert isinstance(ctx.obj, dict)
    ctx_dict = cast(dict[str, object], ctx.obj)
    assert "verbosity" not in ctx_dict


def test_main_global_options_click_ctx_dict_path() -> None:
    cli_main_module = _cli_main_module()

    class FakeClickCtx:
        invoked_subcommand = "prepare"

        def get_help(self) -> str:
            return "help"

    with override_attr(
        cli_main_module.click, "get_current_context", lambda silent=True: FakeClickCtx()
    ):  # noqa: ARG005
        dummy_command = cli_main_module.click.Command("dummy")
        ctx = cli_main_module.click.Context(dummy_command, obj={"exp_config": None})
        result = cli_main_module.global_options(ctx, None, False, 1)

    assert result is None
    assert isinstance(ctx.obj, dict)
    ctx_dict = cast(dict[str, object], ctx.obj)
    assert "exp_config" in ctx_dict
