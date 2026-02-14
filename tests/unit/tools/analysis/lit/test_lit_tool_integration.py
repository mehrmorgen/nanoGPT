# pyright: reportPrivateUsage=false
"""Tests for `ml_playground.tools.analysis.lit.integration` utilities."""

from __future__ import annotations

import sys
from contextlib import contextmanager
from pathlib import Path
from types import ModuleType, SimpleNamespace
from typing import Dict, Mapping, Optional

import pytest

from ml_playground.tools.analysis.lit import integration


@contextmanager
def override_attr(target: object, name: str, value: object):
    missing = object()
    original = getattr(target, name, missing)
    setattr(target, name, value)
    try:
        yield
    finally:
        if original is missing:
            if hasattr(target, "__delattr__"):
                target.__delattr__(name)
        else:
            setattr(target, name, original)


def _install_modules(modules: Dict[str, ModuleType]) -> Dict[str, Optional[ModuleType]]:
    originals: Dict[str, Optional[ModuleType]] = {}
    for name, module in modules.items():
        originals[name] = sys.modules.get(name)
        sys.modules[name] = module
    return originals


def _restore_modules(originals: Dict[str, Optional[ModuleType]]) -> None:
    for name, original in originals.items():
        if original is None:
            sys.modules.pop(name, None)
        else:
            sys.modules[name] = original


def test_load_lit_components_uses_expected_modules() -> None:
    """`_load_lit_components` should import dataset, model, and types modules."""
    dataset_module = ModuleType("lit_nlp.api.dataset")
    model_module = ModuleType("lit_nlp.api.model")
    types_module = ModuleType("lit_nlp.api.types")

    class DummyDataset:  # noqa: D401 - simple stub
        """Dataset stub."""

    class DummyModel:  # noqa: D401 - simple stub
        """Model stub."""

    def text_segment() -> str:
        return "segment"

    dataset_module.Dataset = DummyDataset  # type: ignore[attr-defined]
    model_module.Model = DummyModel  # type: ignore[attr-defined]
    types_module.TextSegment = text_segment  # type: ignore[attr-defined]

    api_module = ModuleType("lit_nlp.api")

    modules = {
        "lit_nlp.api": api_module,
        "lit_nlp.api.dataset": dataset_module,
        "lit_nlp.api.model": model_module,
        "lit_nlp.api.types": types_module,
    }
    originals = _install_modules(modules)
    try:
        dataset_mod, model_mod, types_mod = integration._load_lit_components()
        assert dataset_mod.Dataset is DummyDataset
        assert model_mod.Model is DummyModel
        assert types_mod.TextSegment is text_segment
    finally:
        _restore_modules(originals)


def test_import_lit_server_prefers_primary_module() -> None:
    """`_import_lit_server` should return the first available server module."""
    server_module = ModuleType("lit_nlp.server")
    originals = _install_modules({"lit_nlp.server": server_module})
    try:
        imported = integration._import_lit_server()
        assert imported is server_module
    finally:
        _restore_modules(originals)


def test_import_lit_server_raises_runtime_error_with_version_hint() -> None:
    """Missing server modules should raise `RuntimeError` including version info."""
    lit_pkg = ModuleType("lit_nlp")
    lit_pkg.__version__ = "9.9.9"  # type: ignore[attr-defined]

    originals = _install_modules({"lit_nlp": lit_pkg})
    try:
        for name in [
            "lit_nlp.server",
            "lit_nlp.dev_server",
            "lit_nlp.runtime.server",
            "lit_nlp.lib.server",
        ]:
            sys.modules.pop(name, None)

        with pytest.raises(RuntimeError) as exc_info:
            integration._import_lit_server()

        # Accessing exc_info.value directly causes Pyright errors due to its complex type.
        # We convert to string for assertion which is enough for coverage.
        assert "Unable to import LIT server module" in str(exc_info.value)
        assert "9.9.9" in str(exc_info.value)
    finally:
        _restore_modules(originals)


def test_load_lit_components_raises_runtime_error_on_missing_api() -> None:
    """`_load_lit_components` should raise `RuntimeError` if lit_nlp.api is missing."""
    originals: dict[str, ModuleType | None] = {}
    if "lit_nlp.api" in sys.modules:
        originals["lit_nlp.api"] = sys.modules.pop("lit_nlp.api")

    try:
        # Also need to make sure it's not importable
        with pytest.raises(RuntimeError) as exc_info:
            integration._load_lit_components()
        assert "LIT dependencies are unavailable" in str(exc_info.value)
    finally:
        for name_to_restore, mod_to_restore in originals.items():
            if mod_to_restore is not None:
                sys.modules[name_to_restore] = mod_to_restore
            else:
                sys.modules.pop(name_to_restore, None)


def test_run_server_bundestag_char_input_file_logic(tmp_path: Path) -> None:
    """Test reading from input.txt if it exists."""
    dataset_module = ModuleType("lit_nlp.api.dataset")
    model_module = ModuleType("lit_nlp.api.model")
    types_module = ModuleType("lit_nlp.api.types")
    dataset_module.Dataset = object  # type: ignore
    model_module.Model = object  # type: ignore
    types_module.TextSegment = lambda: None  # type: ignore

    server_module = ModuleType("lit_nlp.server")
    server_module.Server = lambda m, d: SimpleNamespace(serve=lambda **_kw: None)  # type: ignore

    modules = {
        "lit_nlp.api": ModuleType("lit_nlp.api"),
        "lit_nlp.api.dataset": dataset_module,
        "lit_nlp.api.model": model_module,
        "lit_nlp.api.types": types_module,
        "lit_nlp.server": server_module,
    }
    originals = _install_modules(modules)

    # Create a real file in tmp_path
    exp_dir = tmp_path / "experiments" / "bundestag_char"
    datasets_dir = exp_dir / "datasets"
    datasets_dir.mkdir(parents=True)
    input_file = datasets_dir / "input.txt"
    input_file.write_text("Line 1\nLine 2\n")

    # Surgical mock of Path to avoid hanging
    # We want to mock Path in the module to return a path that leads to our tmp_path
    class FakePath:
        def __init__(self, *args: object, **kwargs: object) -> None:
            self.parents = {2: tmp_path}

        def resolve(self) -> "FakePath":
            return self

    with override_attr(integration, "Path", FakePath):
        integration.run_server_bundestag_char()

    _restore_modules(originals)


def test_parse_cli_args_branches() -> None:
    """Test all branches of _parse_cli_args."""
    # Default
    h, p, o = integration._parse_cli_args([])
    assert h == "127.0.0.1"
    assert p == 5432
    assert o is False

    # Overrides
    h, p, o = integration._parse_cli_args(
        ["--host", "localhost", "--port", "1234", "--open-browser"]
    )
    assert h == "localhost"
    assert p == 1234
    assert o is True

    # TypeError for host
    def fake_parse_args_host_invalid():
        return SimpleNamespace(host=123, port=5432, open_browser=False)

    def fake_parse_args_browser_invalid():
        return SimpleNamespace(host="localhost", port=5432, open_browser="not-a-bool")

    with override_attr(
        integration.argparse.ArgumentParser,
        "parse_args",
        lambda self, argv=None: fake_parse_args_host_invalid(),
    ):
        with pytest.raises(TypeError, match="--host must be a string"):
            integration._parse_cli_args([])

    with override_attr(
        integration.argparse.ArgumentParser,
        "parse_args",
        lambda self, argv=None: fake_parse_args_browser_invalid(),
    ):
        h, p, o = integration._parse_cli_args([])
        assert o is True  # bool("not-a-bool") is True


def test_run_server_bundestag_char_invokes_server_factory() -> None:
    """`run_server_bundestag_char` should build datasets, models, and start the server."""
    dataset_module = ModuleType("lit_nlp.api.dataset")
    model_module = ModuleType("lit_nlp.api.model")
    types_module = ModuleType("lit_nlp.api.types")

    class DummyDatasetBase:  # noqa: D401 - simple stub
        """Base dataset stub."""

    class DummyModelBase:  # noqa: D401 - simple stub
        """Base model stub."""

    def text_segment() -> str:
        return "segment"

    dataset_module.Dataset = DummyDatasetBase  # type: ignore[attr-defined]
    model_module.Model = DummyModelBase  # type: ignore[attr-defined]
    types_module.TextSegment = text_segment  # type: ignore[attr-defined]

    api_module = ModuleType("lit_nlp.api")
    server_module = ModuleType("lit_nlp.server")

    class ServerApp:
        instances: list[ServerApp] = []

        def __init__(
            self,
            models: Mapping[str, object],
            datasets: Mapping[str, object],
        ) -> None:
            self.models = models
            self.datasets = datasets
            self.calls: list[tuple[int, str, bool]] = []
            ServerApp.instances.append(self)

        def serve(self, *, port: int, host: str, open_browser: bool) -> None:
            self.calls.append((port, host, open_browser))

    class ServerFactory:
        def __call__(
            self, models: Mapping[str, object], datasets: Mapping[str, object]
        ) -> ServerApp:
            return ServerApp(models, datasets)

    server_module.Server = ServerFactory()  # type: ignore[attr-defined]

    modules = {
        "lit_nlp.api": api_module,
        "lit_nlp.api.dataset": dataset_module,
        "lit_nlp.api.model": model_module,
        "lit_nlp.api.types": types_module,
        "lit_nlp.server": server_module,
    }
    originals = _install_modules(modules)
    try:
        import logging

        logger = logging.getLogger("test_logger")
        integration.run_server_bundestag_char(
            host="0.0.0.0", port=1234, open_browser=True, logger=logger
        )
        assert ServerApp.instances, "Server factory should be invoked"
        app = ServerApp.instances[-1]
        assert "echo_model" in app.models
        assert "bundestag_char_sample" in app.datasets
        assert app.calls == [(1234, "0.0.0.0", True)]

        # Exercise dataset and model methods
        dataset = app.datasets["bundestag_char_sample"]
        assert len(dataset) > 0  # type: ignore
        assert "text" in dataset.spec()  # type: ignore
        assert next(iter(dataset))["text"]  # type: ignore

        model = app.models["echo_model"]
        assert "text" in model.input_spec()  # type: ignore
        assert "generated" in model.output_spec()  # type: ignore
        preds = model.predict([{"text": "hello"}])  # type: ignore
        assert preds[0]["generated"] == "hello\n\n[echo] olleh"
    finally:
        _restore_modules(originals)
        ServerApp.instances.clear()


def test_run_server_bundestag_char_all_fails() -> None:
    """Test when all serve attempts fail."""
    dataset_module = ModuleType("lit_nlp.api.dataset")
    model_module = ModuleType("lit_nlp.api.model")
    types_module = ModuleType("lit_nlp.api.types")
    dataset_module.Dataset = object  # type: ignore
    model_module.Model = object  # type: ignore
    types_module.TextSegment = lambda: None  # type: ignore

    server_module = ModuleType("lit_nlp.server")
    server_module.Server = lambda m, d: object()  # type: ignore

    modules = {
        "lit_nlp.api": ModuleType("lit_nlp.api"),
        "lit_nlp.api.dataset": dataset_module,
        "lit_nlp.api.model": model_module,
        "lit_nlp.api.types": types_module,
        "lit_nlp.server": server_module,
    }

    originals = _install_modules(modules)
    try:
        with pytest.raises(
            RuntimeError, match="LIT server app does not expose a serve"
        ):
            integration.run_server_bundestag_char()
    finally:
        _restore_modules(originals)


def test_run_server_bundestag_char_empty_input_file(tmp_path: Path) -> None:
    """Test handling of empty input.txt."""
    dataset_module = ModuleType("lit_nlp.api.dataset")
    model_module = ModuleType("lit_nlp.api.model")
    types_module = ModuleType("lit_nlp.api.types")
    dataset_module.Dataset = object  # type: ignore
    model_module.Model = object  # type: ignore
    types_module.TextSegment = lambda: None  # type: ignore
    server_module = ModuleType("lit_nlp.server")
    server_module.Server = lambda m, d: SimpleNamespace(serve=lambda **_kw: None)  # type: ignore

    modules = {
        "lit_nlp.api": ModuleType("lit_nlp.api"),
        "lit_nlp.api.dataset": dataset_module,
        "lit_nlp.api.model": model_module,
        "lit_nlp.api.types": types_module,
        "lit_nlp.server": server_module,
    }
    originals = _install_modules(modules)

    # Create an empty file
    exp_dir = tmp_path / "experiments" / "bundestag_char"
    datasets_dir = exp_dir / "datasets"
    datasets_dir.mkdir(parents=True)
    input_file = datasets_dir / "input.txt"
    input_file.write_text("\n\n")  # only whitespace -> file_lines will be empty

    class FakePath:
        def __init__(self, *args: object, **kwargs: object) -> None:
            self.parents = {2: tmp_path}

        def resolve(self) -> "FakePath":
            return self

    with override_attr(integration, "Path", FakePath):
        integration.run_server_bundestag_char()

    _restore_modules(originals)


def test_run_server_bundestag_char_non_callable_factory() -> None:
    """Test failure when Server factory is not callable."""
    dataset_module = ModuleType("lit_nlp.api.dataset")
    model_module = ModuleType("lit_nlp.api.model")
    types_module = ModuleType("lit_nlp.api.types")
    dataset_module.Dataset = object  # type: ignore
    model_module.Model = object  # type: ignore
    types_module.TextSegment = lambda: None  # type: ignore
    server_module = ModuleType("lit_nlp.server")
    server_module.Server = "not-callable"  # type: ignore

    modules = {
        "lit_nlp.api": ModuleType("lit_nlp.api"),
        "lit_nlp.api.dataset": dataset_module,
        "lit_nlp.api.model": model_module,
        "lit_nlp.api.types": types_module,
        "lit_nlp.server": server_module,
    }
    originals = _install_modules(modules)
    try:
        with pytest.raises(
            RuntimeError, match="LIT server module does not expose a Server factory"
        ):
            integration.run_server_bundestag_char()
    finally:
        _restore_modules(originals)


def test_parse_cli_args_type_validation() -> None:
    """`_import_lit_server` should fall back to `runtime.server` if `dev_server` is missing."""
    server_module = ModuleType("lit_nlp.server")
    dev_server_module = ModuleType("lit_nlp.dev_server")
    runtime_server_module = ModuleType("lit_nlp.runtime.server")

    modules = {
        "lit_nlp.server": server_module,
        "lit_nlp.dev_server": dev_server_module,
        "lit_nlp.runtime.server": runtime_server_module,
    }
    originals = _install_modules(modules)
    try:
        for name in ["lit_nlp.dev_server", "lit_nlp.server"]:
            sys.modules.pop(name, None)

        imported = integration._import_lit_server()
        assert imported is runtime_server_module
    finally:
        _restore_modules(originals)
