# pyright: reportPrivateUsage=false
"""Tests for `ml_playground.analysis.lit.integration` utilities."""

from __future__ import annotations

import sys
from pathlib import Path
from types import ModuleType
from typing import Dict, Mapping, Optional, cast

import pytest

from ml_playground.analysis.lit import integration


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
        dataset_mod, model_mod, types_mod = integration.load_lit_components()
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
        imported = integration.import_lit_server()
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
            integration.import_lit_server()

        exception = cast(Exception, exc_info.value)
        assert "Unable to import LIT server module" in str(exception)
        assert "9.9.9" in str(exception)
    finally:
        _restore_modules(originals)


def test_run_server_bundestag_char_invokes_server_factory(tmp_path: Path) -> None:
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
        instances: list["ServerApp"] = []

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
        integration.run_server_bundestag_char(
            host="0.0.0.0", port=8080, open_browser=True
        )
        assert ServerApp.instances, "Server factory should be invoked"
        app = ServerApp.instances[-1]
        assert "echo_model" in app.models
    finally:
        _restore_modules(originals)


def test_import_lit_server_falls_back_to_runtime_server() -> None:
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

        imported = integration.import_lit_server()
        assert imported is runtime_server_module
    finally:
        _restore_modules(originals)
