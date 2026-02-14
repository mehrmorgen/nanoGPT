# pyright: reportPrivateUsage=false
"""Tests for `ml_playground.tools.analysis.lit.integration` utilities."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from types import ModuleType, SimpleNamespace
from typing import Any, Dict, Optional, cast

import pytest

from ml_playground.tools.analysis.lit import integration


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
        exc = cast(Exception, exc_info.value)
        message = str(exc)
        assert "Unable to import LIT server module" in message
        assert "9.9.9" in message
    finally:
        _restore_modules(originals)


def test_load_lit_components_raises_runtime_error_on_missing_api() -> None:
    """`_load_lit_components` should raise `RuntimeError` if lit_nlp.api is missing."""
    originals: dict[str, ModuleType | None] = {}
    if "lit_nlp.api" in sys.modules:
        originals["lit_nlp.api"] = sys.modules.pop("lit_nlp.api")

    try:
        with pytest.raises(RuntimeError) as exc_info:
            integration.load_lit_components()
        exc = cast(Exception, exc_info.value)
        assert "LIT dependencies are unavailable" in str(exc)
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

    class _PathStub:
        def __init__(self, resolved: Path) -> None:
            self._resolved = SimpleNamespace(parents={2: resolved})

        def __call__(self, *args: Any, **kwargs: Any) -> "_PathStub":
            return self

        def resolve(self) -> SimpleNamespace:
            return self._resolved

    original_path = integration.Path
    integration.Path = _PathStub(tmp_path)  # type: ignore[attr-defined]
    try:
        integration.run_server_bundestag_char()
    finally:
        integration.Path = original_path

    _restore_modules(originals)


def test_parse_cli_args_branches() -> None:
    """Test all branches of _parse_cli_args."""
    # Default
    h, p, o = integration.parse_cli_args([])
    assert h == "127.0.0.1"
    assert p == 5432
    assert o is False

    # Overrides
    h, p, o = integration.parse_cli_args(
        ["--host", "localhost", "--port", "1234", "--open-browser"]
    )
    assert h == "localhost"
    assert p == 1234
    assert o is True

    original_parse_args = argparse.ArgumentParser.parse_args

    def _patch_parse_args(result: SimpleNamespace):
        def _impl(_self: Any, *args: Any, **kwargs: Any) -> Any:
            return result

        argparse.ArgumentParser.parse_args = cast(Any, _impl)
        return original_parse_args

    try:
        # TypeError for host
        _patch_parse_args(SimpleNamespace(host=123, port=5432, open_browser=False))
        with pytest.raises(TypeError, match="--host must be a string"):
            integration.parse_cli_args([])

        # Open browser not bool -> coerces truthy value
        _patch_parse_args(
            SimpleNamespace(host="localhost", port=1234, open_browser="nope")
        )
        h2, p2, o2 = integration.parse_cli_args([])

        # Port not int
        _patch_parse_args(
            SimpleNamespace(host="localhost", port="bad", open_browser=False)
        )
        with pytest.raises(TypeError, match="parsed as an integer"):
            integration.parse_cli_args([])
    finally:
        argparse.ArgumentParser.parse_args = original_parse_args
