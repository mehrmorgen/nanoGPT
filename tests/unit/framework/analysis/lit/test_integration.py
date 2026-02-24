# pyright: reportPrivateUsage=false
"""Tests for `ml_playground.schema.lit.integration` utilities."""

from __future__ import annotations

import sys
from types import ModuleType
from typing import Dict, Optional, Any

import pytest

from ml_playground.framework.analysis.lit import integration
from ml_playground.framework.core.protocols import (
    ModuleImporter,
)


class StubModuleImporter(ModuleImporter):
    """Stub implementation for ModuleImporter."""

    def __init__(self, fail_import: bool = False) -> None:
        self.fail_import = fail_import

    def import_api_module(self) -> object:
        if self.fail_import:
            raise ImportError("missing lit-nlp")
        return ModuleType("lit_nlp.api")

    def import_dataset_module(self) -> object:
        return ModuleType("lit_nlp.api.dataset")

    def import_model_module(self) -> object:
        return ModuleType("lit_nlp.api.model")

    def import_types_module(self) -> object:
        return ModuleType("lit_nlp.api.types")


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

    class StubDataset:  # noqa: D401 - simple stub
        """Dataset stub."""

    class StubModel:  # noqa: D401 - simple stub
        """Model stub."""

    def text_segment() -> str:
        return "segment"

    # We use __dict__ assignment to avoid setattr lints if needed,
    # but here we are just setting attributes on a local ModuleType object.
    # Actually, direct attribute access on ModuleType is fine.
    dataset_module.Dataset = StubDataset  # type: ignore[attr-defined]
    model_module.Model = StubModel  # type: ignore[attr-defined]
    types_module.TextSegment = text_segment  # type: ignore[attr-defined]

    class ConfiguredImporter(StubModuleImporter):
        def import_dataset_module(self) -> object:
            return dataset_module

        def import_model_module(self) -> object:
            return model_module

        def import_types_module(self) -> object:
            return types_module

    importer = ConfiguredImporter()
    dataset_mod, model_mod, types_mod = integration._load_lit_components(
        importer=importer
    )
    assert dataset_mod.Dataset is StubDataset
    assert model_mod.Model is StubModel
    assert types_mod.TextSegment() == "segment"


def test_load_lit_components_requires_lit_dependencies() -> None:
    """Import failures should explain how to install lit dependencies."""
    importer = StubModuleImporter(fail_import=True)
    with pytest.raises(RuntimeError, match="LIT dependencies are unavailable") as exc:
        integration._load_lit_components(importer=importer)
    assert "uv sync --extra lit" in str(exc.value)


def test_import_lit_server_prefers_primary_module() -> None:
    """`_import_lit_server` should return the first available server module."""
    server_module = ModuleType("lit_nlp.server")

    def fake_import(name: str) -> Any:
        if name == "lit_nlp.server":
            return server_module
        raise ImportError(name)

    imported = integration._import_lit_server(import_fn=fake_import)
    assert imported is server_module


def test_import_lit_server_raises_runtime_error_with_version_hint() -> None:
    """Missing server modules should raise `RuntimeError` including version info."""
    lit_pkg = ModuleType("lit_nlp")
    lit_pkg.__version__ = "9.9.9"  # type: ignore[attr-defined]

    def fake_import(name: str) -> Any:
        if name == "lit_nlp":
            return lit_pkg
        raise ImportError(f"No module named '{name}'")

    with pytest.raises(RuntimeError) as exc_info:
        integration._import_lit_server(import_fn=fake_import)

    message = str(exc_info.value)
    assert "Unable to import LIT server module" in message
    assert "9.9.9" in message


def test_parse_cli_args_parses_values() -> None:
    """CLI argument parser should return the expected tuple of values."""

    host, port, open_browser = integration._parse_cli_args(
        ["--host", "0.0.0.0", "--port", "1234", "--open-browser"]
    )

    assert host == "0.0.0.0"
    assert port == 1234
    assert open_browser is True


def test_parse_cli_args_type_validation_host() -> None:
    """CLI argument parser should validate host type."""

    import argparse

    class BadArgs:
        host = 123
        port = 5432
        open_browser = False

    class StubParser(argparse.ArgumentParser):
        def parse_args(self, *a: Any, **k: Any) -> Any:
            return BadArgs()

    with pytest.raises(TypeError, match="--host must be a string"):
        integration._parse_cli_args([], parser_factory=lambda **k: StubParser())


def test_parse_cli_args_type_validation_port() -> None:
    """CLI argument parser should validate port type."""

    import argparse

    class BadArgs:
        host = "localhost"
        port = "not-an-int"
        open_browser = False

    class StubParser(argparse.ArgumentParser):
        def parse_args(self, *a: Any, **k: Any) -> Any:
            return BadArgs()

    with pytest.raises(TypeError, match="--port must be parsed as an integer"):
        integration._parse_cli_args([], parser_factory=lambda **k: StubParser())


def test_resolve_experiment_lit_runner_finds_module() -> None:
    """`_resolve_experiment_lit_runner` should locate and return the module's run_server."""

    def stub_runner(**k: Any) -> None:
        return None

    stub_module = ModuleType("stub_lit_integration")
    stub_module.run_server = stub_runner  # type: ignore

    def stub_import(name: str) -> Any:
        if name == "ml_playground.experiments.test_exp.lit_integration":
            return stub_module
        raise ImportError(name)

    runner = integration._resolve_experiment_lit_runner(
        "test_exp", import_fn=stub_import
    )
    assert runner is stub_runner


def test_resolve_experiment_lit_runner_finds_legacy_name() -> None:
    """`_resolve_experiment_lit_runner` should fall back to legacy runner name."""

    def stub_runner(**k: Any) -> None:
        return None

    stub_module = ModuleType("stub_lit_integration")
    # We use __dict__ to avoid setattr lint if needed, although direct assignment on ModuleType is usually okay
    # unless strictly forbidden by the dynamic attribute access check.
    # The check flags 'setattr' calls, not attribute assignment.
    stub_module.run_server_test_exp = stub_runner  # type: ignore

    def stub_import(name: str) -> Any:
        if name == "ml_playground.experiments.test_exp.lit_integration":
            return stub_module
        raise ImportError(name)

    runner = integration._resolve_experiment_lit_runner(
        "test_exp", import_fn=stub_import
    )
    assert runner is stub_runner


def test_resolve_experiment_lit_runner_raises_on_missing_module() -> None:
    def stub_import(name: str) -> Any:
        raise ImportError(name)

    with pytest.raises(RuntimeError, match="No LIT integration module registered"):
        integration._resolve_experiment_lit_runner("missing", import_fn=stub_import)


def test_resolve_experiment_lit_runner_raises_on_missing_runner() -> None:
    stub_module = ModuleType("stub_lit_integration")

    def stub_import(name: str) -> Any:
        return stub_module

    with pytest.raises(RuntimeError, match="does not expose run_server"):
        integration._resolve_experiment_lit_runner("test_exp", import_fn=stub_import)
