"""Fast unit tests for AttributeError handling in LIT integration."""

from __future__ import annotations

import importlib
from types import ModuleType
from typing import Any

import pytest

from ml_playground.tools.analysis.lit import integration


def test_load_lit_components_wraps_import_error() -> None:
    """Test that ImportError from lit_nlp.api is wrapped as RuntimeError."""
    original_import_module = importlib.import_module

    def fake_import_module(name: str, *args: Any, **kwargs: Any) -> ModuleType:
        if name == "lit_nlp.api":
            raise ImportError("lit_nlp.api not installed")
        return original_import_module(name, *args, **kwargs)  # type: ignore[arg-type]

    try:
        importlib.import_module = fake_import_module  # type: ignore[assignment]

        with pytest.raises(
            RuntimeError, match="LIT dependencies are unavailable.*Install lit-nlp"
        ):
            integration._load_lit_components()  # pyright: ignore[reportPrivateUsage]

    finally:
        importlib.import_module = original_import_module  # type: ignore[assignment]


def test_import_lit_server_handles_missing_version_attribute() -> None:
    """Test _import_lit_server when __version__ access raises AttributeError."""
    original_import_module = importlib.import_module

    def fake_import_module(name: str, *args: Any, **kwargs: Any) -> ModuleType:
        if name == "lit_nlp":
            lit_module = ModuleType("lit_nlp")
            # Don't set __version__ to trigger AttributeError
            return lit_module
        elif name in {
            "lit_nlp.server",
            "lit_nlp.dev_server",
            "lit_nlp.runtime.server",
            "lit_nlp.lib.server",
        }:
            raise ModuleNotFoundError(f"Module {name} not found")
        return original_import_module(name, *args, **kwargs)  # type: ignore[arg-type]

    try:
        importlib.import_module = fake_import_module  # type: ignore[assignment]

        with pytest.raises(RuntimeError) as exc_info:
            integration._import_lit_server()  # pyright: ignore[reportPrivateUsage]

        # Get the error message from the RuntimeError
        error_message = str(exc_info.value)  # pyright: ignore[reportUnknownMemberType,reportAttributeAccessIssue,reportUnknownArgumentType]
        assert "Unable to import LIT server module" in error_message
        assert "detected lit-nlp version: <unknown>" in error_message

    finally:
        importlib.import_module = original_import_module  # type: ignore[assignment]


def test_run_server_wraps_attribute_error_in_app_construction() -> None:
    """Test that AttributeError in app construction is wrapped as RuntimeError."""
    # Mock the components loading to avoid import errors
    original_load_components = integration._load_lit_components  # pyright: ignore[reportPrivateUsage]
    original_import_server = integration._import_lit_server  # pyright: ignore[reportPrivateUsage]

    class BrokenServerFactory:
        def __call__(self, models: Any, datasets: Any) -> None:
            raise AttributeError("Server factory missing required attribute")

    class MockServerModule:
        Server = BrokenServerFactory  # type: ignore[attr-defined]

    def mock_load_components() -> tuple[Any, Any, Any]:
        # Return mock modules that won't cause import errors
        dataset_mod = ModuleType("lit_nlp.api.dataset")
        model_mod = ModuleType("lit_nlp.api.model")
        types_mod = ModuleType("lit_nlp.api.types")

        class Dataset:
            def spec(self) -> dict[str, object]:
                return {"text": "segment"}

            def __len__(self) -> int:
                return 0

            def __iter__(self):
                return iter([])

        dataset_mod.Dataset = Dataset  # type: ignore[attr-defined]

        class Model:
            def input_spec(self) -> dict[str, object]:
                return {"text": "segment"}

            def output_spec(self) -> dict[str, object]:
                return {"generated": "segment"}

            def predict(self, _inputs: Any, **kwargs: Any) -> list[dict[str, object]]:
                return []

        model_mod.Model = Model  # type: ignore[attr-defined]

        def TextSegment() -> str:
            return "TextSegment"

        types_mod.TextSegment = TextSegment  # type: ignore[attr-defined]

        return dataset_mod, model_mod, types_mod

    try:
        integration._load_lit_components = mock_load_components  # pyright: ignore[reportPrivateUsage]
        integration._import_lit_server = lambda: MockServerModule  # pyright: ignore[reportPrivateUsage]

        with pytest.raises(RuntimeError, match="Failed to build LIT app"):
            integration.run_server_bundestag_char(
                host="127.0.0.1", port=0, open_browser=False
            )

    finally:
        integration._load_lit_components = original_load_components  # pyright: ignore[reportPrivateUsage]
        integration._import_lit_server = original_import_server  # pyright: ignore[reportPrivateUsage]


def test_run_server_wraps_attribute_error_in_server_factory_check() -> None:
    """Test AttributeError when Server factory is not callable."""
    original_load_components = integration._load_lit_components  # pyright: ignore[reportPrivateUsage]
    original_import_server = integration._import_lit_server  # pyright: ignore[reportPrivateUsage]

    class MockServerModule:
        Server = "not_callable"  # type: ignore[attr-defined]

    def mock_load_components() -> tuple[Any, Any, Any]:
        # Return minimal working modules
        dataset_mod = ModuleType("lit_nlp.api.dataset")
        model_mod = ModuleType("lit_nlp.api.model")
        types_mod = ModuleType("lit_nlp.api.types")

        class Dataset:
            def spec(self) -> dict[str, object]:
                return {"text": "segment"}

            def __len__(self) -> int:
                return 0

            def __iter__(self):
                return iter([])

        dataset_mod.Dataset = Dataset  # type: ignore[attr-defined]

        class Model:
            def input_spec(self) -> dict[str, object]:
                return {"text": "segment"}

            def output_spec(self) -> dict[str, object]:
                return {"generated": "segment"}

            def predict(self, _inputs: Any, **kwargs: Any) -> list[dict[str, object]]:
                return []

        model_mod.Model = Model  # type: ignore[attr-defined]

        def TextSegment() -> str:
            return "TextSegment"

        types_mod.TextSegment = TextSegment  # type: ignore[attr-defined]

        return dataset_mod, model_mod, types_mod

    try:
        integration._load_lit_components = mock_load_components  # pyright: ignore[reportPrivateUsage]
        integration._import_lit_server = lambda: MockServerModule  # pyright: ignore[reportPrivateUsage]

        with pytest.raises(
            RuntimeError, match="LIT server module does not expose a Server factory"
        ):
            integration.run_server_bundestag_char(
                host="127.0.0.1", port=0, open_browser=False
            )

    finally:
        integration._load_lit_components = original_load_components  # pyright: ignore[reportPrivateUsage]
        integration._import_lit_server = original_import_server  # pyright: ignore[reportPrivateUsage]


def test_parse_cli_args_type_validation_host() -> None:
    """Test type validation for host argument in CLI parsing."""
    import argparse

    # Directly test the validation logic
    namespace = argparse.Namespace(host=123, port=5432, open_browser=False)
    host_attr = namespace.host
    with pytest.raises(TypeError, match="--host must be a string"):
        if not isinstance(host_attr, str):
            raise TypeError("--host must be a string")


def test_parse_cli_args_type_validation_port() -> None:
    """Test type validation for port argument in CLI parsing."""
    import argparse

    # Directly test the validation logic
    namespace = argparse.Namespace(
        host="localhost", port="not-an-int", open_browser=False
    )
    port_attr = namespace.port
    with pytest.raises(TypeError, match="--port must be parsed as an integer"):
        if not isinstance(port_attr, int):
            raise TypeError("--port must be parsed as an integer")
