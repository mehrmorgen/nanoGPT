# pyright: reportPrivateUsage=false
"""Unit tests for `ml_playground.analysis.lit.integration`."""

from __future__ import annotations

import argparse
import sys
from contextlib import contextmanager
from types import ModuleType, SimpleNamespace
from typing import Any, Dict, Iterable, Iterator, Mapping, cast

import pytest

from ml_playground.analysis.lit import integration


def _make_fake_lit_modules() -> tuple[dict[str, ModuleType], dict[str, object]]:
    server_state: dict[str, object] = {}

    dataset_module = ModuleType("lit_nlp.api.dataset")

    class DatasetBase:
        def spec(
            self,
        ) -> dict[str, object]:  # pragma: no cover - overridden in subclass
            return {"text": "segment"}

        def __len__(self) -> int:  # pragma: no cover - overridden in subclass
            return 0

        def __iter__(self) -> Iterator[Mapping[str, object]]:  # pragma: no cover - overridden
            return iter(())

    dataset_module.Dataset = DatasetBase  # type: ignore[attr-defined]

    model_module = ModuleType("lit_nlp.api.model")

    class ModelBase:
        def input_spec(self) -> dict[str, object]:  # pragma: no cover - overridden
            return {"text": "segment"}

        def output_spec(self) -> dict[str, object]:  # pragma: no cover - overridden
            return {"generated": "segment"}

        def predict(  # pragma: no cover - overridden
            self,
            _inputs: Iterable[Mapping[str, object]],
            **_kwargs: object,
        ) -> list[Mapping[str, object]]:
            return []

    model_module.Model = ModelBase  # type: ignore[attr-defined]

    types_module = ModuleType("lit_nlp.api.types")

    def TextSegment() -> str:
        return "TextSegment"

    types_module.TextSegment = TextSegment  # type: ignore[attr-defined]

    lit_api_module = ModuleType("lit_nlp.api")

    server_module = ModuleType("lit_nlp.server")

    class FakeServer:
        def __init__(
            self, models: Mapping[str, object], datasets: Mapping[str, object]
        ) -> None:
            server_state["models"] = models
            server_state["datasets"] = datasets

        def serve(self, *, port: int, host: str, open_browser: bool) -> None:
            server_state["serve_kwargs"] = {
                "port": port,
                "host": host,
                "open_browser": open_browser,
            }

    server_module.Server = FakeServer  # type: ignore[attr-defined]
    server_module.state = server_state  # type: ignore[attr-defined]

    lit_module = ModuleType("lit_nlp")
    lit_module.__dict__["__version__"] = "9.9.9"

    modules = {
        "lit_nlp": lit_module,
        "lit_nlp.api": lit_api_module,
        "lit_nlp.api.dataset": dataset_module,
        "lit_nlp.api.model": model_module,
        "lit_nlp.api.types": types_module,
        "lit_nlp.server": server_module,
    }

    return modules, server_state


@contextmanager
def install_fake_lit_modules() -> Iterator[dict[str, object]]:
    modules, server_state = _make_fake_lit_modules()
    originals: Dict[str, ModuleType | None] = {}
    try:
        for name, module in modules.items():
            originals[name] = sys.modules.get(name)
            sys.modules[name] = module
        yield server_state
    finally:
        for name, original in originals.items():
            if original is None:
                sys.modules.pop(name, None)
            else:
                sys.modules[name] = original


def test_load_lit_components_uses_fake_modules() -> None:
    """Test load lit components uses fake modules."""
    with install_fake_lit_modules():
        dataset_mod_raw, model_mod_raw, types_mod_raw = integration._load_lit_components()

    dataset_mod = cast(ModuleType, dataset_mod_raw)
    model_mod = cast(ModuleType, model_mod_raw)
    types_mod = cast(ModuleType, types_mod_raw)

    assert dataset_mod.__name__ == "lit_nlp.api.dataset"
    assert hasattr(dataset_mod, "Dataset")
    assert model_mod.__name__ == "lit_nlp.api.model"
    assert hasattr(model_mod, "Model")
    assert types_mod.__name__ == "lit_nlp.api.types"
    assert callable(getattr(types_mod, "TextSegment"))


def test_import_lit_server_returns_fake_module() -> None:
    """Test import lit server returns fake module."""
    with install_fake_lit_modules():
        module = integration._import_lit_server()
    assert module.__name__ == "lit_nlp.server"
    assert hasattr(module, "Server")


def test_run_server_bundestag_char_invokes_server_factory() -> None:
    """Test run server bundestag char invokes server factory."""
    with install_fake_lit_modules() as server_state:
        integration.run_server_bundestag_char(
            host="0.0.0.0", port=1234, open_browser=True
        )

    assert server_state["serve_kwargs"] == {
        "port": 1234,
        "host": "0.0.0.0",
        "open_browser": True,
    }
    datasets = cast(Mapping[str, object], server_state["datasets"])
    models = cast(Mapping[str, object], server_state["models"])
    assert "bundestag_char_sample" in datasets
    assert "echo_model" in models


def test_parse_cli_args_returns_expected_defaults() -> None:
    """Test parse cli args returns expected defaults."""
    host, port, open_browser = integration._parse_cli_args([])

    assert host == "127.0.0.1"
    assert port == 5432
    assert open_browser is False


def test_parse_cli_args_accepts_overrides() -> None:
    """Test parse cli args accepts overrides."""
    host, port, open_browser = integration._parse_cli_args(
        ["--host", "0.0.0.0", "--port", "8080", "--open-browser"]
    )

    assert host == "0.0.0.0"
    assert port == 8080
    assert open_browser is True


def test_parse_cli_args_type_validation() -> None:
    """Test parse cli args type validation."""
    original = argparse.ArgumentParser.parse_args

    def fake_parse_args(  # type: ignore[override]
        self: argparse.ArgumentParser, _argv: Any | None = None
    ) -> SimpleNamespace:
        return SimpleNamespace(host=123, port="not-an-int", open_browser=False)

    try:
        argparse.ArgumentParser.parse_args = fake_parse_args  # type: ignore[assignment]
        with pytest.raises(TypeError):
            integration._parse_cli_args([])
    finally:
        argparse.ArgumentParser.parse_args = original  # type: ignore[assignment]
