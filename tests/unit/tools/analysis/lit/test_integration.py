# pyright: reportPrivateUsage=false
"""Tests for `ml_playground.analysis.lit.integration` utilities."""

from __future__ import annotations

import sys
from contextlib import contextmanager
from types import ModuleType
from pathlib import Path
from typing import Dict, Iterable, Iterator, Mapping, Optional, cast
from _pytest._code.code import ExceptionInfo
import pytest

from ml_playground.tools.analysis.lit import integration


@contextmanager
def override_attr(obj: object, name: str, value: object) -> Iterator[None]:
    original = getattr(obj, name)
    setattr(obj, name, value)
    try:
        yield
    finally:
        setattr(obj, name, original)


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


def test_protocol_placeholders_have_expected_shape() -> None:
    """Concrete stubs should satisfy the structural protocol contracts."""

    class DummyDataset(integration.LitDataset):  # type: ignore[misc]
        def __init__(self) -> None:
            self.examples: list[dict[str, object]] = [{"text": "hi"}]

        def spec(self) -> dict[str, object]:
            return {"text": "segment"}

        def __len__(self) -> int:
            return len(self.examples)

        def __iter__(self) -> Iterator[Mapping[str, object]]:
            return iter(self.examples)

    class DummyModel(integration.LitModel):  # type: ignore[misc]
        def input_spec(self) -> dict[str, object]:
            return {"text": "segment"}

        def output_spec(self) -> dict[str, object]:
            return {"generated": "segment"}

        def predict(
            self, _inputs: Iterable[Mapping[str, object]], **_kwargs: object
        ) -> list[Mapping[str, object]]:
            return [{"generated": "ok"}]

    class DummyTypes(integration.LitTypesModule):  # type: ignore[misc]
        def TextSegment(self) -> object:  # noqa: N802
            return "segment"

    class DummyApp(integration.LitApp):  # type: ignore[misc]
        def serve(self, *, port: int, host: str, open_browser: bool) -> None:
            self.called = (port, host, open_browser)

    class DummyServerModule(integration.LitServerModule):  # type: ignore[misc]
        def serve(
            self, app: object, *, port: int, host: str, open_browser: bool
        ) -> None:
            cast(integration.LitApp, app).serve(
                port=port, host=host, open_browser=open_browser
            )

    class DummyServerFactory(integration.LitServerFactory):  # type: ignore[misc]
        def __call__(
            self,
            models: Mapping[str, integration.LitModel],
            datasets: Mapping[str, integration.LitDataset],
        ):
            return {"models": models, "datasets": datasets}

    ds = DummyDataset()
    model = DummyModel()
    types = DummyTypes()
    app = DummyApp()
    server_module = DummyServerModule()
    factory = DummyServerFactory()

    assert ds.spec()["text"] == "segment"
    assert len(ds) == 1
    assert list(ds)[0]["text"] == "hi"

    assert "text" in model.input_spec()
    assert "generated" in model.output_spec()
    assert model.predict([{"text": "x"}])[0]["generated"] == "ok"

    assert types.TextSegment() == "segment"

    app.serve(port=0, host="127.0.0.1", open_browser=False)
    assert getattr(app, "called") == (0, "127.0.0.1", False)

    server_module.serve(app, port=1, host="0.0.0.0", open_browser=True)
    assert getattr(app, "called") == (1, "0.0.0.0", True)

    built = factory({"m": model}, {"d": ds})
    assert built["models"]["m"] is model
    assert built["datasets"]["d"] is ds


def test_load_lit_components_requires_lit_dependencies() -> None:
    """Import failures should explain how to install lit dependencies."""

    def failing_import(name: str, package: str | None = None) -> ModuleType:
        raise ImportError(f"missing {name}")

    with override_attr(integration.importlib, "import_module", failing_import):
        with pytest.raises(
            RuntimeError, match="LIT dependencies are unavailable"
        ) as exc_info:
            integration._load_lit_components()
        exc_info = cast(ExceptionInfo[RuntimeError], exc_info)
        assert "uv sync --extra lit" in str(exc_info.value)


def test_run_server_bundestag_char_wraps_runtime_error() -> None:
    def boom() -> tuple[object, object, object]:
        raise RuntimeError("boom")

    with override_attr(integration, "_load_lit_components", boom):
        with pytest.raises(RuntimeError, match="LIT is not available or incompatible"):
            integration.run_server_bundestag_char()


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
        ]:
            sys.modules.pop(name, None)

        with pytest.raises(RuntimeError) as exc_info:
            integration._import_lit_server()

        exc_info = cast(ExceptionInfo[RuntimeError], exc_info)
        message = str(exc_info.value)
        assert "Unable to import LIT server module" in message
        assert "9.9.9" in message
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
            host="0.0.0.0", port=1234, open_browser=True
        )
        assert ServerApp.instances, "Server factory should be invoked"
        app = ServerApp.instances[-1]
        assert "echo_model" in app.models
    finally:
        _restore_modules(originals)


def test_run_server_bundestag_char_uses_module_level_serve(tmp_path: Path) -> None:
    """Fallback to module-level serve should start the server when app lacks serve."""

    dataset_module = ModuleType("lit_nlp.api.dataset")
    model_module = ModuleType("lit_nlp.api.model")
    types_module = ModuleType("lit_nlp.api.types")

    class DummyDatasetBase: ...

    class DummyModelBase: ...

    def text_segment() -> str:
        return "segment"

    dataset_module.Dataset = DummyDatasetBase  # type: ignore[attr-defined]
    model_module.Model = DummyModelBase  # type: ignore[attr-defined]
    types_module.TextSegment = text_segment  # type: ignore[attr-defined]

    api_module = ModuleType("lit_nlp.api")
    server_module = ModuleType("lit_nlp.server")

    class ServerApp:
        def __init__(
            self, models: Mapping[str, object], datasets: Mapping[str, object]
        ) -> None:
            self.models = models
            self.datasets = datasets

    served_calls: list[tuple[object, int, str, bool]] = []

    class ServerFactory:
        def __call__(
            self, models: Mapping[str, object], datasets: Mapping[str, object]
        ) -> ServerApp:
            return ServerApp(models, datasets)

    def module_level_serve(
        app: object, *, port: int, host: str, open_browser: bool
    ) -> None:
        served_calls.append((app, port, host, open_browser))

    server_module.Server = ServerFactory()  # type: ignore[attr-defined]
    server_module.serve = module_level_serve  # type: ignore[attr-defined]

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
            host="127.0.0.1", port=0, open_browser=False
        )
        assert served_calls, "module-level serve should be invoked"
        _, served_port, served_host, served_browser = served_calls[0]
        assert served_port == 0
        assert served_host == "127.0.0.1"
        assert served_browser is False
    finally:
        _restore_modules(originals)


def test_parse_cli_args_parses_values() -> None:
    """CLI argument parser should return the expected tuple of values."""

    host, port, open_browser = integration._parse_cli_args(
        ["--host", "0.0.0.0", "--port", "0", "--open-browser"]
    )

    assert host == "0.0.0.0"
    assert port == 0
    assert open_browser is True


def test_parse_cli_args_rejects_non_int_port() -> None:
    import argparse

    original_parse = argparse.ArgumentParser.parse_args

    def fake_parse_args(  # type: ignore[override]
        self: argparse.ArgumentParser, _argv: object | None = None
    ):
        return argparse.Namespace(host="127.0.0.1", port="nope", open_browser=False)

    try:
        argparse.ArgumentParser.parse_args = fake_parse_args  # type: ignore[assignment]
        with pytest.raises(TypeError, match="--port must be parsed as an integer"):
            integration._parse_cli_args([])
    finally:
        argparse.ArgumentParser.parse_args = original_parse  # type: ignore[assignment]


def test_parse_cli_args_open_browser_non_bool_is_cast() -> None:
    import argparse

    original_parse = argparse.ArgumentParser.parse_args

    def fake_parse_args(  # type: ignore[override]
        self: argparse.ArgumentParser, _argv: object | None = None
    ):
        return argparse.Namespace(host="127.0.0.1", port=1234, open_browser="yes")

    try:
        argparse.ArgumentParser.parse_args = fake_parse_args  # type: ignore[assignment]
        host, port, open_browser = integration._parse_cli_args([])
    finally:
        argparse.ArgumentParser.parse_args = original_parse  # type: ignore[assignment]

    assert host == "127.0.0.1"
    assert port == 1234
    assert open_browser is True


def test_run_server_bundestag_char_optional_input_file_empty_lines() -> None:
    dataset_module = ModuleType("lit_nlp.api.dataset")
    model_module = ModuleType("lit_nlp.api.model")
    types_module = ModuleType("lit_nlp.api.types")

    class DummyDatasetBase: ...

    class DummyModelBase: ...

    def text_segment() -> str:
        return "segment"

    dataset_module.Dataset = DummyDatasetBase  # type: ignore[attr-defined]
    model_module.Model = DummyModelBase  # type: ignore[attr-defined]
    types_module.TextSegment = text_segment  # type: ignore[attr-defined]

    captured: dict[str, object] = {}

    class ServerApp:
        def __init__(
            self, _models: Mapping[str, object], datasets: Mapping[str, object]
        ):
            captured["datasets"] = datasets

        def serve(self, *, port: int, host: str, open_browser: bool) -> None:  # noqa: ARG002
            return

    server_module = ModuleType("lit_nlp.server")
    server_module.Server = ServerApp  # type: ignore[attr-defined]

    def fake_load() -> tuple[object, object, object]:
        return dataset_module, model_module, types_module

    def fake_import_server() -> ModuleType:
        return server_module

    original_exists = integration.Path.exists
    original_read_text = integration.Path.read_text

    def fake_exists(self: Path) -> bool:
        if str(self).endswith("tools/experiments/bundestag_char/datasets/input.txt"):
            return True
        return original_exists(self)

    def fake_read_text(self: Path, *args: object, **kwargs: object) -> str:
        if str(self).endswith("tools/experiments/bundestag_char/datasets/input.txt"):
            return "\n\n"
        return original_read_text(self, *args, **kwargs)  # type: ignore[arg-type]

    with override_attr(integration, "_load_lit_components", fake_load):
        with override_attr(integration, "_import_lit_server", fake_import_server):
            with override_attr(integration.Path, "exists", fake_exists):
                with override_attr(integration.Path, "read_text", fake_read_text):
                    integration.run_server_bundestag_char(host="127.0.0.1", port=0)

    datasets = captured["datasets"]
    dataset = cast(Mapping[str, object], datasets)["bundestag_char_sample"]
    examples = cast(list[Mapping[str, object]], getattr(dataset, "_examples"))
    assert len(examples) >= 1
    assert "Nächste Rednerin" in str(examples[0]["text"])


def test_run_server_bundestag_char_optional_input_file_non_empty_lines() -> None:
    dataset_module = ModuleType("lit_nlp.api.dataset")
    model_module = ModuleType("lit_nlp.api.model")
    types_module = ModuleType("lit_nlp.api.types")

    class DummyDatasetBase: ...

    class DummyModelBase: ...

    def text_segment() -> str:
        return "segment"

    dataset_module.Dataset = DummyDatasetBase  # type: ignore[attr-defined]
    model_module.Model = DummyModelBase  # type: ignore[attr-defined]
    types_module.TextSegment = text_segment  # type: ignore[attr-defined]

    captured: dict[str, object] = {}

    class ServerApp:
        def __init__(
            self, _models: Mapping[str, object], datasets: Mapping[str, object]
        ):
            captured["datasets"] = datasets

        def serve(self, *, port: int, host: str, open_browser: bool) -> None:  # noqa: ARG002
            return

    server_module = ModuleType("lit_nlp.server")
    server_module.Server = ServerApp  # type: ignore[attr-defined]

    def fake_load() -> tuple[object, object, object]:
        return dataset_module, model_module, types_module

    def fake_import_server() -> ModuleType:
        return server_module

    original_exists = integration.Path.exists
    original_read_text = integration.Path.read_text

    def fake_exists(self: Path) -> bool:
        if str(self).endswith("tools/experiments/bundestag_char/datasets/input.txt"):
            return True
        return original_exists(self)

    def fake_read_text(self: Path, *args: object, **kwargs: object) -> str:
        if str(self).endswith("tools/experiments/bundestag_char/datasets/input.txt"):
            return "first\nsecond\nthird\n"
        return original_read_text(self, *args, **kwargs)  # type: ignore[arg-type]

    with override_attr(integration, "_load_lit_components", fake_load):
        with override_attr(integration, "_import_lit_server", fake_import_server):
            with override_attr(integration.Path, "exists", fake_exists):
                with override_attr(integration.Path, "read_text", fake_read_text):
                    integration.run_server_bundestag_char(host="127.0.0.1", port=0)

    datasets = captured["datasets"]
    dataset = cast(Mapping[str, object], datasets)["bundestag_char_sample"]
    examples = cast(list[Mapping[str, object]], getattr(dataset, "_examples"))
    assert len(examples) == 3
    assert examples[0]["text"] == "first"


def test_run_server_bundestag_char_optional_input_file_read_error() -> None:
    dataset_module = ModuleType("lit_nlp.api.dataset")
    model_module = ModuleType("lit_nlp.api.model")
    types_module = ModuleType("lit_nlp.api.types")

    class DummyDatasetBase: ...

    class DummyModelBase: ...

    def text_segment() -> str:
        return "segment"

    dataset_module.Dataset = DummyDatasetBase  # type: ignore[attr-defined]
    model_module.Model = DummyModelBase  # type: ignore[attr-defined]
    types_module.TextSegment = text_segment  # type: ignore[attr-defined]

    captured: dict[str, object] = {}

    class ServerApp:
        def __init__(
            self, _models: Mapping[str, object], datasets: Mapping[str, object]
        ):
            captured["datasets"] = datasets

        def serve(self, *, port: int, host: str, open_browser: bool) -> None:  # noqa: ARG002
            return

    server_module = ModuleType("lit_nlp.server")
    server_module.Server = ServerApp  # type: ignore[attr-defined]

    def fake_load() -> tuple[object, object, object]:
        return dataset_module, model_module, types_module

    def fake_import_server() -> ModuleType:
        return server_module

    original_exists = integration.Path.exists

    def fake_exists(self: Path) -> bool:
        if str(self).endswith("tools/experiments/bundestag_char/datasets/input.txt"):
            return True
        return original_exists(self)

    def fake_read_text(self: Path, *args: object, **kwargs: object) -> str:
        if str(self).endswith("tools/experiments/bundestag_char/datasets/input.txt"):
            raise UnicodeError("boom")
        return ""

    with override_attr(integration, "_load_lit_components", fake_load):
        with override_attr(integration, "_import_lit_server", fake_import_server):
            with override_attr(integration.Path, "exists", fake_exists):
                with override_attr(integration.Path, "read_text", fake_read_text):
                    integration.run_server_bundestag_char(host="127.0.0.1", port=0)

    datasets = captured["datasets"]
    dataset = cast(Mapping[str, object], datasets)["bundestag_char_sample"]
    examples = cast(list[Mapping[str, object]], getattr(dataset, "_examples"))
    assert len(examples) >= 1
    assert "Nächste Rednerin" in str(examples[0]["text"])
