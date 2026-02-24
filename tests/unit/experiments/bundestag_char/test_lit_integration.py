# pyright: reportPrivateUsage=false
"""Tests for `ml_playground.experiments.bundestag_char.lit_integration`."""

from __future__ import annotations

import sys
from collections.abc import Mapping
from pathlib import Path
from types import ModuleType, SimpleNamespace
from typing import Any, Dict, Optional, cast

import pytest

from ml_playground.experiments.bundestag_char import lit_integration
from ml_playground.framework.core.protocols import (
    LitDatasetModule,
    LitModelModule,
    LitTypesModule,
)


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


def test_run_server_bundestag_char_invokes_server_factory() -> None:
    """`run_server_bundestag_char` should build datasets, models, and start the server."""
    dataset_module = ModuleType("lit_nlp.api.dataset")
    model_module = ModuleType("lit_nlp.api.model")
    types_module = ModuleType("lit_nlp.api.types")

    class StubDatasetBase:  # noqa: D401 - simple stub
        """Base dataset stub."""

    class StubModelBase:  # noqa: D401 - simple stub
        """Base model stub."""

    def text_segment() -> str:
        return "segment"

    dataset_module.Dataset = StubDatasetBase  # type: ignore[attr-defined]
    model_module.Model = StubModelBase  # type: ignore[attr-defined]
    types_module.TextSegment = text_segment  # type: ignore[attr-defined]

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

    def stub_loader():
        return (
            cast(LitDatasetModule, dataset_module),
            cast(LitModelModule, model_module),
            cast(LitTypesModule, types_module),
        )

    def stub_server_importer():
        return server_module

    try:
        import logging

        logger = logging.getLogger("test_logger")
        lit_integration.run_server_bundestag_char(
            host="0.0.0.0",
            port=1234,
            open_browser=True,
            logger=logger,
            _loader_override=stub_loader,
            _server_importer_override=stub_server_importer,
            _get_default_host_override=lambda: "127.0.0.1",
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
        ServerApp.instances.clear()


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

    # Create a real file in tmp_path
    exp_dir = tmp_path / "experiments" / "bundestag_char"
    datasets_dir = exp_dir / "datasets"
    datasets_dir.mkdir(parents=True)
    input_file = datasets_dir / "input.txt"
    input_file.write_text("Line 1\nLine 2\n")

    # Stub path resolver returns the exp integration path, used to find datasets/
    def stub_path_resolver(p: Path) -> Path:
        return exp_dir / "lit_integration.py"

    lit_integration.run_server_bundestag_char(
        _loader_override=lambda: (
            cast(LitDatasetModule, dataset_module),
            cast(LitModelModule, model_module),
            cast(LitTypesModule, types_module),
        ),
        _server_importer_override=lambda: server_module,
        _path_resolver_override=stub_path_resolver,
        _get_default_host_override=lambda: "127.0.0.1",
    )


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

    # Create an empty file
    exp_dir = tmp_path / "experiments" / "bundestag_char"
    datasets_dir = exp_dir / "datasets"
    datasets_dir.mkdir(parents=True)
    input_file = datasets_dir / "input.txt"
    input_file.write_text("\n\n")  # only whitespace -> file_lines will be empty

    def stub_path_resolver(p: Path) -> Path:
        return exp_dir / "lit_integration.py"

    lit_integration.run_server_bundestag_char(
        _loader_override=lambda: (
            cast(LitDatasetModule, dataset_module),
            cast(LitModelModule, model_module),
            cast(LitTypesModule, types_module),
        ),
        _server_importer_override=lambda: server_module,
        _path_resolver_override=stub_path_resolver,
        _get_default_host_override=lambda: "127.0.0.1",
    )


class StubPath(type(Path())):
    """A Path subclass that can stub existence and opening."""

    _exists_stub: bool = True
    _content_stub: str = ""
    _throw_on_open: Optional[Exception] = None

    def exists(self) -> bool:
        return self._exists_stub

    def open(self, *args: Any, **kwargs: Any) -> Any:
        if self._throw_on_open:
            raise self._throw_on_open
        from io import StringIO

        return StringIO(self._content_stub)


def test_run_server_bundestag_char_unicode_error_handling(tmp_path: Path) -> None:
    """Test handling of UnicodeError when reading input.txt."""
    dataset_module = ModuleType("lit_nlp.api.dataset")
    model_module = ModuleType("lit_nlp.api.model")
    types_module = ModuleType("lit_nlp.api.types")
    dataset_module.Dataset = object  # type: ignore
    model_module.Model = object  # type: ignore
    types_module.TextSegment = lambda: None  # type: ignore
    server_module = ModuleType("lit_nlp.server")
    server_module.Server = lambda m, d: SimpleNamespace(serve=lambda **_kw: None)  # type: ignore

    exp_dir = tmp_path / "experiments" / "bundestag_char"

    # We use our StubPath via the path resolver override
    def stub_path_resolver(p: Path) -> Path:
        sp = StubPath(exp_dir / "lit_integration.py")
        sp._exists_stub = True
        sp._throw_on_open = UnicodeError("boom")
        return sp

    lit_integration.run_server_bundestag_char(
        _loader_override=lambda: (
            cast(LitDatasetModule, dataset_module),
            cast(LitModelModule, model_module),
            cast(LitTypesModule, types_module),
        ),
        _server_importer_override=lambda: server_module,
        _path_resolver_override=stub_path_resolver,
        _get_default_host_override=lambda: "127.0.0.1",
    )


def test_run_server_bundestag_char_os_error_handling(tmp_path: Path) -> None:
    dataset_module = ModuleType("lit_nlp.api.dataset")
    model_module = ModuleType("lit_nlp.api.model")
    types_module = ModuleType("lit_nlp.api.types")
    dataset_module.Dataset = object  # type: ignore
    model_module.Model = object  # type: ignore
    types_module.TextSegment = lambda: None  # type: ignore
    server_module = ModuleType("lit_nlp.server")
    server_module.Server = lambda m, d: SimpleNamespace(serve=lambda **_kw: None)  # type: ignore

    exp_dir = tmp_path / "experiments" / "bundestag_char"

    def stub_path_resolver(p: Path) -> Path:
        sp = StubPath(exp_dir / "lit_integration.py")
        sp._exists_stub = True
        sp._throw_on_open = OSError("boom")
        return sp

    lit_integration.run_server_bundestag_char(
        _loader_override=lambda: (
            cast(LitDatasetModule, dataset_module),
            cast(LitModelModule, model_module),
            cast(LitTypesModule, types_module),
        ),
        _server_importer_override=lambda: server_module,
        _path_resolver_override=stub_path_resolver,
        _get_default_host_override=lambda: "127.0.0.1",
    )


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

    with pytest.raises(RuntimeError, match="LIT server app does not expose a serve"):
        lit_integration.run_server_bundestag_char(
            _loader_override=lambda: (
                cast(LitDatasetModule, dataset_module),
                cast(LitModelModule, model_module),
                cast(LitTypesModule, types_module),
            ),
            _server_importer_override=lambda: server_module,
            _get_default_host_override=lambda: "127.0.0.1",
        )


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

    with pytest.raises(
        RuntimeError, match="LIT server module does not expose a Server factory"
    ):
        lit_integration.run_server_bundestag_char(
            _loader_override=lambda: (
                cast(LitDatasetModule, dataset_module),
                cast(LitModelModule, model_module),
                cast(LitTypesModule, types_module),
            ),
            _server_importer_override=lambda: server_module,
            _get_default_host_override=lambda: "127.0.0.1",
        )


def test_run_server_bundestag_char_wraps_app_construction_errors() -> None:
    dataset_module = ModuleType("lit_nlp.api.dataset")
    model_module = ModuleType("lit_nlp.api.model")
    types_module = ModuleType("lit_nlp.api.types")
    dataset_module.Dataset = object  # type: ignore
    model_module.Model = object  # type: ignore
    types_module.TextSegment = lambda: None  # type: ignore
    server_module = ModuleType("lit_nlp.server")

    def bad_server(*_args: object, **_kwargs: object) -> object:
        raise ValueError("broken server factory")

    server_module.Server = bad_server  # type: ignore[attr-defined]

    with pytest.raises(RuntimeError, match="Failed to build LIT app"):
        lit_integration.run_server_bundestag_char(
            _loader_override=lambda: (
                cast(LitDatasetModule, dataset_module),
                cast(LitModelModule, model_module),
                cast(LitTypesModule, types_module),
            ),
            _server_importer_override=lambda: server_module,
            _get_default_host_override=lambda: "127.0.0.1",
        )


def test_run_server_bundestag_char_wraps_serve_type_error() -> None:
    dataset_module = ModuleType("lit_nlp.api.dataset")
    model_module = ModuleType("lit_nlp.api.model")
    types_module = ModuleType("lit_nlp.api.types")
    dataset_module.Dataset = object  # type: ignore
    model_module.Model = object  # type: ignore
    types_module.TextSegment = lambda: None  # type: ignore
    server_module = ModuleType("lit_nlp.server")

    class App:
        def serve(self, *, port, host, open_browser):
            raise TypeError("wrong args")

    server_module.Server = lambda m, d: App()  # type: ignore

    with pytest.raises(
        RuntimeError, match=r"LIT server serve\(\) must accept keyword arguments"
    ):
        lit_integration.run_server_bundestag_char(
            _loader_override=lambda: (
                cast(LitDatasetModule, dataset_module),
                cast(LitModelModule, model_module),
                cast(LitTypesModule, types_module),
            ),
            _server_importer_override=lambda: server_module,
            _get_default_host_override=lambda: "127.0.0.1",
        )


def test_run_server_bundestag_char_uses_module_level_serve() -> None:
    dataset_module = ModuleType("lit_nlp.api.dataset")
    model_module = ModuleType("lit_nlp.api.model")
    types_module = ModuleType("lit_nlp.api.types")
    dataset_module.Dataset = object  # type: ignore
    model_module.Model = object  # type: ignore
    types_module.TextSegment = lambda: None  # type: ignore
    server_module = ModuleType("lit_nlp.server")

    class App:
        pass

    calls = []

    def module_serve(app, **kwargs):
        calls.append(kwargs)

    server_module.Server = lambda m, d: App()  # type: ignore
    server_module.serve = module_serve  # type: ignore

    lit_integration.run_server_bundestag_char(
        host="localhost",
        port=8888,
        open_browser=True,
        _loader_override=lambda: (
            cast(LitDatasetModule, dataset_module),
            cast(LitModelModule, model_module),
            cast(LitTypesModule, types_module),
        ),
        _server_importer_override=lambda: server_module,
        _get_default_host_override=lambda: "127.0.0.1",
    )
    assert calls[0]["port"] == 8888


def test_run_server_wrapper() -> None:
    """Test the run_server wrapper calls run_server_bundestag_char."""
    dataset_module = ModuleType("lit_nlp.api.dataset")
    model_module = ModuleType("lit_nlp.api.model")
    types_module = ModuleType("lit_nlp.api.types")
    dataset_module.Dataset = object  # type: ignore
    model_module.Model = object  # type: ignore
    types_module.TextSegment = lambda: None  # type: ignore
    server_module = ModuleType("lit_nlp.server")
    server_module.Server = lambda m, d: SimpleNamespace(serve=lambda **_kw: None)  # type: ignore

    lit_integration.run_server(
        _loader_override=lambda: (
            cast(LitDatasetModule, dataset_module),
            cast(LitModelModule, model_module),
            cast(LitTypesModule, types_module),
        ),
        _server_importer_override=lambda: server_module,
        _get_default_host_override=lambda: "127.0.0.1",
    )
