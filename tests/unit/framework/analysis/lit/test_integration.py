from __future__ import annotations

from pathlib import Path
from types import ModuleType, SimpleNamespace
from typing import Mapping, cast

from ml_playground.framework.analysis.lit import integration


class _Logger:
    def debug(self, msg: object, *args: object, **kwargs: object) -> None:
        del msg, args, kwargs

    def info(self, msg: object, *args: object, **kwargs: object) -> None:
        del msg, args, kwargs

    def warning(self, msg: object, *args: object, **kwargs: object) -> None:
        del msg, args, kwargs

    def error(self, msg: object, *args: object, **kwargs: object) -> None:
        del msg, args, kwargs


def test_read_input_samples_falls_back_to_read_text() -> None:
    input_path = Path("/tmp/input.txt")

    def _open(*args: object, **kwargs: object) -> object:
        del args, kwargs
        raise OSError("open failed")

    def _read_text(
        self: Path,
        *,
        encoding: str | None = None,
        errors: str | None = None,
    ) -> str:
        del encoding, errors
        if self == input_path:
            return "first\n\nsecond\nthird\n"
        return ""

    original_open = Path.open
    original_read_text = Path.read_text
    try:
        Path.open = _open  # type: ignore[assignment]
        Path.read_text = _read_text  # type: ignore[assignment]
        samples = integration._read_input_samples(input_path, limit=2)
    finally:
        Path.open = original_open  # type: ignore[assignment]
        Path.read_text = original_read_text  # type: ignore[assignment]
    assert samples == ["first", "second"]


def test_run_server_bundestag_char_uses_overrides_and_parent_fallback() -> None:
    calls: dict[str, int] = {"loader": 0, "server": 0}
    serve_state: dict[str, object] = {}

    dataset_module = ModuleType("lit_nlp.api.dataset")

    class Dataset:
        pass

    dataset_module.Dataset = Dataset  # type: ignore[attr-defined]

    model_module = ModuleType("lit_nlp.api.model")

    class Model:
        pass

    model_module.Model = Model  # type: ignore[attr-defined]

    types_module = ModuleType("lit_nlp.api.types")
    types_module.TextSegment = lambda: "text"  # type: ignore[attr-defined]

    class App:
        def __init__(
            self, models: Mapping[str, object], datasets: Mapping[str, object]
        ) -> None:
            del models, datasets

        def serve(self, *, port: int, host: str, open_browser: bool) -> None:
            serve_state["args"] = (port, host, open_browser)

    server_module = ModuleType("lit_nlp.server")
    server_module.Server = lambda models, datasets: App(models, datasets)  # type: ignore[attr-defined]

    def _loader() -> tuple[
        integration.LitDatasetModule,
        integration.LitModelModule,
        integration.LitTypesModule,
    ]:
        calls["loader"] += 1
        return (
            cast(integration.LitDatasetModule, dataset_module),
            cast(integration.LitModelModule, model_module),
            cast(integration.LitTypesModule, types_module),
        )

    def _server_importer() -> ModuleType:
        calls["server"] += 1
        return server_module

    original_default_host = integration.get_default_host
    try:
        integration.get_default_host = lambda: "127.0.0.1"  # type: ignore[assignment]
        resolved = SimpleNamespace(parents=[Path("/x"), Path("/y"), Path("/z")])
        integration.run_server_bundestag_char(
            host=None,
            port=0,
            open_browser=False,
            logger=_Logger(),
            _loader_override=_loader,
            _server_importer_override=_server_importer,
            _path_resolver_override=lambda _path: resolved,  # type: ignore[return-value]
        )
    finally:
        integration.get_default_host = original_default_host  # type: ignore[assignment]

    assert calls == {"loader": 1, "server": 1}
    assert serve_state["args"] == (0, "127.0.0.1", False)
