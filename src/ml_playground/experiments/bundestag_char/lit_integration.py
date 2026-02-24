from __future__ import annotations

import logging
import sys
from collections.abc import Callable, Iterable, Mapping
from pathlib import Path
from types import ModuleType
from typing import Protocol

from ml_playground.framework.runtime import protocols
from ml_playground.framework.core.logging_protocol import LoggerLike
from ml_playground.framework.core.project_config import get_default_host
from ml_playground.framework.core.protocols import (
    LitDataset,
    LitDatasetModule,
    LitModel,
    LitModelModule,
    LitTypesModule,
)


@protocols.runtime_checkable
class LitServerFactory(Protocol):
    """Protocol for the LIT server factory."""

    def __call__(
        self,
        models: dict[str, LitModel],
        datasets: dict[str, LitDataset],
    ) -> object: ...


def run_server_bundestag_char(
    host: str | None = None,
    port: int = 5432,
    open_browser: bool = False,
    logger: LoggerLike | None = None,
    _loader_override: Callable[
        [], tuple[LitDatasetModule, LitModelModule, LitTypesModule]
    ]
    | None = None,
    _server_importer_override: Callable[[], ModuleType] | None = None,
    _path_resolver_override: Callable[[Path], Path] | None = None,
    _get_default_host_override: Callable[[], str | None] | None = None,
) -> None:
    """Launch a minimal LIT server for the bundestag_char PoC."""
    if host is None:
        host = (
            _get_default_host_override() if _get_default_host_override else None
        ) or get_default_host()

    if logger is None:
        logger = logging.getLogger(__name__)

    if _loader_override:
        dataset_mod, model_mod, types_mod = _loader_override()
    else:
        from ml_playground.framework.analysis.lit.integration import load_lit_components

        dataset_mod, model_mod, types_mod = load_lit_components()

    if _server_importer_override:
        server_module = _server_importer_override()
    else:
        from ml_playground.framework.analysis.lit.integration import import_lit_server

        server_module = import_lit_server()

    dataset_base = dataset_mod.Dataset
    model_base = model_mod.Model
    text_segment_factory = types_mod.TextSegment

    samples: list[str] = [
        "Nächste Rednerin ist die Vorsitzende der AfD-Fraktion, Dr. Alice Weidel.",
        "Herr Präsident, liebe Kolleginnen und Kollegen, wir beraten heute wichtige Vorlagen.",
        "(Beifall bei der SPD)",
        "Die Bundesregierung handelt entschlossen.",
        "Applaus bei der CDU/CSU.",
        "Vielen Dank. — Zur Geschäftsordnung hat der Abgeordnete das Wort.",
        "Wir müssen die Inflation bekämpfen und Familien entlasten.",
        "Das Wort hat nun die Bundeskanzlerin.",
        "Meine Damen und Herren, die Lage ist ernst, aber beherrschbar.",
        "(Heiterkeit) Der nächste Redner folgt.",
    ]

    try:
        if _path_resolver_override:
            resolved = _path_resolver_override(Path(__file__))
        else:
            resolved = Path(__file__).resolve()
        exp_dir = resolved.parent
        input_path = exp_dir / "datasets" / "input.txt"
        if input_path.exists():
            file_lines: list[str] = []
            with input_path.open("r", encoding="utf-8", errors="ignore") as f:
                for line in f:
                    ln = line.strip()
                    if ln:
                        file_lines.append(ln)
                    if len(file_lines) >= 10:
                        break
            if file_lines:
                samples = file_lines
    except (OSError, UnicodeError):
        pass

    class BundestagTextDataset(dataset_base):  # type: ignore[valid-type, misc]
        def __init__(self, sents: Iterable[str]):
            self._examples: list[Mapping[str, str]] = [{"text": s} for s in sents]

        def spec(self) -> dict[str, object]:
            return {"text": text_segment_factory()}

        def __len__(self) -> int:
            return len(self._examples)

        def __iter__(self):
            return iter(self._examples)

    class EchoModel(model_base):  # type: ignore[valid-type, misc]
        def input_spec(self) -> dict[str, object]:
            return {"text": text_segment_factory()}

        def output_spec(self) -> dict[str, object]:
            return {"generated": text_segment_factory()}

        def predict(
            self, _inputs: Iterable[Mapping[str, object]], **kwargs: object
        ) -> list[Mapping[str, object]]:
            outs: list[Mapping[str, object]] = []
            for ex in _inputs:
                s = str(ex.get("text", ""))
                outs.append({"generated": s + "\n\n[echo] " + s[::-1]})
            return outs

    datasets: dict[str, LitDataset] = {
        "bundestag_char_sample": BundestagTextDataset(samples)
    }
    models: dict[str, LitModel] = {"echo_model": EchoModel()}

    try:
        server_factory_obj = getattr(server_module, "Server", None)
        if not callable(server_factory_obj):
            raise RuntimeError("LIT server module does not expose a Server factory")
        app = server_factory_obj(models, datasets)
    except (
        TypeError,
        AttributeError,
        RuntimeError,
        ValueError,
    ) as exc:
        raise RuntimeError(f"Failed to build LIT app: {exc}") from exc

    url = f"http://{host}:{port if port else '<auto>'}"
    logger.info(f"Registered models: {', '.join(models.keys())}")
    logger.info(f"Registered datasets: {', '.join(datasets.keys())}")
    logger.info(f"Starting server at {url}")
    _ = sys.stdout.flush()

    serve_method = getattr(app, "serve", None)
    use_module_serve = False
    if not callable(serve_method):
        serve_method = getattr(server_module, "serve", None)
        use_module_serve = True
    if not callable(serve_method):
        raise RuntimeError("LIT server app does not expose a serve(...) method.")

    serve_kwargs = {"port": port, "host": host, "open_browser": open_browser}
    try:
        if use_module_serve:
            _ = serve_method(app, **serve_kwargs)
        else:
            _ = serve_method(**serve_kwargs)
    except TypeError as exc:
        raise RuntimeError(
            "LIT server serve() must accept keyword arguments: port, host, open_browser."
        ) from exc


def run_server(
    *,
    host: str | None = None,
    port: int = 5432,
    open_browser: bool = False,
    logger: LoggerLike | None = None,
    _loader_override: Callable[
        [], tuple[LitDatasetModule, LitModelModule, LitTypesModule]
    ]
    | None = None,
    _server_importer_override: Callable[[], ModuleType] | None = None,
    _path_resolver_override: Callable[[Path], Path] | None = None,
    _get_default_host_override: Callable[[], str | None] | None = None,
) -> None:
    """Preferred generic experiment LIT entrypoint."""
    run_server_bundestag_char(
        host=host,
        port=port,
        open_browser=open_browser,
        logger=logger,
        _loader_override=_loader_override,
        _server_importer_override=_server_importer_override,
        _path_resolver_override=_path_resolver_override,
        _get_default_host_override=_get_default_host_override,
    )
