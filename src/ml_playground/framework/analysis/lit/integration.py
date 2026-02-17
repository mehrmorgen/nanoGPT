from __future__ import annotations

import importlib
import logging
import sys
from collections.abc import Callable, Iterable, Mapping, Sequence
import argparse
from pathlib import Path
from types import ModuleType
from typing import Protocol, cast, override

from ml_playground.framework.core.di_implementations import DefaultModuleImporter
from ml_playground.framework.core.logging_protocol import LoggerLike
from ml_playground.framework.core.project_config import get_default_host

WSGIApp = Callable[..., Iterable[bytes]]


class LitDataset(Protocol):
    def spec(self) -> dict[str, object]: ...

    def __len__(self) -> int: ...

    def __iter__(self) -> Iterable[Mapping[str, object]]: ...


class LitDatasetModule(Protocol):
    Dataset: type[LitDataset]


class LitModel(Protocol):
    def input_spec(self) -> dict[str, object]: ...

    def output_spec(self) -> dict[str, object]: ...

    def predict(
        self, _inputs: Iterable[Mapping[str, object]], **kwargs: object
    ) -> list[Mapping[str, object]]: ...


class LitModelModule(Protocol):
    Model: type[LitModel]


class LitTypesModule(Protocol):
    def TextSegment(self) -> object: ...


def _load_lit_components() -> tuple[LitDatasetModule, LitModelModule, LitTypesModule]:
    try:
        importlib.import_module("lit_nlp.api")
    except ImportError as exc:
        message = (
            "LIT dependencies are unavailable. Install lit-nlp in an isolated environment "
            + "or add it as an extra before using this integration. Try `uv sync --extra lit` "
            + "or `uv add lit-nlp`."
        )
        raise RuntimeError(message) from exc

    module_importer = DefaultModuleImporter()
    dataset_mod = cast(LitDatasetModule, module_importer.import_dataset_module())
    model_mod = cast(LitModelModule, module_importer.import_model_module())
    types_mod = cast(LitTypesModule, module_importer.import_types_module())
    return dataset_mod, model_mod, types_mod


class LitServerFactory(Protocol):
    def __call__(
        self,
        models: dict[str, LitModel],
        datasets: dict[str, LitDataset],
    ) -> object: ...


def _import_lit_server() -> ModuleType:
    paths = (
        "lit_nlp.server",
        "lit_nlp.dev_server",
        "lit_nlp.runtime.server",
        "lit_nlp.lib.server",
    )
    last_err: Exception | None = None
    for candidate in paths:
        try:
            return importlib.import_module(candidate)
        except (ImportError, ModuleNotFoundError) as err:
            last_err = err

    try:
        lit_pkg = importlib.import_module("lit_nlp")
        lit_ver = getattr(lit_pkg, "__version__", "<unknown>")
        ver_msg = f"(detected lit-nlp version: {lit_ver})"
    except (ImportError, AttributeError):
        ver_msg = "(lit-nlp not importable)"

    message = (
        "Unable to import LIT server module. Tried: lit_nlp.server, "
        + "lit_nlp.dev_server, lit_nlp.runtime.server, lit_nlp.lib.server.\n"
        + f"{ver_msg}. Last error: {last_err}"
    )
    raise RuntimeError(message)


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
) -> None:
    """Launch a minimal LIT server for the bundestag_char PoC.

    This uses a tiny embedded text dataset and a trivial echo model to
    demonstrate the LIT UI without requiring trained checkpoints.
    """
    if host is None:
        host = get_default_host()

    if logger is None:
        logger = logging.getLogger(__name__)

    try:
        if _loader_override:
            dataset_mod, model_mod, types_mod = _loader_override()
        else:
            dataset_mod, model_mod, types_mod = _load_lit_components()
    except RuntimeError as exc:
        message = (
            "LIT dependencies not available. Install lit-nlp in an isolated environment "
            + "or add it as an extra before using this integration. Try `uv sync --extra lit` "
            + "or `uv add lit-nlp`."
        )
        raise RuntimeError(message) from exc

    try:
        if _server_importer_override:
            server_module = _server_importer_override()
        else:
            server_module = _import_lit_server()
    except RuntimeError as exc:
        message = (
            "LIT server import failed. Ensure a supported lit-nlp version is installed. "
            + "Try `uv sync --extra lit` or `uv add lit-nlp`."
        )
        raise RuntimeError(message) from exc

    dataset_base = dataset_mod.Dataset
    model_base = model_mod.Model
    text_segment_factory = types_mod.TextSegment

    # --- Tiny sample dataset ---
    # Prefer a few lines from the bundestag_char seed if present; otherwise use embedded samples.
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

    # Try to read input.txt if it exists, but keep it optional and tiny.
    try:
        # Resolve to the src/ml_playground/experiments/bundestag_char directory,
        # tolerating environments (e.g., tests) that stub Path.parents with fewer entries.
        if _path_resolver_override:
            resolved = _path_resolver_override(Path(__file__))
        else:
            resolved = Path(__file__).resolve()
        try:
            base_dir = resolved.parents[3]
        except Exception:
            # Fallback for stubs that only provide shallower parents (tests inject parents[2])
            base_dir = resolved.parents[2]
        exp_dir = base_dir / "experiments" / "bundestag_char"
        input = exp_dir / "datasets" / "input.txt"
        if input.exists():
            # Stream only the first few non-empty lines to avoid loading large
            # corpora into memory for this tiny demo dataset.
            file_lines: list[str] = []
            with input.open("r", encoding="utf-8", errors="ignore") as input_file:
                for raw_line in input_file:
                    line = raw_line.strip()
                    if not line:
                        continue
                    file_lines.append(line)
                    if len(file_lines) >= 10:
                        break
            if file_lines:
                samples = file_lines
    except (OSError, UnicodeError):
        # Non-fatal; keep embedded samples
        pass

    class BundestagTextDataset(dataset_base):  # type: ignore[valid-type, misc]
        def __init__(self, sents: Iterable[str]):
            self._examples: list[Mapping[str, str]] = [{"text": s} for s in sents]

        @override
        def spec(self) -> dict[str, object]:
            return {"text": text_segment_factory()}

        @override
        def __len__(self) -> int:
            return len(self._examples)

        @override
        def __iter__(self):
            return iter(self._examples)

    class EchoModel(model_base):  # type: ignore[valid-type, misc]
        """Trivial model that returns the input text as generated output.

        Serves as a PoC to exercise LIT views for text data without trained weights.
        """

        @override
        def input_spec(self) -> dict[str, object]:
            return {"text": text_segment_factory()}

        @override
        def output_spec(self) -> dict[str, object]:
            return {"generated": text_segment_factory()}

        @override
        def predict(
            self, _inputs: Iterable[Mapping[str, object]], **kwargs: object
        ) -> list[Mapping[str, object]]:
            outs: list[Mapping[str, object]] = []
            for ex in _inputs:
                s = str(ex.get("text", ""))
                # Simple deterministic transform to show change
                gen = s + "\n\n[echo] " + s[::-1]
                outs.append({"generated": gen})
            return outs

    datasets: dict[str, LitDataset] = {
        "bundestag_char_sample": BundestagTextDataset(samples)
    }
    models: dict[str, LitModel] = {"echo_model": EchoModel()}

    try:
        server_factory_obj = getattr(server_module, "Server", None)
        if not callable(server_factory_obj):
            raise RuntimeError("LIT server module does not expose a Server factory")
        server_factory = cast(LitServerFactory, server_factory_obj)
        app = server_factory(models, datasets)
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


def _parse_cli_args(argv: Sequence[str] | None = None) -> tuple[str, int, bool]:
    parser = argparse.ArgumentParser(
        description="Run LIT server for bundestag_char PoC"
    )
    try:
        default_host = get_default_host()
    except (ValueError, TypeError):
        default_host = "127.0.0.1"
    _host_arg = parser.add_argument(
        "--host", type=str, default=default_host, help="Host to bind"
    )
    _port_arg = parser.add_argument(
        "--port", type=int, default=5432, help="Port to bind (0 for auto)"
    )
    _open_browser_arg = parser.add_argument(
        "--open-browser", action="store_true", help="Open browser on start"
    )
    namespace = parser.parse_args(argv)

    host_attr = cast(object, namespace.host)
    if not isinstance(host_attr, str):
        raise TypeError("--host must be a string")
    host_value: str = host_attr

    port_attr = cast(object, namespace.port)
    if not isinstance(port_attr, int):
        raise TypeError("--port must be parsed as an integer")
    port_value: int = port_attr

    open_browser_attr = cast(object, namespace.open_browser)
    open_browser_value: bool
    if isinstance(open_browser_attr, bool):
        open_browser_value = open_browser_attr
    else:
        open_browser_value = bool(open_browser_attr)

    return host_value, port_value, open_browser_value


# Public wrappers (non-underscored) for external callers/tests
def load_lit_components() -> tuple[LitDatasetModule, LitModelModule, LitTypesModule]:
    """Public entry to load lit components."""
    return _load_lit_components()


def import_lit_server() -> ModuleType:
    """Public entry to import lit server module."""
    return _import_lit_server()


def parse_cli_args(argv: Sequence[str] | None = None) -> tuple[str, int, bool]:
    """Public entry to parse CLI args for the lit server."""
    return _parse_cli_args(argv)


__all__ = [
    "LitDataset",
    "LitDatasetModule",
    "LitModel",
    "LitModelModule",
    "LitTypesModule",
    "load_lit_components",
    "import_lit_server",
    "parse_cli_args",
    "run_server_bundestag_char",
]


if __name__ == "__main__":
    host_arg, port_arg, open_browser_arg = _parse_cli_args()
    run_server_bundestag_char(
        host=host_arg,
        port=port_arg,
        open_browser=open_browser_arg,
    )
