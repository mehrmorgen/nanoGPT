from __future__ import annotations

import importlib
import logging
import sys
from collections.abc import Callable, Iterable, Mapping, Sequence
from pathlib import Path
from types import ModuleType
from typing import Protocol, cast, override

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

    dataset_mod = cast(LitDatasetModule, importlib.import_module("lit_nlp.api.dataset"))
    model_mod = cast(LitModelModule, importlib.import_module("lit_nlp.api.model"))
    types_mod = cast(LitTypesModule, importlib.import_module("lit_nlp.api.types"))
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
    host: str = "127.0.0.1",
    port: int = 5432,
    open_browser: bool = False,
) -> None:
    """Launch a minimal LIT server for the bundestag_char PoC.

    This uses a tiny embedded text dataset and a trivial echo model to
    demonstrate the LIT UI without requiring trained checkpoints.
    """

    try:
        dataset_mod, model_mod, types_mod = _load_lit_components()
        server_module = _import_lit_server()
    except RuntimeError as exc:
        message = (
            "LIT is not available or incompatible. Install an appropriate version before "
            + "using this integration. Try `uv sync --extra lit` or `uv add lit-nlp`."
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
        # Resolve to the src/ml_playground/experiments/bundestag_char directory
        base_dir = Path(__file__).resolve().parents[2]
        exp_dir = base_dir / "experiments" / "bundestag_char"
        input = exp_dir / "datasets" / "input.txt"
        if input.exists():
            text = input.read_text(encoding="utf-8", errors="ignore")
            # Take up to 10 reasonably short lines.
            file_lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
            if file_lines:
                samples = file_lines[:10]
    except (OSError, UnicodeError):
        # Non-fatal; keep embedded samples
        pass

    class BundestagTextDataset(dataset_base):
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

    class EchoModel(model_base):
        """Trivial model that returns the input text as generated output.

        Serves as a PoC to exercise LIT views for text data without trained weights.
        """

        @override
        def input_spec(self) -> dict[str, object]:
            return {"text": text_segment_factory()}

        @override
        def output_spec(self) -> dict[str, object]:
            # Use TextSegment for broad compatibility; some LIT versions also have GeneratedText.
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
    logger = logging.getLogger(__name__)
    logger.info(f"Registered models: {', '.join(models.keys())}")
    logger.info(f"Registered datasets: {', '.join(datasets.keys())}")
    logger.info(f"Starting server at {url}")
    _ = sys.stdout.flush()

    # Prefer the first-party serve method exposed by lit.Server
    serve_method = getattr(app, "serve", None)
    started = False
    if callable(serve_method):
        serve_kwargs = {"port": port, "host": host, "open_browser": open_browser}
        try:
            _ = serve_method(**serve_kwargs)
            started = True
        except TypeError:
            # Try legacy positional signatures used by older lit-nlp releases.
            try:
                _ = serve_method(port, host, open_browser)
                started = True
            except TypeError:
                try:
                    _ = serve_method(port, host)
                    started = True
                except Exception as err:
                    logger.debug("Failed legacy serve(%s, %s): %s", port, host, err)

    if started:
        return

    module_serve = getattr(server_module, "serve", None)
    if callable(module_serve):
        serve_kwargs = {
            "app": app,
            "port": port,
            "host": host,
            "open_browser": open_browser,
        }
        try:
            _ = module_serve(**serve_kwargs)
            started = True
        except TypeError:
            try:
                _ = module_serve(app, port, host, open_browser)
                started = True
            except TypeError:
                try:
                    _ = module_serve(app, port, host)
                    started = True
                except Exception as err:
                    logger.debug(
                        "Failed module-level serve(%s, %s): %s", port, host, err
                    )

    if started:
        return

    tried_calls: list[str] = []
    try:
        from werkzeug.serving import run_simple  # type: ignore

        # 3a) Try the object itself as a WSGI application
        try:
            logger.info(
                "Fallback: starting via werkzeug.run_simple(...) using app as WSGI application"
            )
            tried_calls.append("werkzeug.run_simple(app)")
            _ = run_simple(
                hostname=host, port=port or 5432, application=cast(WSGIApp, app)
            )
            started = True
        except (RuntimeError, TypeError, ValueError):
            # 3b) Try a nested .app attribute (common Flask pattern)
            if hasattr(app, "app"):
                wsgi_app = cast(WSGIApp, getattr(app, "app"))
                logger.info(
                    "Fallback: starting via werkzeug.run_simple(...) using app.app as WSGI application"
                )
                tried_calls.append("werkzeug.run_simple(app.app)")
                _ = run_simple(hostname=host, port=port or 5432, application=wsgi_app)
                started = True
    except (ImportError, RuntimeError, TypeError, ValueError):
        tried_calls.append("werkzeug.run_simple import failed")

    if not started:
        attempted = ", ".join(tried_calls) if tried_calls else "<none>"
        message = (
            "Unable to start LIT server: no compatible entrypoint found.\n"
            + f"Tried call patterns on: {attempted}.\n"
            + "Consider updating lit-nlp or using an alternative version compatible with this integration."
        )
        raise RuntimeError(message)


def _parse_cli_args(argv: Sequence[str] | None = None) -> tuple[str, int, bool]:
    import argparse

    parser = argparse.ArgumentParser(
        description="Run LIT server for bundestag_char PoC"
    )
    _host_arg = parser.add_argument(
        "--host", type=str, default="127.0.0.1", help="Host to bind"
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


if __name__ == "__main__":
    host_arg, port_arg, open_browser_arg = _parse_cli_args()
    run_server_bundestag_char(
        host=host_arg,
        port=port_arg,
        open_browser=open_browser_arg,
    )
