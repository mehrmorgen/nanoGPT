from __future__ import annotations

import importlib
import logging
import sys
from collections.abc import Callable, Iterable, Mapping, Sequence
from pathlib import Path
from types import ModuleType
from typing import Any, cast

WSGIApp = Callable[..., Iterable[bytes]]


def _load_lit_components() -> tuple[ModuleType, ModuleType, ModuleType]:
    try:
        importlib.import_module("lit_nlp.api")
    except ImportError as exc:
        message = (
            "LIT dependencies are unavailable. Install lit-nlp in an isolated environment "
            + "or add it as an extra before using this integration. Try `uv sync --extra lit` "
            + "or `uv add lit-nlp`."
        )
        raise RuntimeError(message) from exc

    dataset_mod = importlib.import_module("lit_nlp.api.dataset")
    model_mod = importlib.import_module("lit_nlp.api.model")
    types_mod = importlib.import_module("lit_nlp.api.types")
    return dataset_mod, model_mod, types_mod


def _import_lit_server() -> ModuleType:
    # Standard modern import path for LIT development server
    try:
        return importlib.import_module("lit_nlp.dev_server")
    except (ImportError, ModuleNotFoundError) as err:
        # Fallback for older versions or alternative structures
        try:
            return importlib.import_module("lit_nlp.server")
        except (ImportError, ModuleNotFoundError):
            pass

        try:
            lit_pkg = importlib.import_module("lit_nlp")
            lit_ver = getattr(lit_pkg, "__version__", "<unknown>")
            ver_msg = f"(detected lit-nlp version: {lit_ver})"
        except (ImportError, AttributeError):
            ver_msg = "(lit-nlp not importable)"

        message = (
            "Unable to import LIT server module. Tried: lit_nlp.dev_server, lit_nlp.server.\n"
            + f"{ver_msg}. Last error: {err}"
        )
        raise RuntimeError(message) from err


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

    dataset_base_any = cast(type[Any], dataset_base)
    model_base_any = cast(type[Any], model_base)

    class BundestagTextDataset(dataset_base_any):  # type: ignore[valid-type, misc]
        def __init__(self, sents: Iterable[str]):
            self._examples: list[Mapping[str, str]] = [{"text": s} for s in sents]

        def spec(self) -> dict[str, object]:
            return {"text": text_segment_factory()}

        def __len__(self) -> int:
            return len(self._examples)

        def __iter__(self):
            return iter(self._examples)

    class EchoModel(model_base_any):  # type: ignore[valid-type, misc]
        """Trivial model that returns the input text as generated output.

        Serves as a PoC to exercise LIT views for text data without trained weights.
        """

        def input_spec(self) -> dict[str, object]:
            return {"text": text_segment_factory()}

        def output_spec(self) -> dict[str, object]:
            # Use TextSegment for broad compatibility; some LIT versions also have GeneratedText.
            return {"generated": text_segment_factory()}

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

    datasets: dict[str, object] = {
        "bundestag_char_sample": BundestagTextDataset(samples)
    }
    models: dict[str, object] = {"echo_model": EchoModel()}

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
    logger = logging.getLogger(__name__)
    logger.info(f"Registered models: {', '.join(models.keys())}")
    logger.info(f"Registered datasets: {', '.join(datasets.keys())}")
    logger.info(f"Starting server at {url}")
    _ = sys.stdout.flush()

    # Use standard serve method
    app_serve = getattr(app, "serve", None)
    if callable(app_serve):
        app_serve(port=port, host=host, open_browser=open_browser)
        return

    # Fallback: module-level serve if app.serve is missing (older API)
    module_serve = getattr(server_module, "serve", None)
    if callable(module_serve):
        module_serve(app, port=port, host=host, open_browser=open_browser)
        return

    raise RuntimeError(
        "Unable to start LIT server: neither app.serve() nor module serve() available."
    )


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
