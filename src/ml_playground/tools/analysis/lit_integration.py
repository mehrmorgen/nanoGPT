from __future__ import annotations

import sys
from collections.abc import Callable, Iterable, Mapping
from pathlib import Path
from types import ModuleType
from typing import Any, cast

import logging

from ml_playground.core.logging_protocol import LoggerLike


def run_server_bundestag_char(
    host: str,
    port: int,
    open_browser: bool,
    logger: LoggerLike,
) -> None:
    """Launch a minimal LIT server for the bundestag_char PoC.

    This uses a tiny embedded text dataset and a trivial echo model to
    demonstrate the LIT UI without requiring trained checkpoints.
    """

    def _import_lit_server() -> ModuleType:
        try:
            from lit_nlp import server  # type: ignore[import]

            return cast(ModuleType, server)
        except ImportError as err:
            raise RuntimeError(
                "LIT server import failed. Ensure lit-nlp is installed and compatible. "
                f"Error: {err}"
            ) from err

    try:
        # Lazy imports to avoid hard-dependency unless the command is used.
        from lit_nlp.api import dataset as lit_dataset  # type: ignore[import]
        from lit_nlp.api import model as lit_model  # type: ignore[import]
        from lit_nlp.api import types as lit_types  # type: ignore[import]

        lit_server = _import_lit_server()
        dataset_base = cast(type[Any], lit_dataset.Dataset)
        model_base = cast(type[Any], lit_model.Model)
        types_module = cast(ModuleType, lit_types)
    except ImportError as e:
        raise RuntimeError(
            f"LIT dependencies not available: {e}. Install lit-nlp with: uv add lit-nlp"
        ) from e

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

    text_segment_factory = cast(
        Callable[[], object], getattr(types_module, "TextSegment")
    )

    class BundestagTextDataset(dataset_base):  # type: ignore[valid-type, misc]
        def __init__(self, sents: Iterable[str]):
            self._examples: list[Mapping[str, str]] = [{"text": s} for s in sents]

        def spec(self) -> dict[str, object]:  # type: ignore[override]
            return {
                "text": text_segment_factory(),
            }

        def __len__(self) -> int:
            return len(self._examples)

        def __iter__(self):
            return iter(self._examples)

    class EchoModel(model_base):  # type: ignore[valid-type, misc]
        """Trivial model that returns the input text as generated output.

        Serves as a PoC to exercise LIT views for text data without trained weights.
        """

        def input_spec(self) -> dict[str, object]:  # type: ignore[override]
            return {"text": text_segment_factory()}

        def output_spec(self) -> dict[str, object]:  # type: ignore[override]
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

    datasets = {"bundestag_char_sample": BundestagTextDataset(samples)}
    models = {"echo_model": EchoModel()}

    try:
        server_factory = getattr(lit_server, "Server")
        app = server_factory(models, datasets)
    except Exception as e:
        raise RuntimeError(f"Failed to build LIT app: {e}") from e

    url = f"http://{host}:{port if port else '<auto>'}"
    logger.info(f"Registered models: {', '.join(models.keys())}")
    logger.info(f"Registered datasets: {', '.join(datasets.keys())}")
    logger.info(f"Starting server at {url}")
    sys.stdout.flush()

    try:
        serve = getattr(app, "serve")
        serve(port=port, host=host, open_browser=open_browser)
    except AttributeError as exc:
        raise RuntimeError(
            "Installed LIT server does not expose a serve(...) method. "
            "Update lit-nlp to a compatible version."
        ) from exc


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Run LIT server for bundestag_char PoC"
    )
    parser.add_argument("--host", type=str, default="127.0.0.1", help="Host to bind")
    parser.add_argument(
        "--port", type=int, default=5432, help="Port to bind (0 for auto)"
    )
    parser.add_argument(
        "--open-browser", action="store_true", help="Open browser on start"
    )
    args = parser.parse_args()
    run_server_bundestag_char(
        host=args.host,
        port=args.port,
        open_browser=args.open_browser,
        logger=cast(LoggerLike, logging.getLogger(__name__)),
    )
