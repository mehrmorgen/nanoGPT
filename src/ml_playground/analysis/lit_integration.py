from __future__ import annotations

import logging
import sys
from collections.abc import Iterable, Mapping
from pathlib import Path
from typing import Protocol, cast


class LitDataset(Protocol):
    def spec(self) -> dict[str, object]: ...

    def __len__(self) -> int: ...

    def __iter__(self) -> Iterable[Mapping[str, object]]: ...


class LitModel(Protocol):
    def input_spec(self) -> dict[str, object]: ...

    def output_spec(self) -> dict[str, object]: ...

    def predict(
        self, _inputs: Iterable[Mapping[str, object]], **kwargs: object
    ) -> list[Mapping[str, object]]: ...


class LitTypesModule(Protocol):
    def TextSegment(self) -> object: ...


def run_server_bundestag_char(host: str, port: int, open_browser: bool, logger) -> None:
    """Launch a minimal LIT server for the bundestag_char PoC.

    This uses a tiny embedded text dataset and a trivial echo model to
    demonstrate the LIT UI without requiring trained checkpoints.
    """

    def _import_lit_server():
        try:
            from lit_nlp import server  # type: ignore[import]

            return server
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
    except ImportError as e:  # pragma: no cover - import-guard path
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
        base_dir = Path(__file__).resolve().parents[1]
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

    text_segment_factory = cast(LitTypesModule, lit_types).TextSegment

    class BundestagTextDataset(lit_dataset.Dataset):
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

    class EchoModel(lit_model.Model):
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
        app = lit_server.Server(models, datasets)
    except (
        TypeError,
        AttributeError,
        RuntimeError,
        ValueError,
    ) as e:  # pragma: no cover
        raise RuntimeError(f"Failed to build LIT app: {e}") from e

    url = f"http://{host}:{port if port else '<auto>'}"
    logger.info(f"Registered models: {', '.join(models.keys())}")
    logger.info(f"Registered datasets: {', '.join(datasets.keys())}")
    logger.info(f"Starting server at {url}")
    sys.stdout.flush()

    # Start server with maximum compatibility across LIT versions
    started = False
    tried_calls: list[str] = []

    def _try_call(target, name: str) -> bool:
        fn = getattr(target, name, None)
        if not callable(fn):
            return False
        tried_calls.append(f"{target.__class__.__name__}.{name}")
        try:
            fn(app, port=port, host=host, open_browser=open_browser)
            logger.info(
                f"Started via {name}(app, port=..., host=..., open_browser=...)"
            )
            return True
        except TypeError:
            try:
                fn(app, port, host)
                logger.info(f"Started via {name}(app, port, host)")
                return True
            except TypeError:
                try:
                    fn(port=port, host=host, open_browser=open_browser)
                    logger.info(
                        f"Started via {name}(port=..., host=..., open_browser=...)"
                    )
                    return True
                except TypeError:
                    try:
                        fn(port, host)
                        logger.info(f"Started via {name}(port, host)")
                        return True
                    except (RuntimeError, ValueError, OSError):
                        return False

    # 1) Try common module-level starters
    for fname in ("serve", "run", "start", "launch"):
        if _try_call(lit_server, fname):
            started = True
            break

    # 2) Try common app-level starters
    if not started:
        for fname in ("serve", "run", "start", "launch", "serve_forever"):
            fn = getattr(app, fname, None)
            if not callable(fn):
                continue
            tried_calls.append(f"app.{fname}")
            try:
                fn(port=port, host=host, open_browser=open_browser)  # type: ignore[misc]
                logger.info(
                    f"Started via app.{fname}(port=..., host=..., open_browser=...)"
                )
                started = True
                break
            except TypeError:
                try:
                    fn(port, host)  # type: ignore[misc]
                    logger.info(f"Started via app.{fname}(port, host)")
                    started = True
                    break
                except (RuntimeError, ValueError, OSError):
                    continue

    # 3) Final fallback: try to run via werkzeug.run_simple using common WSGI callables
    if not started:
        try:
            from werkzeug.serving import run_simple  # type: ignore

            # 3a) Try the object itself as a WSGI application
            try:
                logger.info(
                    "Fallback: starting via werkzeug.run_simple(...) using app as WSGI application"
                )
                run_simple(hostname=host, port=port or 5432, application=app)  # blocks
                started = True
            except (RuntimeError, TypeError, ValueError):
                # 3b) Try a nested .app attribute (common Flask pattern)
                if hasattr(app, "app"):
                    wsgi_app = getattr(app, "app")
                    logger.info(
                        "Fallback: starting via werkzeug.run_simple(...) using app.app as WSGI application"
                    )
                    run_simple(
                        hostname=host, port=port or 5432, application=wsgi_app
                    )  # blocks
                    started = True
        except (ImportError, RuntimeError, TypeError, ValueError):  # pragma: no cover
            tried_calls.append("werkzeug.run_simple(app|app.app)")

    if not started:
        tried = ", ".join(tried_calls) if tried_calls else "<none>"
        raise RuntimeError(
            "Unable to start LIT server: no compatible entrypoint found.\n"
            f"Tried call patterns on: {tried}.\n"
            "Consider updating lit-nlp or using an alternative version compatible with this integration."
        )


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
        logger=logging.getLogger(__name__),
    )
