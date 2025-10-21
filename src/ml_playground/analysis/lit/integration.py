from __future__ import annotations

import sys
import logging
from pathlib import Path
from typing import Callable, Dict, Iterable, List, Mapping, cast
import importlib

WSGIApp = Callable[..., Iterable[bytes]]


def run_server_bundestag_char(
    host: str = "127.0.0.1",
    port: int = 5432,
    open_browser: bool = False,
) -> None:
    """Launch a minimal LIT server for the bundestag_char PoC.

    This uses a tiny embedded text dataset and a trivial echo model to
    demonstrate the LIT UI without requiring trained checkpoints.
    """

    def _import_lit_server():
        paths = [
            "lit_nlp.server",
            "lit_nlp.dev_server",
            "lit_nlp.runtime.server",
            "lit_nlp.lib.server",
        ]
        last_err: Exception | None = None
        for p in paths:
            try:
                return importlib.import_module(p)
            except (
                ImportError,
                ModuleNotFoundError,
            ) as err:  # pragma: no cover - best-effort compatibility
                last_err = err
        # If all imports failed, raise with context
        try:
            import lit_nlp  # type: ignore

            lit_ver = getattr(lit_nlp, "__version__", "<unknown>")
            ver_msg = f"(detected lit-nlp version: {lit_ver})"
        except (ImportError, AttributeError):
            ver_msg = "(lit-nlp not importable)"
        raise RuntimeError(
            "Unable to import LIT server module. Tried: lit_nlp.server, "
            "lit_nlp.dev_server, lit_nlp.runtime.server, lit_nlp.lib.server.\n"
            f"{ver_msg}. Last error: {last_err}"
        )

    try:
        # Lazy imports to avoid hard-dependency unless the command is used.
        from lit_nlp.api import dataset as lit_dataset  # type: ignore
        from lit_nlp.api import model as lit_model  # type: ignore
        from lit_nlp.api import types as lit_types  # type: ignore

        lit_server = _import_lit_server()
    except ImportError as e:  # pragma: no cover - import-guard path
        # Try to include lit-nlp version info to aid debugging
        try:
            import lit_nlp  # type: ignore

            lit_ver = getattr(lit_nlp, "__version__", "<unknown>")
            ver_msg = f"(detected lit-nlp version: {lit_ver})"
        except (ImportError, AttributeError):
            ver_msg = "(lit-nlp not importable)"
        raise RuntimeError(
            "LIT is not available or incompatible. "
            f"{ver_msg}\n"
            "Install an appropriate version in an isolated Python 3.12 env, e.g.:\n"
            "  uv run --no-project --python 3.12 --with 'lit-nlp>=1.3.1' --with 'numpy<2' -- python -m ml_playground.analysis.lit_integration\n"
            "Alternatively, add the extra directly to your project with:\n"
            "  uv add lit-nlp\n"
            "Or use the lit-tasks CLI: 'uv run lit-tasks setup' followed by 'uv run lit-tasks run'.\n"
            "See docs/LIT.md for details."
        ) from e

    # --- Tiny sample dataset ---
    # Prefer a few lines from the bundestag_char seed if present; otherwise use embedded samples.
    samples: List[str] = [
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

    class BundestagTextDataset(lit_dataset.Dataset):
        def __init__(self, sents: Iterable[str]):
            self._examples: List[Mapping[str, str]] = [{"text": s} for s in sents]

        def spec(self) -> Dict[str, object]:  # type: ignore[override]
            return {
                "text": lit_types.TextSegment(),
            }

        def __len__(self) -> int:
            return len(self._examples)

        def __iter__(self):
            return iter(self._examples)

    class EchoModel(lit_model.Model):
        """Trivial model that returns the input text as generated output.

        Serves as a PoC to exercise LIT views for text data without trained weights.
        """

        def input_spec(self) -> Dict[str, object]:  # type: ignore[override]
            return {"text": lit_types.TextSegment()}

        def output_spec(self) -> Dict[str, object]:  # type: ignore[override]
            # Use TextSegment for broad compatibility; some LIT versions also have GeneratedText.
            return {"generated": lit_types.TextSegment()}

        def predict(
            self, _inputs: Iterable[Mapping[str, object]], **kwargs: object
        ) -> List[Mapping[str, object]]:
            outs: List[Mapping[str, object]] = []
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
    logger = logging.getLogger(__name__)
    logger.info(f"Registered models: {', '.join(models.keys())}")
    logger.info(f"Registered datasets: {', '.join(datasets.keys())}")
    logger.info(f"Starting server at {url}")
    sys.stdout.flush()

    # Prefer the first-party serve method exposed by lit.Server
    serve_method = getattr(app, "serve", None)
    started = False
    if callable(serve_method):
        serve_kwargs = {"port": port, "host": host, "open_browser": open_browser}
        try:
            serve_method(**serve_kwargs)
            started = True
        except TypeError:
            # Try legacy positional signatures used by older lit-nlp releases.
            try:
                serve_method(port, host, open_browser)
                started = True
            except TypeError:
                try:
                    serve_method(port, host)
                    started = True
                except Exception as err:  # pragma: no cover - defensive
                    logger.debug("Failed legacy serve(%s, %s): %s", port, host, err)

    if started:
        return

    module_serve = getattr(lit_server, "serve", None)
    if callable(module_serve):
        serve_kwargs = {
            "app": app,
            "port": port,
            "host": host,
            "open_browser": open_browser,
        }
        try:
            module_serve(**serve_kwargs)
            started = True
        except TypeError:
            try:
                module_serve(app, port, host, open_browser)
                started = True
            except TypeError:
                try:
                    module_serve(app, port, host)
                    started = True
                except Exception as err:  # pragma: no cover - defensive
                    logger.debug(
                        "Failed module-level serve(%s, %s): %s", port, host, err
                    )

    if started:
        return

    # Fall back to running the wrapped Flask/Werkzeug application directly.
    wsgi_app_candidate = getattr(app, "app", None)
    if not callable(wsgi_app_candidate):
        raise RuntimeError(
            "Unable to start LIT server: neither Server.serve nor Server.app "
            "provided a runnable entrypoint."
        )
    wsgi_app = cast(WSGIApp, wsgi_app_candidate)

    try:
        from werkzeug.serving import run_simple  # type: ignore
    except ImportError as err:  # pragma: no cover - Werkzeug should be available
        raise RuntimeError(
            "Unable to import werkzeug.serving.run_simple; cannot launch LIT server."
        ) from err

    logger.debug(
        "Starting LIT via werkzeug.run_simple(hostname=%s, port=%s) using app.app",
        host,
        port,
    )
    run_simple(hostname=host, port=port or 5432, application=wsgi_app)


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
        host=args.host, port=args.port, open_browser=args.open_browser
    )
