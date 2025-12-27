"""Shim for the hardened LIT integration in the lit/ subpackage."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, cast

from .lit.integration import run_server_bundestag_char as _run_server

if TYPE_CHECKING:  # pragma: no cover
    from ml_playground.core.logging_protocol import LoggerLike  # pragma: no cover


def run_server_bundestag_char(
    host: str,
    port: int,
    open_browser: bool,
    logger: LoggerLike,
) -> None:
    """Launch a minimal LIT server for the bundestag_char PoC.

    Delegates to the hardened implementation in `ml_playground.tools.analysis.lit.integration`.
    """
    _run_server(host=host, port=port, open_browser=open_browser)


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

    # Simple logger for CLI usage
    logging.basicConfig(level=logging.INFO)
    cli_logger = logging.getLogger(__name__)

    run_server_bundestag_char(
        host=args.host,
        port=args.port,
        open_browser=args.open_browser,
        logger=cast("LoggerLike", cli_logger),
    )
