"""Shim for the hardened LIT integration in the lit/ subpackage."""

from __future__ import annotations

import logging
from collections.abc import Callable
from typing import TYPE_CHECKING, cast

from ml_playground.framework.core.project_config import get_default_host
from ml_playground.framework.analysis.lit.integration import (
    run_server_experiment as _run_server_experiment,
    run_server_bundestag_char as _run_server,
)

if TYPE_CHECKING:
    from ml_playground.framework.core.logging_protocol import (
        LoggerLike,
    )


def run_server_bundestag_char(
    host: str,
    port: int,
    open_browser: bool,
    logger: LoggerLike,
    _run_server_override: Callable[..., None] | None = None,
) -> None:
    """Launch a minimal LIT server for the bundestag_char PoC.

    Delegates to the implementation in `ml_playground.framework.analysis.lit.integration`.
    """
    if _run_server_override:
        _run_server_override(host=host, port=port, open_browser=open_browser)
    else:
        _run_server(host=host, port=port, open_browser=open_browser)


def run_server_experiment(
    *,
    experiment: str,
    host: str,
    port: int,
    open_browser: bool,
    logger: LoggerLike,
    _run_server_override: Callable[..., None] | None = None,
) -> None:
    """Launch a LIT server for a specific experiment."""
    if _run_server_override:
        _run_server_override(
            experiment=experiment,
            host=host,
            port=port,
            open_browser=open_browser,
        )
    else:
        _run_server_experiment(
            experiment=experiment,
            host=host,
            port=port,
            open_browser=open_browser,
            logger=logger,
        )


def main(
    default_host: str | None = None,
    _run_server_override: Callable[..., None] | None = None,
) -> None:
    """CLI entry point for the LIT shim."""
    import argparse

    if default_host is None:
        try:
            default_host = get_default_host()
        except (ValueError, TypeError):
            default_host = "localhost"

    parser = argparse.ArgumentParser(description="Run experiment LIT server")
    parser.add_argument(
        "--experiment",
        type=str,
        default="bundestag_char",
        help="Experiment name owning the LIT integration",
    )
    parser.add_argument("--host", type=str, default=default_host, help="Host to bind")
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

    run_server_experiment(
        experiment=cast(str, args.experiment),
        host=cast(str, args.host),
        port=cast(int, args.port),
        open_browser=cast(bool, args.open_browser),
        logger=cast("LoggerLike", cli_logger),
        _run_server_override=_run_server_override,
    )


if __name__ == "__main__":
    main()
