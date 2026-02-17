# pyright: reportPrivateUsage=false

"""Compatibility wrapper for framework LIT integration."""

from __future__ import annotations

import argparse
import importlib

from ml_playground.framework.analysis.lit import integration as _integration
from ml_playground.framework.core.logging_protocol import LoggerLike

Path = _integration.Path
_load_lit_components = _integration._load_lit_components
_import_lit_server = _integration._import_lit_server
_parse_cli_args = _integration._parse_cli_args

import_lit_server = _integration.import_lit_server
load_lit_components = _integration.load_lit_components
parse_cli_args = _integration.parse_cli_args


def run_server_experiment(
    *,
    experiment: str,
    host: str = "127.0.0.1",
    port: int = 5432,
    open_browser: bool = False,
    logger: LoggerLike | None = None,
) -> None:
    """Delegate to framework integration while honoring test overrides."""
    original_load = _integration._load_lit_components
    original_import = _integration._import_lit_server
    try:
        _integration._load_lit_components = _load_lit_components
        _integration._import_lit_server = _import_lit_server
        _integration.run_server_experiment(
            experiment=experiment,
            host=host,
            port=port,
            open_browser=open_browser,
            logger=logger,
        )
    finally:
        _integration._load_lit_components = original_load
        _integration._import_lit_server = original_import


def run_server_bundestag_char(
    host: str = "127.0.0.1",
    port: int = 5432,
    open_browser: bool = False,
    logger: LoggerLike | None = None,
) -> None:
    """Backward-compatible alias around run_server_experiment."""
    run_server_experiment(
        experiment="bundestag_char",
        host=host,
        port=port,
        open_browser=open_browser,
        logger=logger,
    )


__all__ = [
    "Path",
    "argparse",
    "importlib",
    "_load_lit_components",
    "_import_lit_server",
    "_parse_cli_args",
    "import_lit_server",
    "load_lit_components",
    "parse_cli_args",
    "run_server_experiment",
    "run_server_bundestag_char",
]
