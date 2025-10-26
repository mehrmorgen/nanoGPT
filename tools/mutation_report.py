#!/usr/bin/env python3
"""Summarize Cosmic Ray session results after a mutation run."""

from __future__ import annotations

import argparse
import sqlite3
from collections import Counter
from collections.abc import Mapping
from pathlib import Path
from typing import cast

from cosmic_ray.config import load_config


def summarize_session(session_path: Path) -> None:
    if not session_path.exists():
        print(f"[mutation] session file not found: {session_path}")
        return

    with sqlite3.connect(session_path) as conn:
        conn.row_factory = lambda cursor, row: row[0]
        total = conn.execute("SELECT COUNT(*) FROM work_results").fetchone() or 0
        outcomes = Counter(
            conn.execute("SELECT COALESCE(test_outcome, 'UNKNOWN') FROM work_results")
        )

    print(f"[mutation] mutants processed: {total}")
    if not outcomes:
        return

    for outcome, count in sorted(outcomes.items()):
        label = outcome.lower()
        print(f"[mutation]   {label}: {count}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Report the number of mutants processed in the current Cosmic Ray session."
    )
    parser.add_argument(
        "--config",
        default="pyproject.toml",
        type=Path,
        help="Path to the Cosmic Ray configuration file (default: pyproject.toml)",
    )
    args = parser.parse_args()

    cfg = load_config(str(args.config))
    session_cfg_raw = cfg.get("session")
    session_cfg: Mapping[str, object]
    if isinstance(session_cfg_raw, Mapping):
        session_cfg = session_cfg_raw
    else:
        session_cfg = {}

    session_file = session_cfg.get("file", ".cache/cosmic-ray/session.sqlite")
    session_path = Path(str(cast(object, session_file)))
    summarize_session(session_path)


if __name__ == "__main__":
    main()
