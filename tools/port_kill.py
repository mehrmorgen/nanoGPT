#!/usr/bin/env python3
"""
Cross-platform utility to kill processes listening on a port.

Usage:
  python tools/port_kill.py --port 5432 [--graceful]

- On all platforms, uses psutil to identify listeners and sends SIGINT, then SIGTERM.
- Requires 'psutil' (lightweight); if missing, instructs how to install.
"""

from __future__ import annotations

import argparse
import os
import signal
import sys
import time
from typing import cast

try:
    import psutil  # type: ignore
except Exception:
    print(
        "\n".join(
            [
                "psutil is required for tools/port_kill.py. Install with:",
                "  uv pip install -p .venv312/bin/python psutil",
                "or: pip install psutil",
            ]
        ),
        file=sys.stderr,
    )
    sys.exit(2)


def listeners_on_port(port: int) -> list[int]:
    pids: set[int] = set()
    for conn in psutil.net_connections(kind="inet"):
        if conn.laddr and conn.laddr.port == port and conn.status == psutil.CONN_LISTEN:
            if conn.pid:
                pids.add(conn.pid)
    return sorted(pids)


def main() -> int:
    ap = argparse.ArgumentParser()
    _ = ap.add_argument("--port", type=int, required=True)
    _ = ap.add_argument(
        "--graceful", action="store_true", help="Send SIGINT before SIGTERM"
    )
    namespace = ap.parse_args()

    port = cast(int, namespace.port)
    graceful = cast(bool, namespace.graceful)

    pids = listeners_on_port(port)
    if not pids:
        print(f"No process listening on {port}")
        return 0

    print(f"Found listeners on port {port}: {pids}")
    if graceful:
        for pid in pids:
            try:
                os.kill(pid, signal.SIGINT)
            except Exception:
                pass
    # brief grace period
    time.sleep(0.5)

    # recheck and force kill remaining
    pids2 = listeners_on_port(port)
    for pid in pids2:
        try:
            os.kill(pid, signal.SIGTERM)
        except Exception:
            pass

    remaining = listeners_on_port(port)
    if remaining:
        print(f"Some listeners remain on {port}: {remaining}")
        return 1
    print(f"Port {port} is clear")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
