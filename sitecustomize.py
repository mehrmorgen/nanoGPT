"""Bootstrap src-layout imports for local CLI and tooling runs.

This file is auto-imported by Python's `site` module when present on
`sys.path`. It ensures `src/` is discoverable even when editable-install
`.pth` processing is unavailable in the active environment.
"""

from __future__ import annotations

import sys
from pathlib import Path


_REPO_ROOT = Path(__file__).resolve().parent
_SRC_PATH = _REPO_ROOT / "src"

if _SRC_PATH.is_dir():
    src_str = str(_SRC_PATH)
    if src_str not in sys.path:
        sys.path.insert(0, src_str)
