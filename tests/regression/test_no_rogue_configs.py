from __future__ import annotations

import tomllib
from pathlib import Path
from typing import Any

# Rogue configuration files that should not exist in the root (all config must be in pyproject.toml)
ROGUE_CONFIG_PATTERNS = [
    ".ruff.toml",
    "ruff.toml",
    ".mypy.ini",
    "mypy.ini",
    ".pyrightconfig.json",
    "pyrightconfig.json",
    ".pyrefly.toml",
    "pyrefly.toml",
    "pytest.ini",
    "setup.cfg",
    ".flake8",
    ".isort.cfg",
]


def test_enforce_no_rogue_configs(request: Any) -> None:
    """Enforce that no standalone config files exist outside the allowlist."""
    root_dir = Path(request.config.rootdir)
    pyproject_path = root_dir / "pyproject.toml"

    with open(pyproject_path, "rb") as f:
        config = tomllib.load(f)

    allowed = set(
        config.get("tool", {})
        .get("ml_playground", {})
        .get("regression", {})
        .get("policy", {})
        .get("allowed_config_files", [])
    )

    rogue_found: list[str] = []
    # Check root for specific rogue patterns
    for pattern in ROGUE_CONFIG_PATTERNS:
        for path in root_dir.glob(pattern):
            rel = path.name
            if rel not in allowed:
                rogue_found.append(rel)

    assert not rogue_found, (
        "Rogue configuration files found in root. All config must be in pyproject.toml.\n"
        "Offending files: " + ", ".join(rogue_found)
    )
