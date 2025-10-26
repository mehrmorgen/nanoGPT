#!/usr/bin/env python3
# pyright: reportMissingTypeStubs=false
"""Emit a quick summary of the current Cosmic Ray mutation configuration."""

from __future__ import annotations

import argparse
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import TYPE_CHECKING, cast

if TYPE_CHECKING:  # pragma: no cover - import-time typing helpers
    from cosmic_ray.config import load_config
    from cosmic_ray.modules import find_modules
else:  # pragma: no cover - runtime imports
    try:  # pragma: no cover - defensive
        from cosmic_ray.config import load_config  # type: ignore[import]
        from cosmic_ray.modules import find_modules  # type: ignore[import]
    except ModuleNotFoundError as exc:  # pragma: no cover
        raise RuntimeError(
            "cosmic_ray must be installed to use tools/mutation_summary.py"
        ) from exc


def _format_duration(seconds: float) -> str:
    if seconds < 60:
        return f"{seconds:.1f}s"
    minutes, rem = divmod(seconds, 60)
    if minutes < 60:
        return f"{int(minutes)}m {rem:.0f}s"
    hours, minutes = divmod(minutes, 60)
    return f"{int(hours)}h {int(minutes)}m"


def _ensure_mapping(value: object) -> dict[str, object]:
    if isinstance(value, Mapping):
        result: dict[str, object] = {}
        for key, val in value.items():
            result[str(key)] = cast(object, val)
        return result
    return {}


def _ensure_sequence(value: object | None) -> Sequence[object]:
    if value is None:
        return ()
    if isinstance(value, (str, bytes, bytearray)):
        return (value,)
    if isinstance(value, Sequence):
        return tuple(value)
    return (value,)


def _to_float(value: object, default: float) -> float:
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        try:
            return float(value)
        except ValueError:
            return default
    return default


def _count_modules(module_path: Path) -> int:
    modules = list(find_modules([module_path]))
    return len(modules)


def summarize(config_path: Path) -> None:
    cfg = load_config(str(config_path))

    module_path_raw = cfg.get("module-path", config_path.parent)
    module_path = Path(str(module_path_raw))

    timeout_raw = cfg.get("timeout", 10.0)
    timeout = _to_float(timeout_raw, 10.0)

    session_cfg = _ensure_mapping(cfg.get("session"))
    session_file = str(session_cfg.get("file", ".cache/cosmic-ray/session.sqlite"))

    test_command_raw = cfg.get("test-command")
    rendered_command: str
    sequence = _ensure_sequence(test_command_raw)
    rendered_command = " ".join(str(part) for part in sequence) or "<unset>"

    module_count = _count_modules(module_path)
    optimistic_total = module_count * timeout

    print(f"[mutation] config: {config_path}")
    print(f"[mutation] session file: {session_file}")
    print(
        f"[mutation] module path: {module_path} ({module_count} module(s) discovered)"
    )
    print(f"[mutation] pytest command: {rendered_command}")
    print(f"[mutation] timeout per mutant: {timeout:.1f}s")
    print(
        f"[mutation] baseline+exec worst-case: {_format_duration(optimistic_total)} (~{module_count} mutants)"
    )


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Display the active Cosmic Ray mutation configuration summary."
    )
    _ = parser.add_argument(
        "--config",
        default="pyproject.toml",
        type=Path,
        help="Path to the Cosmic Ray configuration file (default: pyproject.toml)",
    )
    args = parser.parse_args()
    summarize(cast(Path, args.config))


if __name__ == "__main__":
    main()
