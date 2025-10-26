"""Shared utilities for uv-backed task CLIs."""

from __future__ import annotations

import os
import shlex
import shutil
import subprocess
from importlib import resources
from pathlib import Path
from subprocess import CompletedProcess
from collections.abc import Iterable, Mapping
from typing import cast

import tomllib

import typer


def _discover_root() -> Path:
    """Locate the repository root by walking up to the first `pyproject.toml`."""

    def expand_chain(start: Path, seen: set[Path], order: list[Path]) -> None:
        for path in (start, *start.parents):
            if path in seen:
                continue
            seen.add(path)
            order.append(path)

    seen: set[Path] = set()
    ordered: list[Path] = []
    expand_chain(Path(__file__).resolve(), seen, ordered)
    expand_chain(Path.cwd(), seen, ordered)

    for path in ordered:
        if (path / "pyproject.toml").exists():
            return path

    return Path(__file__).resolve().parents[1]


ROOT = _discover_root()
PKG = "ml_playground"
PKG_PATH = ROOT / "src" / PKG
PYTEST_BASE = ["-q", "-n", "auto", "-W", "error", "--strict-markers", "--strict-config"]
PRE_COMMIT_CONFIG = ROOT / ".githooks" / ".pre-commit-config.yaml"


def _as_mapping(obj: object) -> Mapping[str, object] | None:
    if isinstance(obj, Mapping):
        return cast(Mapping[str, object], obj)
    return None


def _load_cache_settings() -> tuple[Path, dict[str, str]]:
    cache_dir = ROOT / ".cache"
    cache_env: dict[str, str] = {}

    try:
        with (ROOT / "pyproject.toml").open("rb") as fp:
            pyproject_raw: object = tomllib.load(fp)
    except FileNotFoundError:  # pragma: no cover - defensive
        pyproject_raw = {}

    pyproject_data = _as_mapping(pyproject_raw)
    if pyproject_data is None:
        return cache_dir, cache_env

    tool_section = _as_mapping(pyproject_data.get("tool"))
    if tool_section is None:
        return cache_dir, cache_env

    ml_playground_section = _as_mapping(tool_section.get("ml_playground"))
    if ml_playground_section is None:
        return cache_dir, cache_env

    cache_config = _as_mapping(ml_playground_section.get("cache"))
    if cache_config is None:
        return cache_dir, cache_env

    base_dir_obj = cache_config.get("base_dir")
    if isinstance(base_dir_obj, str) and base_dir_obj:
        cache_dir = (ROOT / base_dir_obj).resolve()

    env_mapping = _as_mapping(cache_config.get("env"))
    if env_mapping is not None:
        for key_obj, value_obj in env_mapping.items():
            if not isinstance(value_obj, str):
                continue
            resolved = (ROOT / value_obj).resolve()
            resolved_str = str(resolved)
            cache_env[key_obj] = resolved_str
            _ = os.environ.setdefault(key_obj, resolved_str)

    return cache_dir, cache_env


CACHE_DIR, _CACHE_ENV = _load_cache_settings()

LIT_VENV = ROOT / ".venv312"
LIT_REQUIREMENTS = resources.files("ml_playground.analysis.lit") / "requirements.txt"


class CommandError(RuntimeError):
    """Raised when an invoked subprocess fails."""


def _echo_command(command: list[str]) -> None:
    formatted = " ".join(shlex.quote(arg) for arg in command)
    typer.echo(f"$ {formatted}")


def _run(
    command: list[str], *, env: dict[str, str] | None = None, check: bool = True
) -> CompletedProcess[str]:
    _echo_command(command)
    run_env = os.environ.copy()
    run_env.update(_CACHE_ENV)
    if env:
        run_env.update(env)
    result = subprocess.run(command, cwd=ROOT, env=run_env, text=True)
    if check and result.returncode != 0:
        raise CommandError(f"Command failed with exit code {result.returncode}")
    return result


def uv(
    *args: str, env: dict[str, str] | None = None, check: bool = True
) -> CompletedProcess[str]:
    return _run(["uv", *args], env=env, check=check)


def uv_run(
    *args: str,
    python: str | None = None,
    env: dict[str, str] | None = None,
    check: bool = True,
    no_project: bool = False,
) -> CompletedProcess[str]:
    command: list[str] = ["uv", "run"]
    if no_project:
        command.append("--no-project")
    else:
        command.extend(["--project", str(ROOT)])
    if python:
        command.extend(["--python", python])
    command.extend(args)
    return _run(command, env=env, check=check)


def ensure_cache_dirs(*subdirs: str) -> None:
    for subdir in subdirs:
        (CACHE_DIR / subdir).mkdir(parents=True, exist_ok=True)
    for path in _CACHE_ENV.values():
        Path(path).mkdir(parents=True, exist_ok=True)


def forwarded_args(args: object) -> list[str]:
    if args is None:
        return []

    try:
        from typer.models import ArgumentInfo, OptionInfo  # type: ignore

        if isinstance(args, (ArgumentInfo, OptionInfo)):
            return []
    except ImportError:  # pragma: no cover - Typer not installed
        pass

    if isinstance(args, str):
        return [args]

    if isinstance(args, Iterable):
        return [str(item) for item in args]

    return [str(args)]


def pytest_command(extra: list[str] | None = None) -> list[str]:
    return ["pytest", *PYTEST_BASE, *(extra or [])]


def remove_path(path: Path) -> None:
    if not path.exists():
        return
    if path.is_dir():
        shutil.rmtree(path, ignore_errors=True)
    else:
        path.unlink(missing_ok=True)  # type: ignore[arg-type]


def coverage_file() -> Path:
    return CACHE_DIR / "coverage" / "coverage.sqlite"


def coverage_fragments(dest: Path) -> list[Path]:
    dest.parent.mkdir(parents=True, exist_ok=True)
    return [p for p in dest.parent.glob("coverage.sqlite.*") if p.name != dest.name]


def cosmic_ray_session_file() -> Path:
    return CACHE_DIR / "cosmic-ray" / "session.sqlite"


def lit_python() -> Path:
    if os.name == "nt":
        return LIT_VENV / "Scripts" / "python.exe"
    return LIT_VENV / "bin" / "python"
