from __future__ import annotations

import ast
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
SRC_ROOT = REPO_ROOT / "src" / "ml_playground"

_ALLOWED_SIDE_EFFECT_DIRS = {
    SRC_ROOT / "tools",
    SRC_ROOT / "experiments",
    SRC_ROOT / "analysis",
}


def _iter_python_files(base: Path) -> list[Path]:
    paths: list[Path] = []
    for path in base.rglob("*.py"):
        if not path.is_file():
            continue
        if any(allowed in path.parents for allowed in _ALLOWED_SIDE_EFFECT_DIRS):
            continue
        paths.append(path)
    return paths


def test_src_avoids_subprocess_outside_tools_and_experiments() -> None:
    offenders: list[str] = []
    for file_path in _iter_python_files(SRC_ROOT):
        tree = ast.parse(file_path.read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    if alias.name == "subprocess":
                        offenders.append(
                            f"{file_path}:L{node.lineno} -> import subprocess"
                        )
            elif isinstance(node, ast.ImportFrom):
                if node.module == "subprocess":
                    offenders.append(
                        f"{file_path}:L{node.lineno} -> from subprocess import ..."
                    )
            elif isinstance(node, ast.Call):
                if isinstance(node.func, ast.Attribute) and isinstance(
                    node.func.value, ast.Name
                ):
                    if node.func.value.id == "os" and node.func.attr == "system":
                        offenders.append(
                            f"{file_path}:L{node.lineno} -> os.system(...)"
                        )
                    if node.func.value.id == "subprocess" and node.func.attr in {
                        "run",
                        "Popen",
                        "check_call",
                        "check_output",
                        "call",
                    }:
                        offenders.append(
                            f"{file_path}:L{node.lineno} -> subprocess.{node.func.attr}(...)"
                        )

    assert not offenders, (
        "Subprocess usage is restricted to src/ml_playground/tools and src/ml_playground/experiments.\n"
        + "\n".join(offenders)
    )


def test_src_avoids_http_clients_outside_tools_and_experiments() -> None:
    offenders: list[str] = []
    for file_path in _iter_python_files(SRC_ROOT):
        tree = ast.parse(file_path.read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    if alias.name in {"requests", "httpx"}:
                        offenders.append(
                            f"{file_path}:L{node.lineno} -> import {alias.name}"
                        )
                    if alias.name.startswith("urllib"):
                        offenders.append(
                            f"{file_path}:L{node.lineno} -> import {alias.name}"
                        )
            elif isinstance(node, ast.ImportFrom):
                if node.module in {"requests", "httpx"}:
                    offenders.append(
                        f"{file_path}:L{node.lineno} -> from {node.module} import ..."
                    )
                if (node.module or "").startswith("urllib"):
                    offenders.append(
                        f"{file_path}:L{node.lineno} -> from {node.module} import ..."
                    )

    assert not offenders, (
        "HTTP client usage (requests/httpx/urllib) is restricted to src/ml_playground/tools and src/ml_playground/experiments.\n"
        + "\n".join(offenders)
    )
