from __future__ import annotations

import ast
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
SRC_ROOT = REPO_ROOT / "src" / "ml_playground"

_EXCLUDED_DIRS = {
    SRC_ROOT / "tools",
    SRC_ROOT / "analysis",
    SRC_ROOT / "experiments",
}


def _iter_python_files(base: Path) -> list[Path]:
    paths: list[Path] = []
    for path in base.rglob("*.py"):
        if not path.is_file():
            continue
        if any(excluded in path.parents for excluded in _EXCLUDED_DIRS):
            continue
        paths.append(path)
    return paths


def test_src_does_not_use_print_or_debug_breakpoints() -> None:
    offenders: list[str] = []
    for file_path in _iter_python_files(SRC_ROOT):
        tree = ast.parse(file_path.read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if isinstance(node, ast.Call):
                if isinstance(node.func, ast.Name) and node.func.id == "print":
                    offenders.append(f"{file_path}:L{node.lineno} -> print(...)")
                elif isinstance(node.func, ast.Name) and node.func.id == "breakpoint":
                    offenders.append(f"{file_path}:L{node.lineno} -> breakpoint(...)")
                elif (
                    isinstance(node.func, ast.Attribute)
                    and node.func.attr == "set_trace"
                    and isinstance(node.func.value, ast.Name)
                    and node.func.value.id == "pdb"
                ):
                    offenders.append(
                        f"{file_path}:L{node.lineno} -> pdb.set_trace(...)"
                    )

    assert not offenders, (
        "Do not commit print()/breakpoint()/pdb.set_trace() in src/ml_playground (excluding tools/analysis/experiments).\n"
        + "\n".join(offenders)
    )


def test_src_does_not_use_wildcard_imports() -> None:
    offenders: list[str] = []
    for file_path in _iter_python_files(SRC_ROOT):
        tree = ast.parse(file_path.read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom) and any(
                alias.name == "*" for alias in node.names
            ):
                offenders.append(
                    f"{file_path}:L{node.lineno} -> from {node.module} import *"
                )

    assert not offenders, (
        "Wildcard imports are forbidden in src/ml_playground (excluding tools/analysis/experiments).\n"
        + "\n".join(offenders)
    )
