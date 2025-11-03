from __future__ import annotations

import ast
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
SRC_ROOT = REPO_ROOT / "src" / "ml_playground"
TESTS_ROOT = REPO_ROOT / "tests"
RUNTIME_ROOT = SRC_ROOT / "runtime"


def _iter_python_files(base: Path) -> list[Path]:
    return [path for path in base.rglob("*.py") if path.is_file()]


def _collect_private_import_violations(path: Path) -> list[str]:
    violations: list[str] = []
    tree = ast.parse(path.read_text(encoding="utf-8"))
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom):
            module = node.module or ""
            if module.startswith("ml_playground"):
                for alias in node.names:
                    if alias.name.startswith("_"):
                        violations.append(
                            f"L{node.lineno}: from {module} import {alias.name}"
                        )
        elif isinstance(node, ast.Attribute) and node.attr.startswith("_"):
            root = node.value
            while isinstance(root, ast.Attribute):
                root = root.value
            if isinstance(root, ast.Name) and root.id.startswith("ml_playground"):
                violations.append(
                    f"L{node.lineno}: private attribute access {ast.unparse(node)}"
                )
    return violations


def test_tests_do_not_import_private_runtime_symbols() -> None:
    offenders: list[str] = []
    for file_path in _iter_python_files(TESTS_ROOT):
        violations = _collect_private_import_violations(file_path)
        if violations:
            offenders.append(f"{file_path} ->\n  - " + "\n  - ".join(violations))
    assert not offenders, (
        "Found test modules importing private/internal runtime APIs.\n"
        + "\n\n".join(offenders)
    )


def test_runtime_modules_do_not_import_tooling_namespaces() -> None:
    offenders: list[str] = []
    for file_path in _iter_python_files(RUNTIME_ROOT):
        tree = ast.parse(file_path.read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom) and (node.module or "").startswith(
                "ml_playground.tools"
            ):
                offenders.append(
                    f"{file_path}:L{node.lineno} -> from {node.module} import "
                    + ", ".join(alias.name for alias in node.names)
                )
            elif isinstance(node, ast.Import) and any(
                name.name.startswith("ml_playground.tools") for name in node.names
            ):
                offenders.append(
                    f"{file_path}:L{node.lineno} -> import "
                    + ", ".join(name.name for name in node.names)
                )
    assert not offenders, (
        "Runtime modules must not depend on tooling namespaces.\n"
        + "\n".join(offenders)
    )
