"""Regression guards against prohibited cross-imports between runtime, tools, and commands."""

from __future__ import annotations

import ast
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
SRC_ROOT = PROJECT_ROOT / "src" / "ml_playground"


def _iter_python_files(root: Path) -> list[Path]:
    return [p for p in root.rglob("*.py") if p.is_file()]


def _collect_imports(path: Path) -> set[str]:
    tree = ast.parse(path.read_text())
    seen: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                seen.add(alias.name)
        elif isinstance(node, ast.ImportFrom):
            module = node.module or ""
            seen.add(module)
    return seen


def test_runtime_sources_do_not_import_tools() -> None:
    """Runtime package must not import tools anywhere in source."""
    runtime_root = SRC_ROOT / "runtime"
    for path in _iter_python_files(runtime_root):
        imports = _collect_imports(path)
        assert all(not imp.startswith("ml_playground.tools") for imp in imports), (
            f"Prohibited tools import found in {path}"
        )


def test_tools_sources_do_not_import_runtime() -> None:
    """Tools package must not import runtime anywhere in source."""
    tools_root = SRC_ROOT / "tools"
    for path in _iter_python_files(tools_root):
        imports = _collect_imports(path)
        assert all(not imp.startswith("ml_playground.runtime") for imp in imports), (
            f"Prohibited runtime import found in {path}"
        )


def test_command_modules_do_not_import_peers() -> None:
    """Each CLI command module should avoid importing sibling command modules."""
    prefix = "ml_playground.tools.cli.commands"
    commands_root = SRC_ROOT / "tools" / "cli" / "commands"
    command_files = [p for p in commands_root.glob("*.py") if p.name != "__init__.py"]

    for path in command_files:
        imports = _collect_imports(path)
        for imp in imports:
            if imp.startswith(prefix):
                # Allow self or top-level package only
                assert imp in {
                    prefix,
                    f"{prefix}.{path.stem}",
                }, f"{path} imports peer command module: {imp}"
