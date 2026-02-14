from __future__ import annotations

import ast
from dataclasses import dataclass
from pathlib import Path

import pytest


PROJECT_ROOT = Path(__file__).resolve().parents[3]
SRC_ROOT = PROJECT_ROOT / "src"
PACKAGE_ROOT = SRC_ROOT / "ml_playground"


@dataclass(frozen=True)
class BoundaryRule:
    root: Path
    forbidden_prefixes: tuple[str, ...]


def _iter_python_files(root: Path) -> list[Path]:
    if not root.exists():
        return []
    return [path for path in root.rglob("*.py") if path.is_file()]


def _module_path_for_file(path: Path) -> str | None:
    try:
        relative = path.relative_to(SRC_ROOT)
    except ValueError:
        return None

    parts = list(relative.parts)
    if not parts or parts[0] != "ml_playground":
        return None

    if parts[-1] == "__init__.py":
        parts = parts[:-1]
    else:
        parts[-1] = parts[-1].removesuffix(".py")

    return ".".join(parts)


def _resolve_import(module_path: str, node: ast.ImportFrom) -> str | None:
    if node.level == 0:
        return node.module

    package_parts = module_path.split(".")
    package_parts = package_parts[:-1]

    parent_levels = max(node.level - 1, 0)
    if parent_levels > len(package_parts):
        return None

    base_parts = package_parts[: len(package_parts) - parent_levels]
    if node.module:
        base_parts.extend(node.module.split("."))
    return ".".join(base_parts)


def _collect_imports(path: Path) -> list[str]:
    module_path = _module_path_for_file(path)
    if module_path is None:
        return []

    try:
        parsed = ast.parse(path.read_text(encoding="utf-8"))
    except SyntaxError:
        return []

    imports: list[str] = []
    for node in ast.walk(parsed):
        if isinstance(node, ast.Import):
            for alias in node.names:
                imports.append(alias.name)
        elif isinstance(node, ast.ImportFrom):
            resolved = _resolve_import(module_path, node)
            if resolved:
                imports.append(resolved)
    return imports


@pytest.mark.parametrize(  # type: ignore[reportAny]
    "rule",
    [
        BoundaryRule(
            root=PACKAGE_ROOT / "framework",
            forbidden_prefixes=("ml_playground.tools", "ml_playground.experiments"),
        ),
        BoundaryRule(
            root=PACKAGE_ROOT / "tools",
            forbidden_prefixes=("ml_playground.experiments",),
        ),
        BoundaryRule(
            root=PACKAGE_ROOT / "experiments",
            forbidden_prefixes=(
                "ml_playground.tools",
                "ml_playground.runtime_cli",
            ),
        ),
    ],
)
def test_import_boundaries(rule: BoundaryRule) -> None:
    offenders: list[str] = []
    for path in _iter_python_files(rule.root):
        for imported in _collect_imports(path):
            if any(imported.startswith(prefix) for prefix in rule.forbidden_prefixes):
                relative = path.relative_to(PROJECT_ROOT)
                offenders.append(f"{relative}: {imported}")

    if offenders:
        message = "\n".join(sorted(offenders))
        raise AssertionError("Import boundary violations:\n" + message)
