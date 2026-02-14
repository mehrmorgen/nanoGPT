from __future__ import annotations

import ast
import sys
from pathlib import Path
from typing import List

# Define architectural boundaries (namespace: forbidden_imports)
BOUNDARIES = {
    "ml_playground.tools": {"ml_playground.experiments"},
    "ml_playground.framework": {"ml_playground.experiments"},
    "ml_playground.core": {
        "ml_playground.framework",
        "ml_playground.tools",
        "ml_playground.experiments",
    },
}


def get_module_path(path: Path, root: Path) -> str:
    """Convert file path to dot-separated module path."""
    rel = path.relative_to(root)
    return ".".join(rel.with_suffix("").parts)


def check_file(path: Path, root: Path) -> List[str]:
    """Check a single file for boundary violations."""
    module_path = get_module_path(path, root)
    violations = []

    # Find active boundary for this module
    active_boundaries = []
    for namespace, forbidden in BOUNDARIES.items():
        if module_path.startswith(namespace):
            active_boundaries.append(forbidden)

    if not active_boundaries:
        return []

    try:
        tree = ast.parse(path.read_text(encoding="utf-8"))
    except Exception as e:
        return [f"{path}: Could not parse AST: {e}"]

    for node in ast.walk(tree):
        imported_modules: List[str] = []
        if isinstance(node, ast.Import):
            for alias in node.names:
                imported_modules.append(alias.name)
        elif isinstance(node, ast.ImportFrom):
            if node.module:
                imported_modules.append(node.module)

        for imp in imported_modules:
            for forbidden_set in active_boundaries:
                for forbidden in forbidden_set:
                    if imp == forbidden or imp.startswith(forbidden + "."):
                        violations.append(
                            f"{path}: Boundary violation: '{module_path}' imports '{imp}' (forbidden: '{forbidden}')"
                        )

    return violations


def main() -> int:
    root = Path("src").resolve()
    all_violations = []

    for py_file in root.rglob("*.py"):
        all_violations.extend(check_file(py_file, root))

    if all_violations:
        print("Found architectural boundary violations:")
        for v in all_violations:
            print(f"  {v}")
        return 1

    print("Architectural boundaries verified. No violations found.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
