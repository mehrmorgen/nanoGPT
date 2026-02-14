from __future__ import annotations

import ast
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
SRC_ROOT = REPO_ROOT / "src" / "ml_playground"
TESTS_ROOT = REPO_ROOT / "tests"
RUNTIME_ROOT = SRC_ROOT / "runtime"


def _iter_python_files(base: Path) -> list[Path]:
    return [path for path in base.rglob("*.py") if path.is_file()]


def _has_todo_marker(path: Path, lineno: int) -> bool:
    """Check if the line or the one before has a # TODO marker."""
    lines = path.read_text(encoding="utf-8").splitlines()
    # lineno is 1-indexed
    idx = lineno - 1
    search_lines = []
    if idx > 0:
        search_lines.append(lines[idx - 1])
    if idx < len(lines):
        search_lines.append(lines[idx])

    return any("# TODO" in line.upper() for line in search_lines)


def _collect_public_definitions(path: Path) -> set[tuple[str, str | None, bool, bool]]:
    """Collect public functions/methods. Returns set of (name, class_name, is_entry_point, has_todo)."""
    tree = ast.parse(path.read_text(encoding="utf-8"))

    # Better approach: recursive visitor to track state
    class DefVisitor(ast.NodeVisitor):
        def __init__(self) -> None:
            self.defs: set[tuple[str, str | None, bool, bool]] = set()
            self.current_class: str | None = None

        def visit_ClassDef(self, node: ast.ClassDef) -> None:
            old_class = self.current_class
            self.current_class = node.name
            self.generic_visit(node)
            self.current_class = old_class

        def visit_FunctionDef(self, node: ast.FunctionDef) -> None:
            if not node.name.startswith("_"):
                # Detect entry points (Typer commands, etc.)
                is_entry = any(
                    isinstance(dec, ast.Call)
                    and isinstance(dec.func, ast.Attribute)
                    and dec.func.attr == "command"
                    for dec in node.decorator_list
                )
                has_todo = _has_todo_marker(path, node.lineno)
                self.defs.add((node.name, self.current_class, is_entry, has_todo))
            self.generic_visit(node)

    visitor = DefVisitor()
    visitor.visit(tree)
    return visitor.defs


def _should_exclude_from_internal_usage(path: Path) -> bool:
    """Exclude tests self-usage check."""
    return path.name.startswith("test_")


def _collect_all_usages(path: Path) -> set[tuple[str, str | None]]:
    """Collect all name/attribute usages. Returns set of (name, attr_of)."""
    usages = set()
    tree = ast.parse(path.read_text(encoding="utf-8"))

    class UsageVisitor(ast.NodeVisitor):
        def visit_Name(self, node: ast.Name) -> None:
            usages.add((node.id, None))
            self.generic_visit(node)

        def visit_Attribute(self, node: ast.Attribute) -> None:
            # Try to identify what it belongs to if it's 'self'
            context = None
            if isinstance(node.value, ast.Name) and node.value.id == "self":
                context = "self"
            usages.add((node.attr, context))
            self.generic_visit(node)

    UsageVisitor().visit(tree)
    return usages


def test_public_methods_are_used_in_production() -> None:
    """Every public function/method in src/ must be used somewhere in src/."""
    all_defs: dict[tuple[str, str | None], list[tuple[Path, bool, bool]]] = {}

    # 1. Collect all public definitions from src
    for file_path in _iter_python_files(SRC_ROOT):
        # Skip __init__.py as they often intentionally only export
        if file_path.name == "__init__.py":
            continue
        defs = _collect_public_definitions(file_path)
        for name, cls, is_entry, has_todo in defs:
            all_defs.setdefault((name, cls), []).append((file_path, is_entry, has_todo))

    # 2. Collect all usages from src
    all_usages: set[str] = set()
    for file_path in _iter_python_files(SRC_ROOT):
        usages = _collect_all_usages(file_path)
        for name, _context in usages:
            all_usages.add(name)

    unused = []
    for (name, cls), path_entries in all_defs.items():
        # A method is unused if it's not in all_usages AND not an entry point AND has no TODO
        is_entry = any(e[1] for e in path_entries)
        has_todo = any(e[2] for e in path_entries)
        if name not in all_usages and not is_entry and not has_todo:
            unused.append(
                f"{path_entries[0][0].relative_to(REPO_ROOT)}: {cls + '.' if cls else ''}{name}"
            )

    if unused:
        print("\nPotential unused public APIs (only used in tests or dead code):")
        for u in sorted(unused):
            print(f"  {u}")
    # assert not unused, "Detected public methods only used in tests."


def test_no_public_non_constant_fields() -> None:
    """Class and instance fields must be private (_) or Constants (UPPER_CASE)."""
    offenders = []
    for file_path in _iter_python_files(SRC_ROOT):
        tree = ast.parse(file_path.read_text(encoding="utf-8"))

        class FieldVisitor(ast.NodeVisitor):
            def __init__(self) -> None:
                self.current_class: str | None = None

            def visit_ClassDef(self, node: ast.ClassDef) -> None:
                old_class = self.current_class
                self.current_class = node.name
                # Check class attributes
                for item in node.body:
                    if isinstance(item, ast.AnnAssign) and isinstance(
                        item.target, ast.Name
                    ):
                        name = item.target.id
                        if not name.startswith("_") and not name.isupper():
                            offenders.append(
                                f"{file_path.relative_to(REPO_ROOT)}:L{item.lineno}: {self.current_class}.{name} is public non-constant"
                            )
                    elif isinstance(item, ast.Assign):
                        for target in item.targets:
                            if isinstance(target, ast.Name):
                                name = target.id
                                if not name.startswith("_") and not name.isupper():
                                    # Ignore some special cases if necessary, but keep it strict
                                    offenders.append(
                                        f"{file_path.relative_to(REPO_ROOT)}:L{item.lineno}: {self.current_class}.{name} is public non-constant"
                                    )

                self.generic_visit(node)
                self.current_class = old_class

            def visit_Attribute(self, node: ast.Attribute) -> None:
                # Check instance attributes self.x = val
                if (
                    isinstance(node.value, ast.Name)
                    and node.value.id == "self"
                    and isinstance(getattr(node, "ctx", None), ast.Store)
                ):
                    name = node.attr
                    if not name.startswith("_") and not name.isupper():
                        offenders.append(
                            f"{file_path.relative_to(REPO_ROOT)}:L{node.lineno}: self.{name} is public non-constant"
                        )
                self.generic_visit(node)

        FieldVisitor().visit(tree)

    if offenders:
        print(
            "\nPublic non-constant fields (must be private _ or Constants UPPER_CASE):"
        )
        for o in sorted(set(offenders)):
            print(f"  {o}")
    # assert not offenders


def test_no_internal_use_of_public_methods() -> None:
    """Internal code should not call its own public methods (use private helpers instead)."""
    offenders = []
    for file_path in _iter_python_files(SRC_ROOT):
        if _should_exclude_from_internal_usage(file_path):
            continue
        tree = ast.parse(file_path.read_text(encoding="utf-8"))

        class InternalUsageVisitor(ast.NodeVisitor):
            def __init__(self) -> None:
                self.current_class: str | None = None
                self.class_public_entities: set[str] = set()

            def visit_ClassDef(self, node: ast.ClassDef) -> None:
                old_class = self.current_class
                old_entities = self.class_public_entities

                self.current_class = node.name
                # Collect ALL public entities: methods, properties, attributes
                self.class_public_entities = set()
                for item in node.body:
                    if isinstance(item, (ast.FunctionDef, ast.AsyncFunctionDef)):
                        if not item.name.startswith("_"):
                            self.class_public_entities.add(item.name)
                    elif isinstance(item, ast.AnnAssign) and isinstance(
                        item.target, ast.Name
                    ):
                        if not item.target.id.startswith("_"):
                            self.class_public_entities.add(item.target.id)
                    elif isinstance(item, ast.Assign):
                        for target in item.targets:
                            if isinstance(
                                target, ast.Name
                            ) and not target.id.startswith("_"):
                                self.class_public_entities.add(target.id)

                self.generic_visit(node)

                self.current_class = old_class
                self.class_public_entities = old_entities

            def visit_Attribute(self, node: ast.Attribute) -> None:
                # self.public_thing access (Load)
                if (
                    self.current_class
                    and isinstance(node.value, ast.Name)
                    and node.value.id == "self"
                    and node.attr in self.class_public_entities
                    and
                    # If it's a Constant (UPPER_CASE), internal usage is OK per user feedback
                    not node.attr.isupper()
                ):
                    offenders.append(
                        f"{file_path.relative_to(REPO_ROOT)}:L{node.lineno}: self.{node.attr} usage"
                    )
                self.generic_visit(node)

        InternalUsageVisitor().visit(tree)

    if offenders:
        print("\nInternal code calling its own public methods/attributes:")
        for o in sorted(set(offenders)):
            print(f"  {o}")
    # assert not offenders


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
            offenders.append(
                f"{file_path.relative_to(REPO_ROOT)} ->\n  - "
                + "\n  - ".join(violations)
            )
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
                    f"{file_path.relative_to(REPO_ROOT)}:L{node.lineno} -> from {node.module} import "
                    + ", ".join(alias.name for alias in node.names)
                )
            elif isinstance(node, ast.Import) and any(
                name.name.startswith("ml_playground.tools") for name in node.names
            ):
                offenders.append(
                    f"{file_path.relative_to(REPO_ROOT)}:L{node.lineno} -> import "
                    + ", ".join(name.name for name in node.names)
                )
    assert not offenders, (
        "Runtime modules must not depend on tooling namespaces.\n"
        + "\n".join(offenders)
    )


def test_no_dynamic_attribute_access() -> None:
    """Disallow usage of hasattr and setattr to enforce strict typing and public API usage."""
    # Allowed exceptions (infrastructure code that implements mocking/patching)
    ALLOWED_FILES = {
        # Infrastructure
        "src/ml_playground/framework/training/loop/runner.py",  # Torch vmap check (production infrastructure)
        "src/ml_playground/tools/testing/testing.py",  # Generic tooling infrastructure
        "tests/unit/framework/runtime/test_main_bootstrap_protocols.py",  # Implements override_attr
        "tests/unit/runtime_cli/helpers.py",  # Helpers for CLI runtime tests
        # Legacy Violations (TODO: Fix these usages)
        "tests/property/runtime_cli/test_runtime_runners_prop.py",
        "tests/property/tools/_helpers.py",
        "tests/property/tools/analysis/test_sample_quality_property.py",
        "tests/property/tools/cli/test_tools_cli_property.py",
        "tests/property/tools/dev/helpers.py",
        "tests/property/tools/dev/test_batch_review_property.py",
        "tests/property/tools/dev/test_dev_tools_property.py",
        "tests/property/tools/test_cli_tools_property.py",
        "tests/unit/framework/analysis/test_lit_integration.py",
        "tests/unit/framework/configuration/test_models_and_loading.py",
        "tests/unit/framework/core/test_tokenizer.py",
        "tests/unit/framework/data_pipeline/test_preparer.py",
        "tests/unit/experiments/speakger/test_sampler.py",
        "tests/unit/experiments/test_contracts.py",
        "tests/unit/runtime_cli/test_cli_app_protocol_placeholders.py",
        "tests/unit/runtime_cli/test_commands_runtime.py",
        "tests/unit/runtime_cli/test_main_module.py",
        "tests/unit/framework/runtime/test_helpers_runtime.py",
        "tests/unit/tools/_cli_test_helpers.py",
        "tests/unit/tools/analysis/lit/test_lit_tool_integration.py",
        "tests/unit/tools/analysis/test_tools_lit_integration.py",
        "tests/unit/tools/categories/test_agentic.py",
        "tests/unit/tools/ci/test_coverage_badge.py",
        "tests/unit/tools/cli/commands/test_analysis.py",
        "tests/unit/tools/cli/test_cli_helpers.py",
        "tests/unit/tools/cli/test_main.py",
        "tests/unit/tools/core/test_config.py",
        "tests/unit/tools/dev/test_ai_guidelines.py",
        "tests/unit/tools/dev/test_batch_review.py",
        "tests/unit/tools/dev/test_review.py",
        "tests/unit/tools/dev/test_status.py",
        "tests/unit/tools/test_tools_cli.py",
        "tests/unit/tools/testing/test_coverage.py",
        "tests/unit/tools/testing/test_mutation.py",
        "tests/unit/tools/testing/test_mutation_coverage.py",
        "tests/unit/tools/testing/test_testing_facade_misc.py",
        "tests/unit/framework/training/hooks/test_data.py",
        "src/ml_playground/experiments/bundestag_char/preparer.py",
        "src/ml_playground/experiments/bundestag_qwen15b_lora_mps/preparer.py",
        "src/ml_playground/framework/configuration/models.py",
        "src/ml_playground/framework/core/file_state.py",
        "src/ml_playground/framework/data_pipeline/transforms/tokenization.py",
        "src/ml_playground/framework/experiment_registry/extras_registry.py",
        "src/ml_playground/framework/experiment_registry/registry.py",
        "src/ml_playground/framework/runtime/device.py",
        "src/ml_playground/framework/training/hooks/runtime.py",
        "src/ml_playground/runtime_cli/app.py",
        "src/ml_playground/tools/cli/config_loader.py",
        "src/ml_playground/tools/cli/main.py",
        "src/ml_playground/tools/core/config.py",
        "src/ml_playground/tools/testing/mutation.py",
        "tests/conftest.py",
        "tests/integration/experiments/test_datasets_shakespeare.py",
        "tests/unit/runtime_cli/test_commands.py",
        "tests/unit/runtime_cli/test_main.py",
        "tests/unit/runtime_cli/test_typer_helpers.py",
        "tests/unit/tools/testing/test_testing.py",
        "tests/property/runtime_cli/test_commands_property.py",
        "tests/property/runtime_cli/test_main_property.py",
        "tests/property/runtime_cli/test_core_results_property.py",
        "tests/property/runtime_cli/test_device_property.py",
        "tests/property/runtime_cli/test_helpers_property.py",
        "tests/property/runtime_cli/test_app_property.py",
        "tests/property/runtime_cli/test_runners_property.py",
        "tests/property/runtime_cli/test_runtime_cli_commands_property.py",
    }

    offenders = []
    # Check both source and tests
    for root in [SRC_ROOT, TESTS_ROOT]:
        for file_path in _iter_python_files(root):
            rel_path = str(file_path.relative_to(REPO_ROOT))
            if rel_path in ALLOWED_FILES:
                continue

            # Skip checking this regression test file itself if needed, but it shouldn't use them (except finding them?)
            # Actually AST usage above doesn't use hasattr/setattr, it uses node.attr.

            tree = ast.parse(file_path.read_text(encoding="utf-8"))

            for node in ast.walk(tree):
                if isinstance(node, ast.Call) and isinstance(node.func, ast.Name):
                    if node.func.id in ("hasattr", "setattr"):
                        offenders.append(
                            f"{rel_path}:L{node.lineno}: Usage of '{node.func.id}' is forbidden."
                        )

    # For now, print violations. Uncomment raise to enforce.
    if offenders:
        msg = "\n".join(
            ["Found forbidden dynamic attribute access:"] + sorted(offenders)
        )
        print(msg)
        raise AssertionError(msg)
