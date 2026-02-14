from __future__ import annotations

from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
SRC_ROOT = PROJECT_ROOT / "src" / "ml_playground"
TESTS_ROOT = PROJECT_ROOT / "tests"


def _is_compliant_init(path: Path) -> bool:
    """Check if an __init__.py file is compliant with the repository policy.

    A compliant __init__.py must contain a TODO comment in the format:
    # TODO Remove <context>: <reason>
    """
    content = path.read_text(encoding="utf-8")
    # Basic check for the required TODO format as specified in IMPORT_GUIDELINES.md
    return "# TODO Remove" in content and ":" in content


def test_enforce_pep420_namespace_compliance() -> None:
    """Enforce PEP 420 namespace compliance across src and tests.

    Mandatory Rule: Every ml_playground/ and tests/ sub-package is an implicit
    namespace by default. Do not add __init__.py merely for package recognition.
    If __init__.py is required (e.g. for metadata), it must contain a TODO
    explaining the exception. See dev-guidelines/IMPORT_GUIDELINES.md.
    """
    non_compliant: list[str] = []
    compliant_exceptions: list[str] = []

    # Search in src/ml_playground and tests/
    search_paths = [SRC_ROOT, TESTS_ROOT]

    for root in search_paths:
        if not root.exists():
            continue

        for path in root.rglob("__init__.py"):
            relative_path = path.relative_to(PROJECT_ROOT)
            if _is_compliant_init(path):
                compliant_exceptions.append(str(relative_path))
            else:
                non_compliant.append(str(relative_path))

    # Fail on non-compliant ones
    assert not non_compliant, (
        "Non-compliant __init__.py files found. PEP 420 namespace policy requires "
        "removing __init__.py files unless they are approved exceptions. "
        "Approved exceptions MUST contain a '# TODO Remove <context>: <reason>' comment "
        "per dev-guidelines/IMPORT_GUIDELINES.md.\n"
        "Offending files:\n" + "\n".join(sorted(non_compliant))
    )
