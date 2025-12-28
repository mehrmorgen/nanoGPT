from pathlib import Path
from typing import Any

# Tokens that are forbidden in tests
FORBIDDEN_TOKENS = [
    "monkeypatch",
    "pytest.MonkeyPatch",
    "unittest.mock",
    "from unittest import mock",
    "pytest_mock",
    "MagicMock",
    "patch(",
]


def _is_token_outside_strings(line: str, token: str) -> bool:
    """Check if token exists in line outside of string literals."""
    in_single = False
    in_double = False
    token_len = len(token)

    i = 0
    while i <= len(line) - token_len:
        char = line[i]

        if char == '"' and (i == 0 or line[i - 1] != "\\"):
            in_double = not in_double
        elif char == "'" and (i == 0 or line[i - 1] != "\\"):
            in_single = not in_single

        if not in_single and not in_double:
            if line[i : i + token_len] == token:
                return True
        i += 1
    return False


def test_no_mocks_in_tests(request: Any) -> None:
    """Regression test to ensure no mocking libraries are used in tests."""
    root_dir = Path(request.config.rootdir)
    tests_dir = root_dir / "tests"

    violations: list[str] = []

    for py_file in tests_dir.rglob("*.py"):
        # Skip this test file itself
        if py_file.name == "test_no_mocks.py":
            continue

        try:
            content = py_file.read_text(encoding="utf-8")
            lines = content.splitlines()

            for line_num, line in enumerate(lines, 1):
                line_stripped = line.strip()
                if not line_stripped or line_stripped.startswith("#"):
                    continue

                for token in FORBIDDEN_TOKENS:
                    if token in line and _is_token_outside_strings(line, token):
                        violations.append(
                            f"{py_file.relative_to(root_dir)}:{line_num}: Found forbidden token '{token}'"
                        )
        except Exception as e:
            violations.append(f"Could not read {py_file}: {e}")

    if violations:
        msg = "\n".join(["Found forbidden mocking patterns:"] + violations)
        raise AssertionError(msg)
