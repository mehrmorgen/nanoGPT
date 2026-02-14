from __future__ import annotations

from pathlib import Path


_FORBIDDEN_TOKENS = (
    ".__file__ =",
    ".create_tokenizer =",
    ".requests.get =",
)


def test_shakespeare_preparer_tests_avoid_module_globals() -> None:
    test_path = Path(__file__).with_name("test_shakespeare_preparer.py")
    text = test_path.read_text(encoding="utf-8")
    offenders = [token for token in _FORBIDDEN_TOKENS if token in text]
    assert not offenders, (
        "Found module-global reassignment in shakespeare preparer tests: "
        + ", ".join(offenders)
    )
