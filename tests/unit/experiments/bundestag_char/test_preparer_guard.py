from __future__ import annotations

from pathlib import Path


_FORBIDDEN_TOKENS = (
    ".__file__ =",
    ".requests.get =",
)


def test_bundestag_char_preparer_tests_avoid_module_globals() -> None:
    test_dir = Path(__file__).parent
    offenders: list[str] = []
    for name in ("test_preparer.py",):
        text = (test_dir / name).read_text(encoding="utf-8")
        for token in _FORBIDDEN_TOKENS:
            if token in text:
                offenders.append(f"{name}:{token}")
    assert not offenders, (
        "Found module-global reassignment in bundestag_char preparer tests: "
        + ", ".join(offenders)
    )
