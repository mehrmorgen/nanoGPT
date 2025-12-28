from __future__ import annotations


def output_text(result: object) -> str:
    stdout = getattr(result, "stdout", None) or ""
    stderr = getattr(result, "stderr", None) or ""
    output = getattr(result, "output", None) or ""
    return f"{stdout}{stderr}{output}"


def assert_traceback_free(text: str) -> None:
    assert "traceback" not in text.lower()


def assert_cli_error(result: object, *needles: str) -> None:
    text = output_text(result)
    assert_traceback_free(text)
    lowered = text.lower()
    assert any(needle.lower() in lowered for needle in needles) or "usage:" in lowered
