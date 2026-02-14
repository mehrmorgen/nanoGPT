from __future__ import annotations

from pathlib import Path


def _collect_toml_files() -> list[Path]:
    return sorted(Path(".").rglob("*.toml"))


def _check_file(path: Path) -> tuple[bool, str]:
    try:
        text = path.read_text(encoding="utf-8")
    except OSError as exc:
        return False, f"I/O error: {exc}"
    try:
        import tomllib  # type: ignore[import]

        tomllib.loads(text)
    except ImportError:  # pragma: no cover - python < 3.11
        try:
            import tomli  # type: ignore[import]

            tomli.loads(text)
        except ImportError as exc:
            return False, f"tomllib/tomli is required to parse TOML files ({exc})"
        except tomli.TOMLDecodeError as exc:  # type: ignore[attr-defined]
            return False, f"TOML syntax error: {exc}"
    except tomllib.TOMLDecodeError as exc:  # type: ignore[attr-defined]
        return False, f"TOML syntax error: {exc}"
    return True, ""


def main() -> int:
    files = _collect_toml_files()
    failures: list[str] = []
    for path in files:
        ok, message = _check_file(path)
        if not ok:
            failures.append(f"{path}: {message}")
    if failures:
        for failure in failures:
            print(failure)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
