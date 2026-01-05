from __future__ import annotations

from pathlib import Path

import pytest

from ml_playground.configuration.models import (
    _resolve_path_strict,
    _resolve_if_relative,
    ExperienceStorageConfig,
)


def test_resolve_path_strict_raises_on_invalid_path(tmp_path: Path) -> None:
    invalid = tmp_path / "nonexistent" / "file.txt"
    with pytest.raises(ValueError, match="Invalid path"):
        _resolve_path_strict(invalid)


def test_resolve_if_relative_returns_absolute_when_relative(tmp_path: Path) -> None:
    relative = "data/file.txt"
    result = _resolve_if_relative(relative, tmp_path)
    assert result.is_absolute()
    assert result == (tmp_path / "data/file.txt").resolve()


def test_resolve_if_relative_returns_absolute_when_path_relative(
    tmp_path: Path,
) -> None:
    relative = Path("data/file.txt")
    result = _resolve_if_relative(relative, tmp_path)
    assert result.is_absolute()
    assert result == (tmp_path / "data/file.txt").resolve()


def test_resolve_if_relative_returns_absolute_when_absolute(tmp_path: Path) -> None:
    absolute = tmp_path / "file.txt"
    result = _resolve_if_relative(absolute, tmp_path)
    assert result == absolute.resolve()


def test_resolve_if_relative_returns_string_when_absolute_string(
    tmp_path: Path,
) -> None:
    absolute = str(tmp_path / "file.txt")
    result = _resolve_if_relative(absolute, tmp_path)
    assert result == Path(absolute).resolve()


def test_experience_storage_json_file_requires_path(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="experience storage path is required"):
        ExperienceStorageConfig(strategy="json_file", path=None)


def test_experience_storage_json_file_path_must_be_file_not_dir(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="must point to a file"):
        ExperienceStorageConfig(strategy="json_file", path=tmp_path)


def test_experience_storage_json_file_path_must_not_be_empty_string(
    tmp_path: Path,
) -> None:
    with pytest.raises(ValueError, match="must point to a file"):
        ExperienceStorageConfig(strategy="json_file", path=tmp_path / "")


def test_experience_storage_memory_strategy_warns_on_path(tmp_path: Path) -> None:
    logger = _FakeLogger()
    cfg = ExperienceStorageConfig(strategy="memory", path=tmp_path / "ignore.json")
    cfg.logger = logger  # type: ignore[attr-defined]
    cfg._validate_strategy()
    assert any("ignored for strategy memory" in msg for msg in logger.warnings)


class _FakeLogger:
    def __init__(self) -> None:
        self.warnings: list[str] = []

    def warning(self, msg: str, *args: object, **kwargs: object) -> None:
        self.warnings.append(msg)
