"""Tests for configuration validation utilities."""

from __future__ import annotations

from pathlib import Path

import pytest

from ml_playground.framework.configuration.validation import (
    coerce_path,
    resolve_if_relative,
    resolve_path_strict,
)


def test_coerce_path_with_valid_inputs() -> None:
    """Test coerce_path with valid inputs."""
    # String path
    result = coerce_path("/valid/path")
    assert isinstance(result, Path)
    assert result == Path("/valid/path")

    # Path object
    path = Path("/another/path")
    result = coerce_path(path)
    assert result is path  # Should return same object

    # Relative string
    result = coerce_path("relative/path")
    assert isinstance(result, Path)
    assert result == Path("relative/path")


def test_coerce_path_with_invalid_inputs() -> None:
    """Test coerce_path with invalid types."""
    assert coerce_path(123) is None
    assert coerce_path(None) is None
    assert coerce_path([]) is None
    assert coerce_path({"path": "/value"}) is None


def test_resolve_if_relative_with_relative_paths(tmp_path: Path) -> None:
    """Test resolve_if_relative with relative paths."""
    base = tmp_path / "base"
    base.mkdir()

    # String relative path
    result = resolve_if_relative("subdir", base)
    assert result == base / "subdir"

    # Path relative path
    result = resolve_if_relative(Path("subdir"), base)
    assert result == base / "subdir"

    # Nested relative
    result = resolve_if_relative("deeply/nested/path", base)
    assert result == base / "deeply" / "nested" / "path"


def test_resolve_if_relative_with_absolute_paths(tmp_path: Path) -> None:
    """Test resolve_if_relative with absolute paths."""
    abs_path = Path("/absolute/path")
    result = resolve_if_relative(abs_path, tmp_path)
    assert result == abs_path

    # String absolute path
    result = resolve_if_relative("/another/absolute", tmp_path)
    assert result == Path("/another/absolute")


def test_resolve_if_relative_with_none() -> None:
    """Test resolve_if_relative with None value."""
    result = resolve_if_relative(None, Path("/base"))
    assert result is None


def test_resolve_path_strict_with_valid_paths(tmp_path: Path) -> None:
    """Test resolve_path_strict with valid paths."""
    # Absolute path
    result = resolve_path_strict(tmp_path / "file.txt")
    assert result == (tmp_path / "file.txt").resolve()

    # String absolute path
    result = resolve_path_strict(str(tmp_path / "file.txt"))
    assert isinstance(result, Path)
    assert result.is_absolute()


def test_resolve_path_strict_with_relative_paths() -> None:
    """Test resolve_path_strict rejects relative paths."""
    with pytest.raises(ValueError, match="Path must be absolute"):
        resolve_path_strict(Path("relative/path"))

    with pytest.raises(ValueError, match="Path must be absolute"):
        resolve_path_strict("relative/path")


def test_resolve_path_strict_with_none() -> None:
    """Test resolve_path_strict with None value."""
    with pytest.raises(ValueError, match="Path cannot be None"):
        resolve_path_strict(None)


def test_resolve_path_strict_with_invalid_type() -> None:
    """Test resolve_path_strict with invalid types."""
    with pytest.raises(ValueError, match="Expected Path or str"):
        resolve_path_strict(123)  # type: ignore[arg-type]

    with pytest.raises(ValueError, match="Expected Path or str"):
        resolve_path_strict(["not", "a", "path"])  # type: ignore[arg-type]


def test_resolve_path_strict_with_unresolvable_path() -> None:
    """Test resolve_path_strict with path that can't be resolved."""
    # Path with null byte causes OSError on resolve - make it absolute first
    invalid_path = Path("/tmp/\x00invalid")

    with pytest.raises(ValueError, match="embedded null character"):
        resolve_path_strict(invalid_path)


def test_resolve_path_strict_with_custom_resolve() -> None:
    """Test resolve_path_strict with custom resolve function."""

    def mock_resolve(path: Path) -> Path:
        return Path("/mocked") / path.name

    result = resolve_path_strict(Path("/test/path"), resolve=mock_resolve)
    assert result == Path("/mocked/path")
