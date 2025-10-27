"""Filesystem utilities with dependency injection support."""

from __future__ import annotations

import json
import shutil
from pathlib import Path
from typing import Any, List, Protocol, Union


class FilesystemOperations(Protocol):
    """Protocol for filesystem operations."""

    def exists(self, path: Union[str, Path]) -> bool:
        """Check if path exists."""
        ...

    def is_file(self, path: Union[str, Path]) -> bool:
        """Check if path is a file."""
        ...

    def is_dir(self, path: Union[str, Path]) -> bool:
        """Check if path is a directory."""
        ...

    def unlink(self, path: Union[str, Path]) -> None:
        """Remove a file."""
        ...

    def rmtree(self, path: Union[str, Path]) -> None:
        """Remove a directory tree."""
        ...

    def mkdir(self, path: Union[str, Path], parents: bool = False) -> None:
        """Create a directory."""
        ...

    def iterdir(self, path: Union[str, Path]) -> List[Path]:
        """List directory contents."""
        ...

    def glob(self, path: Union[str, Path], pattern: str) -> List[Path]:
        """Glob pattern matching."""
        ...

    def rglob(self, path: Union[str, Path], pattern: str) -> List[Path]:
        """Recursive glob pattern matching."""
        ...

    def stat_size(self, path: Union[str, Path]) -> int:
        """Get file size."""
        ...

    def read_text(self, path: Union[str, Path]) -> str:
        """Read file content as text."""
        ...

    def write_text(self, path: Union[str, Path], content: str) -> None:
        """Write text content to file."""
        ...


class JsonOperations(Protocol):
    """Protocol for JSON operations."""

    def load(self, path: Union[str, Path]) -> Any:
        """Load JSON from file."""
        ...

    def dump(self, data: Any, path: Union[str, Path]) -> None:
        """Dump JSON to file."""
        ...


class RealFilesystemOperations:
    """Real filesystem operations implementation."""

    def exists(self, path: Union[str, Path]) -> bool:
        """Check if path exists."""
        return Path(path).exists()

    def is_file(self, path: Union[str, Path]) -> bool:
        """Check if path is a file."""
        return Path(path).is_file()

    def is_dir(self, path: Union[str, Path]) -> bool:
        """Check if path is a directory."""
        return Path(path).is_dir()

    def unlink(self, path: Union[str, Path]) -> None:
        """Remove a file."""
        Path(path).unlink(missing_ok=True)

    def rmtree(self, path: Union[str, Path]) -> None:
        """Remove a directory tree."""
        shutil.rmtree(path, ignore_errors=True)

    def mkdir(self, path: Union[str, Path], parents: bool = False) -> None:
        """Create a directory."""
        Path(path).mkdir(parents=parents, exist_ok=True)

    def iterdir(self, path: Union[str, Path]) -> List[Path]:
        """List directory contents."""
        return list(Path(path).iterdir())

    def glob(self, path: Union[str, Path], pattern: str) -> List[Path]:
        """Glob pattern matching."""
        return list(Path(path).glob(pattern))

    def rglob(self, path: Union[str, Path], pattern: str) -> List[Path]:
        """Recursive glob pattern matching."""
        return list(Path(path).rglob(pattern))

    def stat_size(self, path: Union[str, Path]) -> int:
        """Get file size."""
        return Path(path).stat().st_size

    def read_text(self, path: Union[str, Path]) -> str:
        """Read file content as text."""
        return Path(path).read_text(encoding="utf-8")

    def write_text(self, path: Union[str, Path], content: str) -> None:
        """Write text content to file."""
        Path(path).write_text(content, encoding="utf-8")


class RealJsonOperations:
    """Real JSON operations implementation."""

    def load(self, path: Union[str, Path]) -> Any:
        """Load JSON from file."""
        with Path(path).open(encoding="utf-8") as f:
            return json.load(f)

    def dump(self, data: Any, path: Union[str, Path]) -> None:
        """Dump JSON to file."""
        with Path(path).open("w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)


# Global instances for backward compatibility
_default_filesystem = RealFilesystemOperations()
_default_json = RealJsonOperations()
