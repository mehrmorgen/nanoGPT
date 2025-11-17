"""Lightweight fakes for testing tools without mocking."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Union, TypedDict

from ml_playground.tools.core.interfaces import OperationId, ToolResult


class _SubprocessCall(TypedDict, total=False):
    command: list[str]
    cwd: str | Path | None
    env: dict[str, str] | None
    timeout: int | None
    operation_id: OperationId
    capture_output: bool


class FakeSubprocessRunner:
    """Fake subprocess runner for testing."""

    def __init__(self) -> None:
        """Initialize fake subprocess runner."""
        self.calls: list[_SubprocessCall] = []
        self._result_queue: list[Callable[[OperationId], ToolResult]] = []

    def set_results(self, results: List[ToolResult]) -> None:
        """Set the results to return for subsequent calls."""
        self._result_queue = [lambda op_id, res=result: res for result in results]  # type: ignore[arg-type]

    def add_result(self, result: ToolResult) -> None:
        """Add a single result to return."""
        self._result_queue.append(lambda _op_id, res=result: res)

    def queue_result_factory(
        self, factory: Callable[[OperationId], ToolResult]
    ) -> None:
        """Append a callable factory that can produce dynamic results."""
        self._result_queue.append(factory)

    def _next_result(self, operation_id: OperationId) -> ToolResult:
        if self._result_queue:
            factory = self._result_queue.pop(0)
            return factory(operation_id)
        return ToolResult(
            success=True,
            exit_code=0,
            stdout="",
            stderr="",
            operation_id=operation_id,
        )

    def run_subprocess(
        self,
        command: List[str],
        *,
        cwd: Optional[Union[str, Path]] = None,
        env: Optional[Dict[str, str]] = None,
        timeout: Optional[int] = None,
        operation_id: OperationId,
        capture_output: bool = True,
    ) -> ToolResult:
        """Fake subprocess execution."""
        # Record the call
        self.calls.append(
            {
                "command": command,
                "cwd": cwd,
                "env": env,
                "timeout": timeout,
                "operation_id": operation_id,
                "capture_output": capture_output,
            }
        )

        return self._next_result(operation_id)

    def run_uv_command(
        self,
        args: List[str],
        *,
        cwd: Optional[Union[str, Path]] = None,
        env: Optional[Dict[str, str]] = None,
        timeout: Optional[int] = None,
        operation_id: OperationId,
        python: Optional[str] = None,
        no_project: bool = False,
    ) -> ToolResult:
        """Fake uv command execution."""
        # Build the full command for recording
        command = ["uv", "run"]
        if no_project:
            command.append("--no-project")
        else:
            project_root = Path(cwd) if isinstance(cwd, str) else cwd or Path.cwd()
            command.extend(["--project", str(project_root)])
        if python:
            command.extend(["--python", python])
        command.extend(args)

        return self.run_subprocess(
            command,
            cwd=cwd,
            env=env,
            timeout=timeout,
            operation_id=operation_id,
        )

    def run_pytest_command(
        self,
        args: List[str],
        *,
        cwd: Optional[Union[str, Path]] = None,
        env: Optional[Dict[str, str]] = None,
        timeout: Optional[int] = None,
        operation_id: OperationId,
    ) -> ToolResult:
        """Fake pytest command execution."""
        pytest_base = [
            "-n",
            "auto",
            "-W",
            "error",
            "--strict-markers",
            "--strict-config",
        ]
        return self.run_uv_command(
            ["pytest", *pytest_base, *args],
            cwd=cwd,
            env=env,
            timeout=timeout,
            operation_id=operation_id,
        )


class FakeFilesystem:
    """Fake filesystem for testing."""

    def __init__(self) -> None:
        """Initialize fake filesystem."""
        self.files: Dict[str, bytes] = {}
        self.directories: set[str] = set()
        self.removed_paths: List[str] = []
        self.created_paths: List[str] = []

    def create_file(
        self, path: Union[str, Path], content: Union[str, bytes] = b""
    ) -> None:
        """Create a fake file."""
        path_str = str(path)
        if isinstance(content, str):
            content = content.encode("utf-8")
        self.files[path_str] = content
        self.created_paths.append(path_str)

        # Ensure parent directories exist
        parent = str(Path(path_str).parent)
        if parent != path_str:  # Not root
            self.directories.add(parent)

    def create_directory(self, path: Union[str, Path]) -> None:
        """Create a fake directory."""
        path_str = str(path)
        self.directories.add(path_str)
        self.created_paths.append(path_str)

    def exists(self, path: Union[str, Path]) -> bool:
        """Check if path exists."""
        path_str = str(path)
        return path_str in self.files or path_str in self.directories

    def is_file(self, path: Union[str, Path]) -> bool:
        """Check if path is a file."""
        return str(path) in self.files

    def is_dir(self, path: Union[str, Path]) -> bool:
        """Check if path is a directory."""
        return str(path) in self.directories

    def read_text(self, path: Union[str, Path]) -> str:
        """Read file content as text."""
        path_str = str(path)
        if path_str not in self.files:
            raise FileNotFoundError(f"No such file: {path_str}")
        return self.files[path_str].decode("utf-8")

    def read_bytes(self, path: Union[str, Path]) -> bytes:
        """Read file content as bytes."""
        path_str = str(path)
        if path_str not in self.files:
            raise FileNotFoundError(f"No such file: {path_str}")
        return self.files[path_str]

    def write_text(self, path: Union[str, Path], content: str) -> None:
        """Write text content to file."""
        self.create_file(path, content.encode("utf-8"))

    def write_bytes(self, path: Union[str, Path], content: bytes) -> None:
        """Write bytes content to file."""
        self.create_file(path, content)

    def unlink(self, path: Union[str, Path]) -> None:
        """Remove a file."""
        path_str = str(path)
        if path_str in self.files:
            del self.files[path_str]
            self.removed_paths.append(path_str)

    def rmtree(self, path: Union[str, Path]) -> None:
        """Remove a directory tree."""
        path_str = str(path)

        # Remove the directory itself
        if path_str in self.directories:
            self.directories.remove(path_str)
            self.removed_paths.append(path_str)

        # Remove all files and subdirectories under this path
        to_remove_files = []
        to_remove_dirs = []

        for file_path in self.files:
            if file_path.startswith(path_str + "/") or file_path == path_str:
                to_remove_files.append(file_path)

        for dir_path in self.directories:
            if dir_path.startswith(path_str + "/") or dir_path == path_str:
                to_remove_dirs.append(dir_path)

        for file_path in to_remove_files:
            del self.files[file_path]
            self.removed_paths.append(file_path)

        for dir_path in to_remove_dirs:
            self.directories.remove(dir_path)
            self.removed_paths.append(dir_path)

    def mkdir(self, path: Union[str, Path], parents: bool = False) -> None:
        """Create a directory."""
        path_str = str(path)

        if parents:
            # Create all parent directories
            parts = Path(path_str).parts
            for i in range(1, len(parts) + 1):
                parent_path = str(Path(*parts[:i]))
                self.directories.add(parent_path)
        else:
            self.directories.add(path_str)

        self.created_paths.append(path_str)

    def iterdir(self, path: Union[str, Path]) -> List[Path]:
        """List directory contents."""
        path_str = str(path)
        if path_str not in self.directories:
            raise FileNotFoundError(f"No such directory: {path_str}")

        contents = []

        # Find direct children
        for file_path in self.files:
            if Path(file_path).parent == Path(path_str):
                contents.append(Path(file_path))

        for dir_path in self.directories:
            if Path(dir_path).parent == Path(path_str):
                contents.append(Path(dir_path))

        return sorted(contents)

    def glob(self, path: Union[str, Path], pattern: str) -> List[Path]:
        """Glob pattern matching."""
        path_str = str(path)
        results = []

        # Simple pattern matching for common cases
        if pattern == "*":
            return self.iterdir(path_str)
        elif pattern.endswith("*"):
            prefix = pattern[:-1]
            for file_path in self.files:
                if Path(file_path).parent == Path(path_str) and Path(
                    file_path
                ).name.startswith(prefix):
                    results.append(Path(file_path))

        return sorted(results)

    def rglob(self, path: Union[str, Path], pattern: str) -> List[Path]:
        """Recursive glob pattern matching."""
        path_str = str(path)
        results = []

        # Simple recursive pattern matching
        if pattern == "*":
            # All files and directories under path
            for file_path in self.files:
                if file_path.startswith(path_str + "/") or file_path == path_str:
                    results.append(Path(file_path))
            for dir_path in self.directories:
                if dir_path.startswith(path_str + "/") or dir_path == path_str:
                    results.append(Path(dir_path))
        elif pattern == "__pycache__":
            # Find all __pycache__ directories
            for dir_path in self.directories:
                if (
                    dir_path.startswith(path_str + "/") or dir_path == path_str
                ) and dir_path.endswith("__pycache__"):
                    results.append(Path(dir_path))

        return sorted(results)

    def stat_size(self, path: Union[str, Path]) -> int:
        """Get file size."""
        path_str = str(path)
        if path_str not in self.files:
            raise FileNotFoundError(f"No such file: {path_str}")
        return len(self.files[path_str])


class FakeJsonHandler:
    """Fake JSON handler for testing."""

    def __init__(self, filesystem: FakeFilesystem) -> None:
        """Initialize with filesystem fake."""
        self.filesystem = filesystem

    def load(self, file_path: Union[str, Path]) -> Any:
        """Load JSON from fake file."""
        content = self.filesystem.read_text(file_path)
        return json.loads(content)

    def dump(self, data: Any, file_path: Union[str, Path]) -> None:
        """Dump JSON to fake file."""
        content = json.dumps(data, indent=2)
        self.filesystem.write_text(file_path, content)


# ---------------- Shared runners for coverage flows ----------------


class RecordingRunner:
    """Baseline fake runner collecting pytest/uv invocations."""

    def __init__(self) -> None:
        self.pytest_calls: list[dict[str, object]] = []
        self.uv_calls: list[dict[str, object]] = []

    def run_subprocess(
        self,
        command: list[str],
        *,
        cwd: Optional[Union[str, Path]] = None,
        env: Optional[Dict[str, str]] = None,
        timeout: Optional[int] = None,
        operation_id: OperationId,
        capture_output: bool = True,
    ) -> ToolResult:
        return create_success_result(operation_id, stdout="subprocess")

    def run_pytest_command(
        self,
        args: list[str],
        *,
        cwd: Optional[Union[str, Path]] = None,
        env: Optional[Dict[str, str]] = None,
        timeout: Optional[int] = None,
        operation_id: OperationId,
    ) -> ToolResult:
        self.pytest_calls.append({"args": args, "env": env, "cwd": cwd})
        if env and "COVERAGE_FILE" in env:
            coverage_path = Path(env["COVERAGE_FILE"])  # type: ignore[index]
            coverage_path.parent.mkdir(parents=True, exist_ok=True)
            coverage_path.write_bytes(b"data")
        return create_success_result(operation_id, stdout="pytest")

    def run_uv_command(
        self,
        args: list[str],
        *,
        cwd: Optional[Union[str, Path]] = None,
        env: Optional[Dict[str, str]] = None,
        timeout: Optional[int] = None,
        operation_id: OperationId,
        python: Optional[str] = None,
        no_project: bool = False,
    ) -> ToolResult:
        self.uv_calls.append({"args": args, "env": env, "cwd": cwd})
        if args[:2] == ["coverage", "json"]:
            out_path = Path(args[args.index("-o") + 1])
            out_path.parent.mkdir(parents=True, exist_ok=True)
            out_path.write_text(
                json.dumps(
                    {
                        "totals": {
                            "num_statements": 100,
                            "covered_lines": 100,
                            "num_branches": 10,
                            "covered_branches": 10,
                        },
                        "files": {},
                    }
                )
            )
        if args[:2] == ["coverage", "combine"] and env and "COVERAGE_FILE" in env:
            coverage_path = Path(env["COVERAGE_FILE"])  # type: ignore[index]
            coverage_path.parent.mkdir(parents=True, exist_ok=True)
            coverage_path.write_bytes(b"combined")
        return create_success_result(operation_id, stdout="uv")


class MetricsRunner(RecordingRunner):
    def __init__(self) -> None:
        super().__init__()
        self._payload = {
            "totals": {
                "num_statements": 10,
                "covered_lines": 9,
                "num_branches": 4,
                "covered_branches": 3,
            },
            "files": {
                "src/ml_playground/tools/a.py": {
                    "summary": {
                        "percent_covered_display": "90.00",
                        "num_branches": 2,
                        "covered_branches": 1,
                    }
                },
                "src/ml_playground/tools/nested/b.py": {
                    "summary": {
                        "percent_covered": 75.0,
                        "num_branches": 2,
                        "covered_branches": 2,
                    }
                },
            },
        }

    def run_uv_command(  # type: ignore[override]
        self,
        args: list[str],
        *,
        cwd: Optional[Union[str, Path]] = None,
        env: Optional[Dict[str, str]] = None,
        timeout: Optional[int] = None,
        operation_id: OperationId,
        python: Optional[str] = None,
        no_project: bool = False,
    ) -> ToolResult:
        result = super().run_uv_command(
            args,
            cwd=cwd,
            env=env,
            timeout=timeout,
            operation_id=operation_id,
            python=python,
            no_project=no_project,
        )
        if args[:2] == ["coverage", "json"]:
            out_path = Path(args[args.index("-o") + 1])
            out_path.write_text(json.dumps(self._payload))
        return result


class PytestFailureRunner(RecordingRunner):
    def run_pytest_command(  # type: ignore[override]
        self,
        args: list[str],
        *,
        cwd: Optional[Union[str, Path]] = None,
        env: Optional[Dict[str, str]] = None,
        timeout: Optional[int] = None,
        operation_id: OperationId,
    ) -> ToolResult:
        return create_failure_result(operation_id, stderr="pytest failed")


class CombineFailureRunner(RecordingRunner):
    def run_uv_command(  # type: ignore[override]
        self,
        args: list[str],
        *,
        cwd: Optional[Union[str, Path]] = None,
        env: Optional[Dict[str, str]] = None,
        timeout: Optional[int] = None,
        operation_id: OperationId,
        python: Optional[str] = None,
        no_project: bool = False,
    ) -> ToolResult:
        # Record call like RecordingRunner
        self.uv_calls.append({"args": args, "env": env, "cwd": cwd})
        if args[:2] == ["coverage", "combine"]:
            return create_failure_result(operation_id, stderr="combine failed")
        return create_success_result(operation_id, stdout="ok")


class FailingJsonRunner(RecordingRunner):
    def run_uv_command(  # type: ignore[override]
        self,
        args: list[str],
        *,
        cwd: Optional[Union[str, Path]] = None,
        env: Optional[Dict[str, str]] = None,
        timeout: Optional[int] = None,
        operation_id: OperationId,
        python: Optional[str] = None,
        no_project: bool = False,
    ) -> ToolResult:
        if args[:2] == ["coverage", "json"]:
            return create_failure_result(operation_id, stderr="json failed")
        return create_success_result(operation_id, stdout="ok")


class ReportFailureRunner(RecordingRunner):
    def run_uv_command(  # type: ignore[override]
        self,
        args: list[str],
        *,
        cwd: Optional[Union[str, Path]] = None,
        env: Optional[Dict[str, str]] = None,
        timeout: Optional[int] = None,
        operation_id: OperationId,
        python: Optional[str] = None,
        no_project: bool = False,
    ) -> ToolResult:
        if args[:3] == ["coverage", "report", "-m"]:
            return create_failure_result(operation_id, stderr="terminal report failed")
        return create_success_result(operation_id, stdout="ok")


# ---------------- Shared helpers for coverage tests ----------------


def create_sample_source_file(root_path: Path) -> Path:
    source_dir = root_path / "src" / "ml_playground" / "tools"
    source_dir.mkdir(parents=True, exist_ok=True)
    sample_file = source_dir / "sample_module.py"
    sample_file.write_text("value = 0", encoding="utf-8")
    return sample_file


def write_manifest(root_path: Path, fingerprint: str) -> Path:
    manifest_path = root_path / ".cache" / "coverage" / "coverage_manifest.json"
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text(json.dumps({"fingerprint": fingerprint}), encoding="utf-8")
    return manifest_path


def write_coverage_file(root_path: Path, payload: bytes = b"data") -> Path:
    coverage_path = root_path / ".cache" / "coverage" / "coverage.sqlite"
    coverage_path.parent.mkdir(parents=True, exist_ok=True)
    coverage_path.write_bytes(payload)
    return coverage_path


def create_success_result(
    operation_id: OperationId, stdout: str = "", stderr: str = ""
) -> ToolResult:
    """Create a successful ToolResult."""
    return ToolResult(
        success=True,
        exit_code=0,
        stdout=stdout,
        stderr=stderr,
        operation_id=operation_id,
    )


def create_failure_result(
    operation_id: OperationId, exit_code: int = 1, stdout: str = "", stderr: str = ""
) -> ToolResult:
    """Create a failed ToolResult."""
    return ToolResult(
        success=False,
        exit_code=exit_code,
        stdout=stdout,
        stderr=stderr,
        operation_id=operation_id,
    )
