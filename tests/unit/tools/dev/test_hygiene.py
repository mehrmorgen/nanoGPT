"""Unit tests for tools.dev.hygiene helpers."""

from __future__ import annotations

import subprocess
from contextlib import contextmanager
from types import SimpleNamespace, ModuleType
from typing import Callable, Iterator
import sys

from ml_playground.tools.core.interfaces import OperationId, ToolResult
from ml_playground.tools.dev import hygiene


class StubRunner:
    """Minimal subprocess runner double returning queued results."""

    def __init__(self, responders: list[Callable[[OperationId], ToolResult]]) -> None:
        self.responders = responders
        self.commands: list[list[str]] = []

    def run_subprocess(
        self,
        command: list[str],
        *,
        cwd: str | None,
        operation_id: OperationId,
    ) -> ToolResult:
        self.commands.append(command)
        responder = self.responders.pop(0)
        return responder(operation_id)


def _result(
    *,
    success: bool,
    stdout: str = "",
    stderr: str = "",
) -> Callable[[OperationId], ToolResult]:
    def factory(operation_id: OperationId) -> ToolResult:
        return ToolResult(
            success=success,
            exit_code=0 if success else 1,
            stdout=stdout,
            stderr=stderr,
            operation_id=operation_id,
        )

    return factory


def test_run_cleanup_no_files_returns_message(tmp_path) -> None:
    runner = StubRunner(
        [
            _result(success=True, stdout=""),
        ]
    )

    result = hygiene.run_cleanup_ignored_tracked(runner, tmp_path)

    assert result.success is True
    assert result.stdout == "No ignored tracked files found."
    assert runner.commands[0][:3] == ["git", "ls-files", "-i"]


def test_run_cleanup_propagates_failed_rm(tmp_path) -> None:
    listing_stdout = "foo.log\nbar.ckpt\n"
    runner = StubRunner(
        [
            _result(success=True, stdout=listing_stdout),
            _result(success=True, stdout="removed foo"),
            _result(success=False, stderr="failed to remove bar"),
        ]
    )

    result = hygiene.run_cleanup_ignored_tracked(runner, tmp_path)

    assert result.success is False
    assert "failed to remove bar" in (result.stderr or "")
    assert runner.commands[1] == ["git", "rm", "--cached", "foo.log"]
    assert runner.commands[2][3] == "bar.ckpt"


def test_run_cleanup_returns_error_on_exception(tmp_path) -> None:
    class ExplodingRunner(StubRunner):
        def __init__(self) -> None:
            super().__init__([])

        def run_subprocess(self, *args, **kwargs):  # type: ignore[override]
            raise subprocess.SubprocessError("git missing")

    result = hygiene.run_cleanup_ignored_tracked(ExplodingRunner(), tmp_path)

    assert result.success is False
    assert "Failed to cleanup ignored tracked files" in (result.stderr or "")


@contextmanager
def override_attr(obj: object, name: str, value: object) -> Iterator[None]:
    original = getattr(obj, name)
    setattr(obj, name, value)
    try:
        yield
    finally:
        setattr(obj, name, original)


def test_run_kill_port_dedupes_and_reports_success(tmp_path) -> None:
    seen: list[int] = []

    def fake_pids(port: int) -> list[int]:
        assert port == 8080
        return [111, 111, 222]

    def fake_kill(pid: int) -> bool:
        seen.append(pid)
        return True

    with (
        override_attr(hygiene, "_pids_by_port", fake_pids),
        override_attr(hygiene, "_kill_pid", fake_kill),
    ):
        result = hygiene.run_kill_port(8080, StubRunner([]), tmp_path)

    assert result.success is True
    assert seen == [111, 222]
    assert "Killed 2 processes" in (result.stdout or "")


def test_run_kill_port_propagates_kill_failure(tmp_path) -> None:
    def fake_pids(_: int) -> list[int]:
        return [42]

    def fake_kill(_: int) -> bool:
        return False

    with (
        override_attr(hygiene, "_pids_by_port", fake_pids),
        override_attr(hygiene, "_kill_pid", fake_kill),
    ):
        result = hygiene.run_kill_port(42, StubRunner([]), tmp_path)

    assert result.success is False
    assert "Failed to kill PID 42" in (result.stderr or "")


def test_run_kill_port_handles_lookup_exception(tmp_path) -> None:
    def raising(_: int) -> list[int]:
        raise OSError("psutil error")

    with (
        override_attr(hygiene, "_pids_by_port", raising),
        override_attr(hygiene, "_kill_pid", lambda _: True),
    ):
        result = hygiene.run_kill_port(9000, StubRunner([]), tmp_path)

    assert result.success is False
    assert "Failed to kill port 9000" in (result.stderr or "")


def _install_fake_psutil(fake: ModuleType) -> ModuleType | None:
    original = sys.modules.get("psutil")
    sys.modules["psutil"] = fake
    return original


def _restore_psutil(original: ModuleType | None) -> None:
    if original is None:
        sys.modules.pop("psutil", None)
    else:
        sys.modules["psutil"] = original


def test_pids_by_port_collects_from_net_connections() -> None:
    connections = [
        SimpleNamespace(laddr=SimpleNamespace(port=8000), pid=10),
        SimpleNamespace(laddr=SimpleNamespace(port=7000), pid=20),
        SimpleNamespace(laddr=SimpleNamespace(port=8000), pid=10),
    ]

    fake_psutil = ModuleType("psutil")

    def net_connections(kind: str):  # type: ignore[override]
        assert kind == "inet"
        return connections

    fake_psutil.net_connections = net_connections  # type: ignore[attr-defined]

    original = _install_fake_psutil(fake_psutil)
    try:
        result = hygiene._pids_by_port(8000)
    finally:
        _restore_psutil(original)

    assert result == [10]


def test_pids_by_port_falls_back_to_process_iter_on_error() -> None:
    fake_psutil = ModuleType("psutil")

    def net_connections(kind: str):  # type: ignore[override]
        raise RuntimeError("blocked")

    class Proc:
        def __init__(self, pid: int, ports: list[int]) -> None:
            self.pid = pid
            self._ports = ports

        def net_connections(self, kind: str):  # type: ignore[override]
            assert kind == "inet"
            return [SimpleNamespace(laddr=SimpleNamespace(port=p)) for p in self._ports]

    def process_iter(attrs=None):  # type: ignore[override]
        assert attrs == ["pid"]
        return [Proc(1, [9000]), Proc(2, [8000]), Proc(3, [9000, 9000])]

    fake_psutil.net_connections = net_connections  # type: ignore[attr-defined]
    fake_psutil.process_iter = process_iter  # type: ignore[attr-defined]

    original = _install_fake_psutil(fake_psutil)
    try:
        result = hygiene._pids_by_port(9000)
    finally:
        _restore_psutil(original)

    assert result == [1, 3]


def test_kill_pid_returns_false_on_psutil_errors() -> None:
    fake_psutil = ModuleType("psutil")

    class TimeoutExpired(Exception):
        pass

    class Process:
        def __init__(self, pid: int) -> None:
            self.pid = pid

        def terminate(self) -> None:  # type: ignore[override]
            raise TimeoutExpired("timeout")

        def wait(self, timeout: float) -> None:  # type: ignore[override]
            raise TimeoutExpired("timeout")

        def kill(self) -> None:  # type: ignore[override]
            raise TimeoutExpired("timeout")

    def ProcessFactory(pid: int) -> Process:  # type: ignore[override]
        return Process(pid)

    fake_psutil.Process = ProcessFactory  # type: ignore[attr-defined]
    fake_psutil.TimeoutExpired = TimeoutExpired  # type: ignore[attr-defined]
    fake_psutil.NoSuchProcess = TimeoutExpired  # type: ignore[attr-defined]
    fake_psutil.AccessDenied = TimeoutExpired  # type: ignore[attr-defined]

    original = _install_fake_psutil(fake_psutil)
    try:
        result = hygiene._kill_pid(1234)
    finally:
        _restore_psutil(original)

    assert result is False


def test_pids_by_port_falls_back_to_empty_list_on_complete_failure() -> None:
    """Test the final fallback when both net_connections and process_iter fail."""
    fake_psutil = ModuleType("psutil")

    def net_connections(kind: str):  # type: ignore[override]
        raise RuntimeError("blocked")

    def process_iter(attrs=None):  # type: ignore[override]
        raise RuntimeError("access denied")

    fake_psutil.net_connections = net_connections  # type: ignore[attr-defined]
    fake_psutil.process_iter = process_iter  # type: ignore[attr-defined]

    original = _install_fake_psutil(fake_psutil)
    try:
        result = hygiene._pids_by_port(9000)
    finally:
        _restore_psutil(original)

    assert result == []  # Should return empty list when all methods fail


def test_pids_by_port_handles_individual_connection_exceptions() -> None:
    """Test that individual connection exceptions are skipped but processing continues."""
    fake_psutil = ModuleType("psutil")

    class BadConnection:
        def __init__(self, should_fail: bool = False):
            self.should_fail = should_fail

        @property
        def laddr(self):
            if self.should_fail:
                raise RuntimeError("connection error")
            return SimpleNamespace(port=8000)

        @property
        def pid(self):
            return 42 if not self.should_fail else None

    def net_connections(kind: str):  # type: ignore[override]
        assert kind == "inet"
        return [
            BadConnection(should_fail=False),  # Good connection
            BadConnection(should_fail=True),  # Bad connection (should be skipped)
            BadConnection(should_fail=False),  # Another good connection
            SimpleNamespace(laddr=None, pid=24),  # No laddr (should be skipped)
            SimpleNamespace(laddr=SimpleNamespace(port=7000), pid=99),  # Wrong port
        ]

    fake_psutil.net_connections = net_connections  # type: ignore[attr-defined]

    original = _install_fake_psutil(fake_psutil)
    try:
        result = hygiene._pids_by_port(8000)
    finally:
        _restore_psutil(original)

    assert result == [42]  # Should only include the good connections with port 8000
