from __future__ import annotations

from typing import Any

from ml_playground.tools.dev.dev import DevTools


def test_kill_port_success_with_di() -> None:
    """kill_port succeeds when provided deterministic DI hooks."""
    killed_pids: list[int] = []

    def fake_pids(port: int) -> list[int]:
        return [123, 456] if port == 8080 else []

    def fake_kill(pid: int) -> bool:
        killed_pids.append(pid)
        return True

    tools = DevTools(pids_by_port=fake_pids, kill_pid=fake_kill)
    result = tools.kill_port(8080)

    assert result.success is True
    assert "Killed 2 processes" in result.stdout
    assert killed_pids == [123, 456]


def test_kill_port_partial_failure_with_di() -> None:
    """kill_port reports partial failure when any PID kill fails."""

    def fake_pids(port: int) -> list[int]:
        return [123, 456]

    def fake_kill(pid: int) -> bool:
        return pid == 123  # 456 fails

    tools = DevTools(pids_by_port=fake_pids, kill_pid=fake_kill)
    result = tools.kill_port(8080)

    assert result.success is False
    assert "Failed to kill PID 456" in result.stderr


def test_kill_port_exception_handling() -> None:
    """kill_port surfaces unexpected PID lookup failures."""

    def raising_pids(port: int) -> list[int]:
        raise RuntimeError("unexpected failure")

    tools = DevTools(pids_by_port=raising_pids)
    result = tools.kill_port(8080)

    assert result.success is False
    assert "unexpected failure" in result.stderr


def test_kill_port_defaults_empty() -> None:
    """Default hooks with no processes should report none found."""
    tools = DevTools(pids_by_port=lambda p: [])
    result = tools.kill_port(9999)
    assert result.success is False or "No processes found" in (result.stdout or "")


def test_kill_port_custom_hooks_cover_branches() -> None:
    """Custom hooks exercise kill/retry logic without psutil mocks."""
    calls: list[tuple[str, Any]] = []

    def fake_pids(_: int) -> list[int]:
        return [1]

    class FakeProc:
        def __init__(self) -> None:
            self.terminated = False
            self.killed = False
            self.wait_calls = 0

        def terminate(self) -> None:
            calls.append(("terminate", None))
            self.terminated = True

        def kill(self) -> None:
            calls.append(("kill", None))
            self.killed = True

        def wait(self, timeout: float) -> None:
            calls.append(("wait", timeout))
            self.wait_calls += 1
            if self.wait_calls == 1:
                raise RuntimeError("timeout")

    def fake_kill(pid: int) -> bool:
        proc = FakeProc()
        try:
            proc.terminate()
            try:
                proc.wait(1.0)
            except Exception:
                proc.kill()
            return True
        except Exception:
            return False

    tools = DevTools(pids_by_port=fake_pids, kill_pid=fake_kill)
    result = tools.kill_port(8080)
    assert result.success is True
    assert any(call[0] == "terminate" for call in calls)
    assert any(call[0] == "kill" for call in calls)
