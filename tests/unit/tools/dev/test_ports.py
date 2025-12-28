from __future__ import annotations

# ruff: noqa: TID251  # allow unittest.mock usage in this test module

import psutil
from unittest.mock import MagicMock, patch, PropertyMock

from ml_playground.tools.dev.dev import DevTools


def test_kill_port_success_with_di() -> None:
    """Test kill_port success path using dependency injection."""
    killed_pids: list[int] = []

    def fake_pids(port: int) -> list[int]:
        return [123, 456]

    def fake_kill(pid: int) -> bool:
        killed_pids.append(pid)
        return True

    tools = DevTools(pids_by_port=fake_pids, kill_pid=fake_kill)
    result = tools.kill_port(8080)

    assert result.success is True
    assert "Killed 2 processes" in result.stdout
    assert killed_pids == [123, 456]


def test_kill_port_partial_failure_with_di() -> None:
    """Test kill_port partial failure using dependency injection."""

    def fake_pids(port: int) -> list[int]:
        return [123, 456]

    def fake_kill(pid: int) -> bool:
        return pid == 123  # 456 fails

    tools = DevTools(pids_by_port=fake_pids, kill_pid=fake_kill)
    result = tools.kill_port(8080)

    # Returns False because at least one failed
    assert result.success is False
    assert "Failed to kill PID 456" in result.stderr


def test_kill_port_exception_handling() -> None:
    """Test kill_port generic exception handling."""

    def raising_pids(port: int) -> list[int]:
        raise RuntimeError("unexpected failure")

    tools = DevTools(pids_by_port=raising_pids)
    result = tools.kill_port(8080)

    assert result.success is False
    assert "Failed to kill port 8080: unexpected failure" in result.stderr


def test_kill_port_psutil_net_connections_success() -> None:
    """Exercise default PID lookup logic using psutil.net_connections."""
    mock_conn = MagicMock()
    mock_conn.laddr.port = 8080
    mock_conn.pid = 999

    # Branch 513: mismatch port
    mock_conn_mismatch = MagicMock()
    mock_conn_mismatch.laddr.port = 111

    # Branch 510: no laddr
    mock_conn_no_laddr = MagicMock(spec=["pid"])
    mock_conn_no_laddr.pid = 112

    # Branch 515: exception accessing laddr
    mock_conn_err = MagicMock()
    type(mock_conn_err).laddr = PropertyMock(side_effect=Exception("oops"))

    with patch(
        "psutil.net_connections",
        return_value=[mock_conn, mock_conn_mismatch, mock_conn_no_laddr, mock_conn_err],
    ):
        tools = DevTools(kill_pid=lambda p: True)
        result = tools.kill_port(8080)
        assert result.success is True
        assert "Killed 1 processes" in result.stdout


def test_kill_port_psutil_fallback_loop() -> None:
    """Exercise default PID lookup logic fallback loop via process_iter."""
    # net_connections fails, trigger process_iter fallback
    with patch("psutil.net_connections", side_effect=Exception("unavailable")):
        mock_proc = MagicMock()
        mock_proc.pid = 777

        mock_conn = MagicMock()
        laddr = MagicMock()
        laddr.port = 9090
        mock_conn.laddr = laddr

        # In fallback loop, it calls proc.net_connections(kind="inet") at line 522
        mock_proc.net_connections.return_value = [mock_conn]

        with patch("psutil.process_iter", return_value=[mock_proc]):
            tools = DevTools(kill_pid=lambda p: True)
            result = tools.kill_port(9090)
            assert result.success is True
            assert "Killed 1 processes" in result.stdout


def test_kill_port_psutil_fallback_loop_errors() -> None:
    """Exercise branches in fallback loop (mismatch, exception)."""
    mock_proc_err = MagicMock()
    mock_proc_err.net_connections.side_effect = Exception("err")

    with patch("psutil.net_connections", side_effect=Exception("unavailable")):
        with patch("psutil.process_iter", return_value=[mock_proc_err]):
            tools = DevTools()
            result = tools.kill_port(8080)
            assert "No processes found" in result.stdout


def test_kill_port_psutil_fallback_loop_total_fail() -> None:
    """Exercise line 530 where process_iter itself fails."""
    with patch("psutil.net_connections", side_effect=Exception("unavailable")):
        with patch("psutil.process_iter", side_effect=Exception("failed")):
            tools = DevTools()
            result = tools.kill_port(8080)
            assert "No processes found" in result.stdout


def test_kill_port_psutil_kill_retry_logic() -> None:
    """Exercise default kill logic including terminate and kill fallback."""
    mock_proc = MagicMock()
    # Mock wait to raise TimeoutExpired to trigger kill()
    # terminate is called, then wait(1.0). If wait fails, kill is called.
    mock_proc.wait.side_effect = [psutil.TimeoutExpired(1.0), None]

    with patch("psutil.Process", return_value=mock_proc):
        # Find one PID to trigger the logic
        tools = DevTools(pids_by_port=lambda p: [123])
        result = tools.kill_port(8080)
        assert result.success is True
        mock_proc.terminate.assert_called_once()
        mock_proc.kill.assert_called_once()


def test_kill_port_psutil_kill_failure_branches() -> None:
    """Exercise default kill logic error branches (line 545)."""
    # 1. NoSuchProcess
    with patch("psutil.Process", side_effect=psutil.NoSuchProcess(123)):
        tools = DevTools(pids_by_port=lambda p: [123])
        result = tools.kill_port(8080)
        assert result.success is False
        assert "Failed to kill PID 123" in result.stderr

    # 2. AccessDenied
    with patch("psutil.Process", side_effect=psutil.AccessDenied(456)):
        tools = DevTools(pids_by_port=lambda p: [456])
        result = tools.kill_port(8080)
        assert result.success is False

    # 3. TimeoutExpired on second wait
    mock_proc = MagicMock()
    mock_proc.wait.side_effect = [
        psutil.TimeoutExpired(1.0),
        psutil.TimeoutExpired(1.0),
    ]
    with patch("psutil.Process", return_value=mock_proc):
        tools = DevTools(pids_by_port=lambda p: [789])
        result = tools.kill_port(8080)
        assert result.success is False
