"""Repository hygiene utilities for the tools CLI."""

from __future__ import annotations

from pathlib import Path
from ..core.interfaces import OperationId, ToolResult
from ..utils.subprocess_utils import SubprocessRunner


def run_cleanup_ignored_tracked(
    subprocess_runner: SubprocessRunner, root_path: Path
) -> ToolResult:
    """Clean up Git-ignored files that are still tracked."""
    operation_id = OperationId(
        namespace="tools", category="dev", command="cleanup-ignored-tracked"
    )
    try:
        listing = subprocess_runner.run_subprocess(
            ["git", "ls-files", "-i", "--exclude-standard"],
            cwd=root_path,
            operation_id=operation_id,
        )
        if not listing.success:
            return listing

        ignored_files = [
            line.strip() for line in listing.stdout.splitlines() if line.strip()
        ]
        if not ignored_files:
            return ToolResult.create(
                success=True,
                exit_code=0,
                namespace=operation_id.namespace,
                category=operation_id.category,
                command=operation_id.command,
                stdout="No ignored tracked files found.",
            )

        for file in ignored_files:
            removal = subprocess_runner.run_subprocess(
                ["git", "rm", "--cached", file],
                cwd=root_path,
                operation_id=operation_id,
            )
            if not removal.success:
                return removal

        return ToolResult.create(
            success=True,
            exit_code=0,
            namespace=operation_id.namespace,
            category=operation_id.category,
            command=operation_id.command,
            stdout=f"Removed {len(ignored_files)} ignored tracked files from git.",
        )
    except Exception as exc:
        return ToolResult.create(
            success=False,
            exit_code=1,
            namespace=operation_id.namespace,
            category=operation_id.category,
            command=operation_id.command,
            stderr=f"Failed to cleanup ignored tracked files: {exc}",
        )


def run_kill_port(
    port: int, subprocess_runner: SubprocessRunner, root_path: Path
) -> ToolResult:
    """Kill processes running on a specific port."""
    operation_id = OperationId(namespace="tools", category="dev", command="kill-port")
    try:
        pids = list(dict.fromkeys(_pids_by_port(port)))  # dedupe, preserve order
        if not pids:
            return ToolResult.create(
                success=True,
                exit_code=0,
                namespace=operation_id.namespace,
                category=operation_id.category,
                command=operation_id.command,
                stdout=f"No processes found running on port {port}.",
            )

        for pid in pids:
            if not _kill_pid(pid):
                return ToolResult.create(
                    success=False,
                    exit_code=1,
                    namespace=operation_id.namespace,
                    category=operation_id.category,
                    command=operation_id.command,
                    stderr=f"Failed to kill PID {pid} on port {port}.",
                )

        return ToolResult.create(
            success=True,
            exit_code=0,
            namespace=operation_id.namespace,
            category=operation_id.category,
            command=operation_id.command,
            stdout=f"Killed {len(pids)} processes running on port {port}.",
        )
    except Exception as exc:
        return ToolResult.create(
            success=False,
            exit_code=1,
            namespace=operation_id.namespace,
            category=operation_id.category,
            command=operation_id.command,
            stderr=f"Failed to kill port {port}: {exc}",
        )


def _pids_by_port(port: int) -> list[int]:
    """Get PIDs of processes using a specific port."""
    import psutil

    pids: set[int] = set()
    try:
        for conn in psutil.net_connections(kind="inet"):
            try:
                laddr = getattr(conn, "laddr", None)
                if not laddr:
                    continue
                conn_port = getattr(laddr, "port", None)
                if conn_port == port and conn.pid is not None:
                    pids.add(int(conn.pid))
            except Exception:
                # Connection may be inaccessible or have incomplete data
                continue
    except Exception:
        # Fallback: iterate processes if net_connections is restricted
        try:
            for proc in psutil.process_iter(attrs=["pid"]):  # type: ignore
                try:
                    for c in proc.net_connections(kind="inet"):
                        laddr = getattr(c, "laddr", None)
                        if laddr and getattr(laddr, "port", None) == port:
                            pids.add(int(proc.pid))
                            break
                except Exception:
                    continue
        except Exception:
            # Process iteration may be restricted on some systems
            return []
    return sorted(pids)


def _kill_pid(pid: int) -> bool:
    """Kill a process by PID."""
    import psutil

    try:
        proc = psutil.Process(pid)
        proc.terminate()
        try:
            proc.wait(timeout=1.0)
        except psutil.TimeoutExpired:
            proc.kill()
            proc.wait(timeout=1.0)
        return True
    except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.TimeoutExpired):
        return False
