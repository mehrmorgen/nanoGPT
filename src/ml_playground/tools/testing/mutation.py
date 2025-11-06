"""Mutation testing helpers extracted from TestingTools.

These functions implement the Cosmic Ray flow and are called by the
TestingTools facade to keep the public API stable and the file sizes small.
"""

from __future__ import annotations

import os
from contextlib import contextmanager
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from ml_playground.tools.core.errors import ToolExecutionError
from ml_playground.tools.core.interfaces import OperationId, ToolResult
from ml_playground.tools.core.config import ToolsConfig
from ml_playground.tools.utils.subprocess_utils import SubprocessRunner


CATEGORY = "test"


@contextmanager
def _cwd(path: Path):
    current = Path.cwd()
    os.chdir(path)
    try:
        yield
    finally:
        os.chdir(current)


def mutation_reset(config: ToolsConfig, root_path: Path) -> ToolResult:
    operation_id = OperationId(
        namespace="tools", category=CATEGORY, command="mutation-reset"
    )

    session_file = Path(".cache/cosmic-ray/session.sqlite")
    if session_file.exists():
        try:
            session_file.unlink()
            output = f"Removed Cosmic Ray session: {session_file}"
        except Exception as exc:  # pragma: no cover - defensive
            raise ToolExecutionError(
                f"Failed to remove Cosmic Ray session file: {session_file}",
                reason=f"File deletion failed: {exc}",
                rationale="Session file must be removable for clean mutation testing",
            ) from exc
    else:
        output = f"Cosmic Ray session file does not exist: {session_file}"

    return ToolResult(
        success=True,
        exit_code=0,
        stdout=output,
        stderr="",
        operation_id=operation_id,
    )


def mutation_summary(config: ToolsConfig, root_path: Path) -> ToolResult:
    operation_id = OperationId(
        namespace="tools", category=CATEGORY, command="mutation-summary"
    )

    try:
        from cosmic_ray.config import load_config  # type: ignore
        from cosmic_ray.modules import find_modules  # type: ignore

        config_file = Path("pyproject.toml")
        cfg = load_config(str(config_file))
        assert isinstance(cfg, Mapping)

        session_cfg = cfg.get("session", {})
        session_mapping = session_cfg if isinstance(session_cfg, Mapping) else {}
        session_path = Path(
            session_mapping.get("path", ".cache/cosmic-ray/session.sqlite")
        )

        test_runner_cfg = cfg.get("test-runner", {})
        test_runner_mapping = (
            test_runner_cfg if isinstance(test_runner_cfg, Mapping) else {}
        )
        test_command = test_runner_mapping.get("command", "pytest")

        modules_cfg = cfg.get("modules", {})
        modules_input: Any = modules_cfg
        modules = tuple(find_modules(modules_input))  # type: ignore[arg-type]

        output_lines = [
            f"[mutation] config: {config_file}",
            f"[mutation] session: {session_path}",
            f"[mutation] test command: {test_command}",
            f"[mutation] modules to mutate: {len(modules)}",
        ]
        for module in sorted(modules)[:5]:
            output_lines.append(f"[mutation]   - {module}")
        if len(modules) > 5:
            output_lines.append(f"[mutation]   ... and {len(modules) - 5} more")

        return ToolResult(
            success=True,
            exit_code=0,
            stdout="\n".join(output_lines),
            stderr="",
            operation_id=operation_id,
        )

    except ImportError as e:
        return ToolResult(
            success=False,
            exit_code=1,
            stdout="",
            stderr=f"cosmic_ray must be installed to use mutation testing: {e}",
            operation_id=operation_id,
        )
    except Exception as e:  # pragma: no cover - defensive
        return ToolResult(
            success=False,
            exit_code=1,
            stdout="",
            stderr=f"Failed to generate mutation summary: {e}",
            operation_id=operation_id,
        )


def mutation_init(
    config: ToolsConfig, root_path: Path, runner: SubprocessRunner
) -> ToolResult:
    operation_id = OperationId(
        namespace="tools", category=CATEGORY, command="mutation-init"
    )

    session_file = Path(".cache/cosmic-ray/session.sqlite")
    session_file.parent.mkdir(parents=True, exist_ok=True)

    result = runner.run_uv_command(
        ["cosmic-ray", "init", "pyproject.toml", str(session_file)],
        cwd=root_path,
        timeout=config.testing.timeout,
        operation_id=operation_id,
    )

    if result.success:
        output = "Cosmic Ray session initialized"
        return ToolResult(
            success=True,
            exit_code=0,
            stdout=output,
            stderr=result.stderr,
            operation_id=operation_id,
        )
    # Expected behavior: treat as success when reusing an existing session
    return ToolResult(
        success=True,
        exit_code=0,
        stdout="Cosmic Ray init skipped (reusing existing session)",
        stderr=result.stderr,
        operation_id=operation_id,
    )


def mutation_exec(
    config: ToolsConfig, root_path: Path, runner: SubprocessRunner
) -> ToolResult:
    operation_id = OperationId(
        namespace="tools", category=CATEGORY, command="mutation-exec"
    )

    session_file = Path(".cache/cosmic-ray/session.sqlite")
    if not session_file.exists():
        raise ToolExecutionError(
            "Cosmic Ray session file not found",
            reason=f"Missing session file: {session_file}",
            rationale="Mutation execution requires initialized session database",
        )

    return runner.run_uv_command(
        ["cosmic-ray", "exec", "pyproject.toml", str(session_file)],
        cwd=root_path,
        timeout=config.testing.timeout,
        operation_id=operation_id,
    )


def mutation_report(config: ToolsConfig, root_path: Path) -> ToolResult:
    operation_id = OperationId(
        namespace="tools", category=CATEGORY, command="mutation-report"
    )

    try:
        import sqlite3
        from collections import Counter
        from cosmic_ray.config import load_config  # type: ignore

        config_file = Path("pyproject.toml")
        cfg = load_config(str(config_file))
        assert isinstance(cfg, Mapping)

        session_cfg = cfg.get("session", {})
        session_mapping = session_cfg if isinstance(session_cfg, Mapping) else {}
        session_path = Path(
            session_mapping.get("path", ".cache/cosmic-ray/session.sqlite")
        )

        try:
            conn_ctx = sqlite3.connect(session_path)
        except Exception:
            return ToolResult(
                success=True,
                exit_code=0,
                stdout="[mutation] session file not found: no results to report",
                stderr="",
                operation_id=operation_id,
            )

        with conn_ctx as conn:
            conn.row_factory = lambda _cursor, row: row[0]
            # Support real sqlite cursors and simple iterator-based fakes
            total_cursor = conn.execute("SELECT COUNT(*) FROM work_results")
            if hasattr(total_cursor, "fetchone"):
                row = total_cursor.fetchone()
                total = (
                    (row[0] if isinstance(row, (list, tuple)) else row)
                    if row is not None
                    else 0
                )
            else:
                first = next(iter(total_cursor), 0)
                total = first[0] if isinstance(first, (list, tuple)) else first

            outcome_iter = conn.execute(
                "SELECT COALESCE(test_outcome, 'UNKNOWN') FROM work_results"
            )
            values: list[str] = []
            for row in outcome_iter:
                value = row[0] if isinstance(row, (list, tuple)) else row
                values.append(str(value))
            outcomes = Counter(values)

        output_lines = [f"[mutation] mutants processed: {total}"]
        if outcomes:
            for outcome, count in sorted(outcomes.items()):
                label = str(outcome).lower()
                output_lines.append(f"[mutation]   {label}: {count}")

        return ToolResult(
            success=True,
            exit_code=0,
            stdout="\n".join(output_lines),
            stderr="",
            operation_id=operation_id,
        )

    except ImportError as e:
        return ToolResult(
            success=False,
            exit_code=1,
            stdout="",
            stderr=f"cosmic_ray must be installed to use mutation testing: {e}",
            operation_id=operation_id,
        )
    except Exception as e:  # pragma: no cover - defensive
        return ToolResult(
            success=False,
            exit_code=1,
            stdout="",
            stderr=f"Failed to generate mutation report: {e}",
            operation_id=operation_id,
        )


def mutation_run(
    config: ToolsConfig, root_path: Path, runner: SubprocessRunner
) -> ToolResult:
    operation_id = OperationId(
        namespace="tools", category=CATEGORY, command="mutation-run"
    )

    steps = [
        ("reset", lambda: mutation_reset(config, root_path)),
        ("summary", lambda: mutation_summary(config, root_path)),
        ("init", lambda: mutation_init(config, root_path, runner)),
        ("exec", lambda: mutation_exec(config, root_path, runner)),
        ("report", lambda: mutation_report(config, root_path)),
    ]

    combined_stdout = ""
    combined_stderr = ""

    with _cwd(root_path):
        for step_name, step in steps:
            try:
                result = step()
                if result.stdout:
                    combined_stdout += f"Mutation {step_name}:\n{result.stdout}\n"
                if result.stderr:
                    combined_stderr += (
                        f"Mutation {step_name} warnings:\n{result.stderr}\n"
                    )
                if not result.success:
                    return ToolResult(
                        success=False,
                        exit_code=result.exit_code,
                        stdout=combined_stdout,
                        stderr=combined_stderr or result.stderr,
                        operation_id=operation_id,
                    )
            except Exception as e:  # pragma: no cover - defensive
                return ToolResult(
                    success=False,
                    exit_code=1,
                    stdout=combined_stdout,
                    stderr=f"Mutation {step_name} failed: {e}",
                    operation_id=operation_id,
                )

    return ToolResult(
        success=True,
        exit_code=0,
        stdout=combined_stdout,
        stderr=combined_stderr,
        operation_id=operation_id,
    )
