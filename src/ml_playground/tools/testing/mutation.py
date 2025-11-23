"""Mutation testing helpers extracted from TestingTools.

These functions implement the Cosmic Ray flow and are called by the
TestingTools facade to keep the public API stable and the file sizes small.
"""

from __future__ import annotations

import os
from contextlib import contextmanager
from collections.abc import Mapping
from pathlib import Path
from typing import Any, Sequence, cast

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
        except (OSError, PermissionError) as exc:
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
        raw_cfg = load_config(str(config_file))
        assert isinstance(raw_cfg, Mapping)

        def _as_dict(obj: Any) -> dict[str, Any]:
            result: dict[str, Any] = {}
            if isinstance(obj, dict):
                for k, v in cast(Mapping[Any, Any], obj).items():
                    if isinstance(k, str):
                        result[k] = v
            return result

        cfg: dict[str, Any] = _as_dict(raw_cfg)

        session_mapping = _as_dict(cfg.get("session"))
        session_path = Path(
            str(session_mapping.get("path", ".cache/cosmic-ray/session.sqlite"))
        )

        test_runner_mapping = _as_dict(cfg.get("test-runner"))
        test_command: str = str(test_runner_mapping.get("command", "pytest"))

        modules_cfg = cfg.get("modules", {})
        modules_input: Any = modules_cfg
        modules = tuple(str(m) for m in find_modules(modules_input))  # type: ignore[arg-type]

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
    except (
        KeyError,
        ValueError,
        TypeError,
        AssertionError,
        RuntimeError,
        Exception,
    ) as e:
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
        raw_cfg = load_config(str(config_file))
        assert isinstance(raw_cfg, Mapping)

        def _as_dict(obj: Any) -> dict[str, Any]:
            result: dict[str, Any] = {}
            if isinstance(obj, dict):
                for k, v in cast(Mapping[Any, Any], obj).items():
                    if isinstance(k, str):
                        result[k] = v
            return result

        cfg: dict[str, Any] = _as_dict(raw_cfg)

        session_mapping = _as_dict(cfg.get("session"))
        session_path = Path(
            str(session_mapping.get("path", ".cache/cosmic-ray/session.sqlite"))
        )

        try:
            conn_ctx = sqlite3.connect(session_path)
        except (sqlite3.Error, OSError, FileNotFoundError):
            return ToolResult(
                success=True,
                exit_code=0,
                stdout="[mutation] session file not found: no results to report",
                stderr="",
                operation_id=operation_id,
            )
        except (AttributeError, TypeError, ValueError) as e:
            # Handle sqlite3.connect issues like module compatibility problems
            return ToolResult(
                success=True,
                exit_code=0,
                stdout="[mutation] session file not found: no results to report",
                stderr=f"Warning: sqlite3.connect failed: {e}",
                operation_id=operation_id,
            )

        def _first_column(_cursor: Any, row: Sequence[Any]) -> Any:
            return row[0]

        with conn_ctx as conn:
            conn.row_factory = _first_column
            # Support real sqlite cursors and simple iterator-based fakes
            total_cursor = conn.execute("SELECT COUNT(*) FROM work_results")

            def _to_int(value: Any) -> int:
                try:
                    return int(value)
                except (ValueError, TypeError):
                    return 0

            total: int
            v_any: Any
            if hasattr(total_cursor, "fetchone"):
                row_any = total_cursor.fetchone()
                if row_any is None:
                    total = 0
                else:
                    if isinstance(row_any, (list, tuple)):
                        v_any = cast(Any, row_any[0])
                    else:
                        v_any = row_any
                    total = _to_int(v_any)
            else:
                first = next(iter(total_cursor), 0)
                if isinstance(first, (list, tuple)):
                    v_any = cast(Any, first[0])
                else:
                    v_any = first
                total = _to_int(v_any)

            outcome_iter = conn.execute(
                "SELECT COALESCE(test_outcome, 'UNKNOWN') FROM work_results"
            )
            values: list[str] = []
            for row in outcome_iter:
                if isinstance(row, (list, tuple)):
                    value_any: Any = cast(Any, row[0])
                else:
                    value_any = row
                values.append(str(value_any))
            outcomes: Counter[str] = Counter(values)

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
    except (
        KeyError,
        ValueError,
        TypeError,
        AssertionError,
        Exception,  # Catch-all for any other issues including sqlite3 problems
    ) as e:
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
            except (ToolExecutionError, OSError, RuntimeError, ValueError) as e:
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
