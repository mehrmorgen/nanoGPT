"""Mutation testing helpers extracted from TestingTools.

These functions implement the Cosmic Ray flow and are called by the
TestingTools facade to keep the public API stable and the file sizes small.
"""

from __future__ import annotations

import os
from collections.abc import Iterable, Mapping, Sequence
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Sequence as TypingSequence, cast

from ml_playground.tools.core.errors import ToolExecutionError
from ml_playground.tools.core.interfaces import OperationId, ToolResult
from ml_playground.tools.core.config import ToolsConfig
from ml_playground.tools.utils.subprocess_utils import SubprocessRunner
from ml_playground.tools.testing.services.mutation_service import MutationService


CATEGORY = "test"

ConfigMapping = Mapping[str, object]
ConfigDict = dict[str, object]
ModulesConfig = ConfigMapping | Sequence[str] | str
Row = tuple[object, ...]


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

    session_file = root_path / ".cache/cosmic-ray/session.sqlite"
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


def mutation_summary(
    config: ToolsConfig, root_path: Path, service: MutationService
) -> ToolResult:
    operation_id = OperationId(
        namespace="tools", category=CATEGORY, command="mutation-summary"
    )

    try:
        config_file = Path("pyproject.toml")
        raw_cfg = service.load_config(config_file)
        assert isinstance(raw_cfg, Mapping)

        def _as_dict(obj: object | None) -> ConfigDict:
            result: ConfigDict = {}
            if obj is None or not isinstance(obj, Mapping):
                return result
            mapping = cast(Mapping[object, object], obj)
            for key_obj, value in mapping.items():
                key_str = str(key_obj)
                result[key_str] = value
            return result

        def _coerce_str(value: object | None, default: str) -> str:
            return str(value) if value is not None else default

        cfg: ConfigDict = _as_dict(raw_cfg)

        session_mapping = _as_dict(cfg.get("session"))
        session_path_str: object | None = session_mapping.get(
            "path", ".cache/cosmic-ray/session.sqlite"
        )
        session_path = Path(
            _coerce_str(session_path_str, ".cache/cosmic-ray/session.sqlite")
        )

        test_runner_mapping = _as_dict(cfg.get("test-runner"))
        test_command_obj: object | None = test_runner_mapping.get("command", "pytest")
        test_command = _coerce_str(test_command_obj, "pytest")

        modules_cfg_obj: object | None = cfg.get("modules")
        modules_cfg = cast(ModulesConfig | None, modules_cfg_obj)
        modules_input: ModulesConfig = modules_cfg if modules_cfg is not None else ()
        modules = tuple(str(module) for module in service.find_modules(modules_input))  # type: ignore[reportAny]

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

    session_file = root_path / ".cache/cosmic-ray/session.sqlite"
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

    session_file = root_path / ".cache/cosmic-ray/session.sqlite"
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


def mutation_report(
    config: ToolsConfig, root_path: Path, service: MutationService
) -> ToolResult:
    operation_id = OperationId(
        namespace="tools", category=CATEGORY, command="mutation-report"
    )

    try:
        from collections import Counter
        import sqlite3

        config_file = Path("pyproject.toml")
        raw_cfg = service.load_config(config_file)
        assert isinstance(raw_cfg, Mapping)

        def _as_dict(obj: object | None) -> ConfigDict:
            result: ConfigDict = {}
            if obj is None or not isinstance(obj, Mapping):
                return result
            mapping = cast(Mapping[object, object], obj)
            for key, value in mapping.items():
                result[str(key)] = value
            return result

        cfg: ConfigDict = _as_dict(raw_cfg)

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

        with conn_ctx as conn:
            # Ensure row_factory is assigned when supported so tests can inspect it
            def _row_factory(
                _cursor: object, row: TypingSequence[Any] | None
            ) -> Any | None:
                return row[0] if row else None

            if hasattr(conn, "row_factory"):
                conn.row_factory = _row_factory  # type: ignore[attr-defined]

            def _to_int(value: object | None) -> int:
                if value is None:
                    return 0
                if isinstance(value, bool):
                    return int(value)
                if isinstance(value, (int, float)):
                    return int(value)
                if isinstance(value, str):
                    try:
                        return int(value)
                    except ValueError:
                        return 0
                return 0

            def _first_or_none(row: object | Row | None) -> object | None:
                if row is None:
                    return None
                if isinstance(row, tuple):
                    typed_row = cast(Row, row)
                    return typed_row[0] if typed_row else None
                return row

            try:
                total_cursor = conn.execute("SELECT COUNT(*) FROM work_results")  # type: ignore[call-arg]
                if hasattr(total_cursor, "fetchone"):
                    total_row = cast(
                        object | Row | None,
                        total_cursor.fetchone(),  # type: ignore[call-arg]
                    )
                else:
                    total_row = next(iter(cast(Iterable[object], total_cursor)), None)
            except Exception:
                total_row = None
            total_value = _first_or_none(total_row)
            total = _to_int(total_value)

            try:
                outcome_iter: Iterable[object] = cast(
                    Iterable[object],
                    conn.execute(
                        "SELECT COALESCE(test_outcome, 'UNKNOWN') FROM work_results"
                    ),  # type: ignore[call-arg]
                )
            except Exception:
                outcome_iter = ()

            values: list[str] = []
            for outcome_row in outcome_iter:
                value = _first_or_none(cast(object | Row | None, outcome_row))
                values.append(str(value))
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
    config: ToolsConfig,
    root_path: Path,
    runner: SubprocessRunner,
    service: MutationService,
) -> ToolResult:
    operation_id = OperationId(
        namespace="tools", category=CATEGORY, command="mutation-run"
    )

    steps = [
        ("reset", lambda: mutation_reset(config, root_path)),
        ("summary", lambda: mutation_summary(config, root_path, service)),
        ("init", lambda: mutation_init(config, root_path, runner)),
        ("exec", lambda: mutation_exec(config, root_path, runner)),
        ("report", lambda: mutation_report(config, root_path, service)),
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
