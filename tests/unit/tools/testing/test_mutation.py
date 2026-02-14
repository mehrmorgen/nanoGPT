"""Unit tests for ml_playground.tools.testing.mutation.

Covers init fallback, exec precondition, reset behavior, summary/report
error paths, and happy paths with faked cosmic_ray and sqlite.
"""

from __future__ import annotations

import builtins
import os
import sys
from contextlib import contextmanager
from pathlib import Path
from types import ModuleType
from typing import Any, Callable, Iterator

import pytest

import ml_playground.tools.core.config as config_module
from ml_playground.tools.core.config import ToolsConfig
from ml_playground.tools.core.errors import ToolExecutionError
from ml_playground.tools.core.interfaces import OperationId, ToolResult
import ml_playground.tools.testing.mutation as mutation_mod
from tests.unit.tools.fakes import FakeSubprocessRunner, create_failure_result


# ---------- Shared config and helpers ----------


_MISSING = object()


def _config() -> ToolsConfig:
    return ToolsConfig(
        testing=config_module.TestToolsConfig(
            timeout=120, coverage_threshold=80.0, parallel_workers=2
        )
    )


@contextmanager
def override_cwd(path: Path) -> Iterator[None]:
    previous = Path.cwd()
    os.chdir(path)
    try:
        yield
    finally:
        os.chdir(previous)


@contextmanager
def override_attr(obj: Any, name: str, value: Any) -> Iterator[None]:
    original = getattr(obj, name, _MISSING)
    setattr(obj, name, value)
    try:
        yield
    finally:
        if original is _MISSING:
            delattr(obj, name)
        else:
            setattr(obj, name, original)


@contextmanager
def install_modules(modules: dict[str, ModuleType]) -> Iterator[None]:
    originals = {name: sys.modules.get(name) for name in modules}
    for name, module in modules.items():
        sys.modules[name] = module
    try:
        yield
    finally:
        for name, original in originals.items():
            if original is None:
                sys.modules.pop(name, None)
            else:
                sys.modules[name] = original


class CosmicRayConfigModule(ModuleType):
    def __init__(self, loader: Callable[[str], dict[str, Any]]) -> None:
        super().__init__("cosmic_ray.config")
        self._loader = loader

    def load_config(self, path: str) -> dict[str, Any]:
        return self._loader(path)


class CosmicRayModulesModule(ModuleType):
    def __init__(self, finder: Callable[[dict[str, Any]], list[str]]) -> None:
        super().__init__("cosmic_ray.modules")
        self._finder = finder

    def find_modules(self, cfg: dict[str, Any]) -> list[str]:
        return self._finder(cfg)


class FakeMutationService:
    def load_config(self, path: str | Path) -> dict[str, Any]:
        from cosmic_ray.config import load_config

        return dict(load_config(str(path)))

    def find_modules(self, config: Any) -> list[Any]:
        from cosmic_ray.modules import find_modules

        return list(find_modules(config))


# ---------- Init fallback path ----------


class InitFailRunner(FakeSubprocessRunner):
    def run_uv_command(
        self,
        args: list[str],
        *,
        cwd: str | Path | None = None,
        env: dict[str, str] | None = None,
        timeout: int | None = None,
        operation_id: OperationId,
        python: str | None = None,
        no_project: bool = False,
    ) -> ToolResult:
        # Force failure to exercise the "reuse existing session" fallback path
        return create_failure_result(operation_id, stderr="init failed")


def test_mutation_init_fallback_success(tmp_path: Path) -> None:
    cfg = _config()
    runner = InitFailRunner()
    result = mutation_mod.mutation_init(cfg, tmp_path, runner)
    assert result.success is True
    assert "init" in result.stdout.lower()
    assert result.exit_code == 0


# ---------- Exec precondition ----------


def test_mutation_exec_raises_without_session(tmp_path: Path) -> None:
    cfg = _config()
    # Ensure session file is absent
    session = tmp_path / ".cache" / "cosmic-ray" / "session.sqlite"
    if session.exists():
        session.unlink()
    with pytest.raises(ToolExecutionError):
        mutation_mod.mutation_exec(cfg, tmp_path, FakeSubprocessRunner())


# ---------- Reset behavior ----------


def test_mutation_reset_handles_existing_session(tmp_path: Path) -> None:
    cfg = _config()
    session = tmp_path / ".cache" / "cosmic-ray" / "session.sqlite"
    session.parent.mkdir(parents=True, exist_ok=True)
    session.write_text("data", encoding="utf-8")
    with override_cwd(tmp_path):
        result = mutation_mod.mutation_reset(cfg, tmp_path)
    assert result.success is True
    assert "Removed Cosmic Ray session" in result.stdout


def test_mutation_reset_raises_tool_execution_error_on_unlink_failure(
    tmp_path: Path,
) -> None:
    """mutation_reset should wrap unexpected unlink failures in ToolExecutionError."""

    cfg = _config()
    # Use the same relative path that the mutation module uses
    session = tmp_path / ".cache" / "cosmic-ray" / "session.sqlite"
    session.parent.mkdir(parents=True, exist_ok=True)
    session.write_text("data", encoding="utf-8")

    def failing_unlink(self: Path) -> None:  # type: ignore[override]
        # Check if this is the session file relative to current working directory
        current_session = Path(".cache/cosmic-ray/session.sqlite")
        if self.resolve() == current_session.resolve():
            raise OSError("boom")
        # Call the original method using super() to avoid recursion
        # Since we're overriding a class method, we need to use the original implementation
        import os

        if self.is_file():
            os.remove(self)
        elif self.is_dir():
            os.rmdir(self)

    with override_cwd(tmp_path):
        with override_attr(Path, "unlink", failing_unlink):  # type: ignore[arg-type]
            with pytest.raises(
                ToolExecutionError, match="Failed to remove Cosmic Ray session file"
            ):
                mutation_mod.mutation_reset(cfg, tmp_path)


# ---------- Summary/report error paths ----------


def test_mutation_report_when_session_missing_with_cosmic_ray(tmp_path: Path) -> None:
    """When cosmic_ray is present but session is missing, we return a friendly success message."""
    cfg = _config()

    # Install minimal cosmic_ray modules
    base = ModuleType("cosmic_ray")
    config_module = CosmicRayConfigModule(
        lambda _path: {"session": {"path": ".cache/cosmic-ray/session.sqlite"}}
    )

    with install_modules(
        {
            "cosmic_ray": base,
            "cosmic_ray.config": config_module,
        }
    ):
        with override_cwd(tmp_path):
            result = mutation_mod.mutation_report(cfg, tmp_path, FakeMutationService())
    assert result.success is True
    assert "session file not found" in (result.stdout or "").lower()


def test_mutation_summary_import_error(tmp_path: Path) -> None:
    cfg = _config()

    # Force ImportError for cosmic_ray*
    def raising_import(
        name: str,
        globals: Any = None,
        locals: Any = None,
        fromlist: tuple[Any, ...] = (),
        level: int = 0,
    ) -> Any:
        if str(name).startswith("cosmic_ray"):
            raise ImportError("no cosmic_ray")
        return __import__(name, globals, locals, fromlist, level)

    with override_cwd(tmp_path):
        with override_attr(builtins, "__import__", raising_import):
            result = mutation_mod.mutation_summary(cfg, tmp_path, FakeMutationService())
    assert result.success is False
    err = result.stderr or ""
    assert (
        "cosmic_ray must be installed" in err
        or "Failed to generate mutation summary" in err
    )


def test_mutation_summary_handles_unexpected_exceptions(tmp_path: Path) -> None:
    """mutation_summary should catch non-ImportError exceptions and fail gracefully."""

    cfg = _config()

    base = ModuleType("cosmic_ray")

    def _loader(_path: str) -> dict[str, Any]:
        # Ensure we exercise the generic exception handler in mutation_summary.
        raise RuntimeError("loader boom")

    # Provide a stub modules implementation so imports succeed and we reach
    # the failing loader call instead of hitting the ImportError path.
    modules_module = CosmicRayModulesModule(lambda _cfg: ["pkg.alpha"])
    config_module = CosmicRayConfigModule(_loader)

    with install_modules(
        {
            "cosmic_ray": base,
            "cosmic_ray.config": config_module,
            "cosmic_ray.modules": modules_module,
        }
    ):
        with override_cwd(tmp_path):
            result = mutation_mod.mutation_summary(cfg, tmp_path, FakeMutationService())

    assert result.success is False
    assert "Failed to generate mutation summary" in (result.stderr or "")


def test_mutation_report_handles_unexpected_exceptions(tmp_path: Path) -> None:
    """mutation_report should catch unexpected exceptions and return failure ToolResult."""

    cfg = _config()

    base = ModuleType("cosmic_ray")

    def _loader(_path: str) -> dict[str, Any]:
        raise RuntimeError("broken config")

    config_module = CosmicRayConfigModule(_loader)

    with install_modules({"cosmic_ray": base, "cosmic_ray.config": config_module}):
        with override_cwd(tmp_path):
            result = mutation_mod.mutation_report(cfg, tmp_path, FakeMutationService())

    assert result.success is False
    assert "Failed to generate mutation report" in (result.stderr or "")


# ---------- Happy path with fakes ----------


class _FakeConn:
    def __init__(self) -> None:
        self._rows = [(2,), ("KILLED",), ("SURVIVED",)]

    def __enter__(self) -> _FakeConn:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        _tb: Any,
    ) -> bool:
        return False

    def execute(self, sql: str):
        if "COUNT(*)" in sql:
            return iter([self._rows[0]])
        return iter(self._rows[1:])


class _FakeSqliteMod(ModuleType):
    def __init__(self) -> None:
        super().__init__("sqlite3")

    def connect(self, _path):  # type: ignore[override]
        return _FakeConn()


def test_mutation_summary_and_report_success(tmp_path: Path) -> None:
    cfg = _config()

    # Prepare a fake cosmic_ray API and sqlite
    base = ModuleType("cosmic_ray")
    session = tmp_path / ".cache" / "cosmic-ray" / "session.sqlite"
    session.parent.mkdir(parents=True, exist_ok=True)

    config_module = CosmicRayConfigModule(
        lambda _path: {
            "session": {"path": str(session)},
            "test-runner": {"command": "pytest"},
            "modules": {"include": ["src"]},
        }
    )
    modules_module = CosmicRayModulesModule(lambda _cfg: ["pkg.alpha", "pkg.beta"])

    with install_modules(
        {
            "cosmic_ray": base,
            "cosmic_ray.config": config_module,
            "cosmic_ray.modules": modules_module,
            "sqlite3": _FakeSqliteMod(),
        }
    ):
        with override_cwd(tmp_path):
            service = FakeMutationService()
            summary = mutation_mod.mutation_summary(cfg, tmp_path, service)
            assert summary.success is True
            assert "modules to mutate: 2" in summary.stdout

            session.write_text("data", encoding="utf-8")
            report = mutation_mod.mutation_report(cfg, tmp_path, service)
            assert report.success is True
            assert "mutants processed: 2" in report.stdout
            assert (
                "killed" in report.stdout.lower() or "survived" in report.stdout.lower()
            )


# ---------- Pipeline tests ----------


def test_mutation_run_reports_failure_when_summary_fails(tmp_path: Path) -> None:
    cfg = _config()

    def raising_import(
        name: str,
        globals: Any = None,
        locals: Any = None,
        fromlist: tuple[Any, ...] = (),
        level: int = 0,
    ) -> Any:
        if str(name).startswith("cosmic_ray"):
            raise ImportError("no cosmic_ray")
        return __import__(name, globals, locals, fromlist, level)

    with override_attr(builtins, "__import__", raising_import):
        result = mutation_mod.mutation_run(
            cfg, tmp_path, FakeSubprocessRunner(), FakeMutationService()
        )
    assert result.success is False
    combined = (result.stdout or "") + (result.stderr or "")
    assert "Mutation summary" in combined


def test_mutation_run_success_with_existing_session(tmp_path: Path) -> None:
    cfg = _config()

    # Minimal cosmic_ray + sqlite so summary and report pass
    base = ModuleType("cosmic_ray")
    config_module = CosmicRayConfigModule(
        lambda _path: {
            "session": {"path": ".cache/cosmic-ray/session.sqlite"},
            "test-runner": {"command": "pytest"},
            "modules": {"include": ["src"]},
        }
    )
    modules_module = CosmicRayModulesModule(lambda _cfg: ["pkg.alpha"])

    class SuccessRunner(FakeSubprocessRunner):
        def run_uv_command(
            self,
            args: list[str],
            *,
            cwd: str | Path | None = None,
            env: dict[str, str] | None = None,
            timeout: int | None = None,
            operation_id: OperationId,
            python: str | None = None,
            no_project: bool = False,
        ) -> ToolResult:
            if args[:2] == ["cosmic-ray", "init"]:
                sess = Path(".cache/cosmic-ray/session.sqlite")
                sess.parent.mkdir(parents=True, exist_ok=True)
                sess.write_text("ok", encoding="utf-8")
                return ToolResult(
                    success=True,
                    exit_code=0,
                    stdout="init ok",
                    stderr="",
                    operation_id=operation_id,
                )
            return ToolResult(
                success=True,
                exit_code=0,
                stdout="ok",
                stderr="",
                operation_id=operation_id,
            )

    with install_modules(
        {
            "cosmic_ray": base,
            "cosmic_ray.config": config_module,
            "cosmic_ray.modules": modules_module,
            "sqlite3": _FakeSqliteMod(),
        }
    ):
        with override_cwd(tmp_path):
            result = mutation_mod.mutation_run(
                cfg, tmp_path, SuccessRunner(), FakeMutationService()
            )
    assert result.success is True
    assert "Mutation report:" in (result.stdout or "")


def test_mutation_run_handles_unexpected_step_exception(tmp_path: Path) -> None:
    """mutation_run should wrap unexpected step exceptions in a failure ToolResult."""

    cfg = _config()

    # Provide a minimal cosmic_ray + sqlite environment so that summary,
    # init, and report can succeed and we specifically exercise the exec
    # step's defensive exception handling.
    base = ModuleType("cosmic_ray")
    session = tmp_path / ".cache" / "cosmic-ray" / "session.sqlite"

    config_module = CosmicRayConfigModule(
        lambda _path: {
            "session": {"path": str(session)},
            "test-runner": {"command": "pytest"},
            "modules": {"include": ["src"]},
        }
    )
    modules_module = CosmicRayModulesModule(lambda _cfg: ["pkg.alpha"])

    class BoomRunner(FakeSubprocessRunner):
        def run_uv_command(
            self,
            args: list[str],
            *,
            cwd: str | Path | None = None,
            env: dict[str, str] | None = None,
            timeout: int | None = None,
            operation_id: OperationId,
            python: str | None = None,
            no_project: bool = False,
        ) -> ToolResult:
            # Allow init to succeed so the session file is created,
            # but raise during exec to trigger the defensive path.
            if args[:2] == ["cosmic-ray", "init"]:
                sess = Path(".cache/cosmic-ray/session.sqlite")
                sess.parent.mkdir(parents=True, exist_ok=True)
                sess.write_text("ok", encoding="utf-8")
                return ToolResult(
                    success=True,
                    exit_code=0,
                    stdout="init ok",
                    stderr="",
                    operation_id=operation_id,
                )
            if args[:2] == ["cosmic-ray", "exec"]:
                raise RuntimeError("exec boom")
            return ToolResult(
                success=True,
                exit_code=0,
                stdout="ok",
                stderr="",
                operation_id=operation_id,
            )

    with install_modules(
        {
            "cosmic_ray": base,
            "cosmic_ray.config": config_module,
            "cosmic_ray.modules": modules_module,
            "sqlite3": _FakeSqliteMod(),
        }
    ):
        with override_cwd(tmp_path):
            result = mutation_mod.mutation_run(
                cfg, tmp_path, BoomRunner(), FakeMutationService()
            )

    assert result.success is False
    assert "Mutation exec failed" in (result.stderr or "")


def test_mutation_summary_truncates_module_list(tmp_path: Path) -> None:
    """mutation_summary should truncate the module list if there are too many."""
    cfg = _config()

    base = ModuleType("cosmic_ray")
    config_module = CosmicRayConfigModule(
        lambda _path: {
            "session": {"path": ".cache/cosmic-ray/session.sqlite"},
            "test-runner": {"command": "pytest"},
        }
    )
    # Return 10 modules to trigger truncation
    modules_module = CosmicRayModulesModule(
        lambda _cfg: [f"pkg.mod{i}" for i in range(10)]
    )

    with install_modules(
        {
            "cosmic_ray": base,
            "cosmic_ray.config": config_module,
            "cosmic_ray.modules": modules_module,
        }
    ):
        with override_cwd(tmp_path):
            result = mutation_mod.mutation_summary(cfg, tmp_path, FakeMutationService())

    assert result.success is True
    assert "modules to mutate: 10" in result.stdout
    assert "... and 5 more" in result.stdout


class _FakeCursorWithFetch:
    """Simulate a real sqlite3 cursor with fetchone."""

    def __init__(self, rows: list[Any]) -> None:
        self._rows = rows
        self._idx = 0

    def fetchone(self) -> Any | None:
        if self._idx >= len(self._rows):
            return None
        row = self._rows[self._idx]
        self._idx += 1
        return row

    def __iter__(self) -> Iterator[Any]:
        return iter(self._rows)


class _FakeConnWithFetch:
    def __init__(self, rows: list[Any]) -> None:
        self._rows = rows

    def __enter__(self) -> _FakeConnWithFetch:
        return self

    def __exit__(self, *args: Any) -> None:
        pass

    def execute(self, sql: str) -> Any:
        if "COUNT(*)" in sql:
            if not self._rows:
                return _FakeCursorWithFetch([])
            return _FakeCursorWithFetch([self._rows[0]])
        if not self._rows:
            return _FakeCursorWithFetch([])
        return _FakeCursorWithFetch(self._rows[1:])


def test_mutation_report_handles_real_cursor_behavior(tmp_path: Path) -> None:
    """mutation_report should handle cursors that have fetchone() (like real sqlite3)."""
    cfg = _config()

    base = ModuleType("cosmic_ray")
    config_module = CosmicRayConfigModule(
        lambda _path: {"session": {"path": "session.sqlite"}}
    )

    # Prepare a fake sqlite module that returns a connection with fetchone-capable cursors
    class FakeSqliteWithFetch(ModuleType):
        def __init__(self) -> None:
            super().__init__("sqlite3")

        def connect(self, _path: Any) -> Any:
            # First row is count (tuple), subsequent are outcomes (tuples)
            return _FakeConnWithFetch(
                [
                    (5,),
                    ("KILLED",),
                    ("KILLED",),
                    ("SURVIVED",),
                    ("SURVIVED",),
                    ("TIMEOUT",),
                ]
            )

    with install_modules(
        {
            "cosmic_ray": base,
            "cosmic_ray.config": config_module,
            "sqlite3": FakeSqliteWithFetch(),
        }
    ):
        with override_cwd(tmp_path):
            result = mutation_mod.mutation_report(cfg, tmp_path, FakeMutationService())

    assert result.success is True
    assert "mutants processed: 5" in result.stdout
    assert "killed: 2" in result.stdout
    assert "survived: 2" in result.stdout
    assert "timeout: 1" in result.stdout


def test_mutation_report_handles_empty_result_set(tmp_path: Path) -> None:
    """mutation_report should handle empty result sets from fetchone."""
    cfg = _config()
    base = ModuleType("cosmic_ray")
    config_module = CosmicRayConfigModule(
        lambda _path: {"session": {"path": "session.sqlite"}}
    )

    class FakeSqliteEmpty(ModuleType):
        def __init__(self) -> None:
            super().__init__("sqlite3")

        def connect(self, _path: Any) -> Any:
            # Return empty rows to trigger None return from fetchone
            return _FakeConnWithFetch([])

    with install_modules(
        {
            "cosmic_ray": base,
            "cosmic_ray.config": config_module,
            "sqlite3": FakeSqliteEmpty(),
        }
    ):
        with override_cwd(tmp_path):
            result = mutation_mod.mutation_report(cfg, tmp_path, FakeMutationService())

    assert result.success is True
    assert "mutants processed: 0" in result.stdout


def test_mutation_report_handles_connection_failure(tmp_path: Path) -> None:
    """mutation_report should handle sqlite3.connect failure gracefully."""
    cfg = _config()
    base = ModuleType("cosmic_ray")
    config_module = CosmicRayConfigModule(
        lambda _path: {"session": {"path": "session.sqlite"}}
    )

    class FakeSqliteBoom(ModuleType):
        def __init__(self) -> None:
            super().__init__("sqlite3")
            # Add the Error attribute that real sqlite3 has
            self.Error = Exception

        def connect(self, _path: Any) -> Any:
            raise Exception("db connect failed")

    with install_modules(
        {
            "cosmic_ray": base,
            "cosmic_ray.config": config_module,
            "sqlite3": FakeSqliteBoom(),
        }
    ):
        with override_cwd(tmp_path):
            result = mutation_mod.mutation_report(cfg, tmp_path, FakeMutationService())

    assert result.success is True  # Should be success but with warning message
    assert "session file not found" in result.stdout


def test_mutation_report_handles_scalar_rows_and_bad_ints(tmp_path: Path) -> None:
    """mutation_report should handle scalar cursor results and parsing errors."""
    cfg = _config()
    base = ModuleType("cosmic_ray")
    config_module = CosmicRayConfigModule(
        lambda _path: {"session": {"path": "session.sqlite"}}
    )

    class FakeSqliteOdd(ModuleType):
        def __init__(self) -> None:
            super().__init__("sqlite3")

        def connect(self, _path: Any) -> Any:
            # First row is scalar count (not tuple)
            # Subsequent rows include a non-string scalar that fails int conversion?
            # Wait, outcome rows are just counted.
            # Count row needs to be parseable as int.
            # Let's fail the count parsing.
            return _FakeConnWithFetch(["not-an-int", "outcome1"])

    with install_modules(
        {
            "cosmic_ray": base,
            "cosmic_ray.config": config_module,
            "sqlite3": FakeSqliteOdd(),
        }
    ):
        with override_cwd(tmp_path):
            result = mutation_mod.mutation_report(cfg, tmp_path, FakeMutationService())

    assert result.success is True
    # _to_int returns 0 on failure
    assert "mutants processed: 0" in result.stdout
    assert "outcome1" in result.stdout
