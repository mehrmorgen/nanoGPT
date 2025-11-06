"""Unit tests for ml_playground.tools.testing.mutation.

Covers init fallback, exec precondition, reset behavior, summary/report
error paths, and happy paths with faked cosmic_ray and sqlite.
"""

from __future__ import annotations

import builtins
import sys
from pathlib import Path
from types import ModuleType

import pytest

from ml_playground.tools.core import config as config_module
from ml_playground.tools.core.config import ToolsConfig
from ml_playground.tools.core.errors import ToolExecutionError
from ml_playground.tools.core.interfaces import OperationId, ToolResult
from ml_playground.tools.testing import mutation as mutation_mod
from tests.unit.tools.fakes import FakeSubprocessRunner, create_failure_result


# ---------- Shared config ----------


def _config() -> ToolsConfig:
    return ToolsConfig(
        testing=config_module.TestToolsConfig(
            timeout=120, coverage_threshold=80.0, parallel_workers=2
        )
    )


# ---------- Init fallback path ----------


class InitFailRunner(FakeSubprocessRunner):
    def run_uv_command(  # type: ignore[override]
        self,
        args: list[str],
        *,
        cwd: Path | None = None,
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


def test_mutation_reset_handles_existing_session(tmp_path: Path, monkeypatch) -> None:
    cfg = _config()
    session = tmp_path / ".cache" / "cosmic-ray" / "session.sqlite"
    session.parent.mkdir(parents=True, exist_ok=True)
    session.write_text("data", encoding="utf-8")
    monkeypatch.chdir(tmp_path)
    result = mutation_mod.mutation_reset(cfg, tmp_path)
    assert result.success is True
    assert "Removed Cosmic Ray session" in result.stdout


# ---------- Summary/report error paths ----------


def test_mutation_report_when_session_missing_with_cosmic_ray(
    tmp_path: Path, monkeypatch
) -> None:
    """When cosmic_ray is present but session is missing, we return a friendly success message."""
    cfg = _config()

    # Install minimal cosmic_ray modules
    base = ModuleType("cosmic_ray")
    mod_config = ModuleType("cosmic_ray.config")

    def load_config(_path: str):
        return {"session": {"path": ".cache/cosmic-ray/session.sqlite"}}

    mod_config.load_config = load_config  # type: ignore[attr-defined]

    originals = {
        "cosmic_ray": sys.modules.get("cosmic_ray"),
        "cosmic_ray.config": sys.modules.get("cosmic_ray.config"),
    }
    sys.modules["cosmic_ray"] = base
    sys.modules["cosmic_ray.config"] = mod_config

    try:
        monkeypatch.chdir(tmp_path)
        result = mutation_mod.mutation_report(cfg, tmp_path)
        assert result.success is True
        assert "session file not found" in (result.stdout or "").lower()
    finally:
        for k, v in originals.items():
            if v is None:
                sys.modules.pop(k, None)
            else:
                sys.modules[k] = v


def test_mutation_summary_import_error(tmp_path: Path, monkeypatch) -> None:
    cfg = _config()
    monkeypatch.chdir(tmp_path)

    # Force ImportError for cosmic_ray*
    def raising_import(name, globals=None, locals=None, fromlist=(), level=0):  # type: ignore[no-redef]
        if str(name).startswith("cosmic_ray"):
            raise ImportError("no cosmic_ray")
        return __import__(name, globals, locals, fromlist, level)

    monkeypatch.setattr(builtins, "__import__", raising_import)
    result = mutation_mod.mutation_summary(cfg, tmp_path)
    assert result.success is False
    err = result.stderr or ""
    assert (
        "cosmic_ray must be installed" in err
        or "Failed to generate mutation summary" in err
    )


# ---------- Happy path with fakes ----------


class _FakeConn:
    def __init__(self) -> None:
        self._rows = [(2,), ("KILLED",), ("SURVIVED",)]

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
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


def test_mutation_summary_and_report_success(tmp_path: Path, monkeypatch) -> None:
    cfg = _config()

    # Prepare a fake cosmic_ray API and sqlite
    base = ModuleType("cosmic_ray")
    mod_config = ModuleType("cosmic_ray.config")
    mod_modules = ModuleType("cosmic_ray.modules")

    session = tmp_path / ".cache" / "cosmic-ray" / "session.sqlite"
    session.parent.mkdir(parents=True, exist_ok=True)

    def load_config(_path: str):
        return {
            "session": {"path": str(session)},
            "test-runner": {"command": "pytest"},
            "modules": {"include": ["src"]},
        }

    def find_modules(_cfg):
        return ["pkg.alpha", "pkg.beta"]

    mod_config.load_config = load_config  # type: ignore[attr-defined]
    mod_modules.find_modules = find_modules  # type: ignore[attr-defined]

    originals = {
        "cosmic_ray": sys.modules.get("cosmic_ray"),
        "cosmic_ray.config": sys.modules.get("cosmic_ray.config"),
        "cosmic_ray.modules": sys.modules.get("cosmic_ray.modules"),
        "sqlite3": sys.modules.get("sqlite3"),
    }
    sys.modules["cosmic_ray"] = base
    sys.modules["cosmic_ray.config"] = mod_config
    sys.modules["cosmic_ray.modules"] = mod_modules
    sys.modules["sqlite3"] = _FakeSqliteMod()

    try:
        # Summary should succeed and list modules
        monkeypatch.chdir(tmp_path)
        summary = mutation_mod.mutation_summary(cfg, tmp_path)
        assert summary.success is True
        assert "modules to mutate: 2" in summary.stdout

        # Create a trivial session (our fake sqlite ignores real file)
        session.write_text("data", encoding="utf-8")

        # Report should succeed and summarize outcomes
        report = mutation_mod.mutation_report(cfg, tmp_path)
        assert report.success is True
        assert "mutants processed: 2" in report.stdout
        assert "killed" in report.stdout.lower() or "survived" in report.stdout.lower()
    finally:
        # Restore modules
        for k, v in originals.items():
            if v is None:
                sys.modules.pop(k, None)
            else:
                sys.modules[k] = v


# ---------- Pipeline tests ----------


def test_mutation_run_reports_failure_when_summary_fails(
    tmp_path: Path, monkeypatch
) -> None:
    cfg = _config()

    def raising_import(name, globals=None, locals=None, fromlist=(), level=0):  # type: ignore[no-redef]
        if str(name).startswith("cosmic_ray"):
            raise ImportError("no cosmic_ray")
        return __import__(name, globals, locals, fromlist, level)

    monkeypatch.setattr(builtins, "__import__", raising_import)
    result = mutation_mod.mutation_run(cfg, tmp_path, FakeSubprocessRunner())
    assert result.success is False
    combined = (result.stdout or "") + (result.stderr or "")
    assert "Mutation summary" in combined


def test_mutation_run_success_with_existing_session(
    tmp_path: Path, monkeypatch
) -> None:
    cfg = _config()

    # Minimal cosmic_ray + sqlite so summary and report pass
    base = ModuleType("cosmic_ray")
    mod_config = ModuleType("cosmic_ray.config")
    mod_modules = ModuleType("cosmic_ray.modules")
    mod_config.load_config = lambda _p: {
        "session": {"path": ".cache/cosmic-ray/session.sqlite"},
        "test-runner": {"command": "pytest"},
        "modules": {"include": ["src"]},
    }  # type: ignore[attr-defined]
    mod_modules.find_modules = lambda _c: ["pkg.alpha"]  # type: ignore[attr-defined]

    originals = {
        "cosmic_ray": sys.modules.get("cosmic_ray"),
        "cosmic_ray.config": sys.modules.get("cosmic_ray.config"),
        "cosmic_ray.modules": sys.modules.get("cosmic_ray.modules"),
        "sqlite3": sys.modules.get("sqlite3"),
    }
    sys.modules["cosmic_ray"] = base
    sys.modules["cosmic_ray.config"] = mod_config
    sys.modules["cosmic_ray.modules"] = mod_modules
    sys.modules["sqlite3"] = _FakeSqliteMod()

    class SuccessRunner(FakeSubprocessRunner):
        def run_uv_command(  # type: ignore[override]
            self,
            args: list[str],
            *,
            cwd: Path | None = None,
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

    try:
        monkeypatch.chdir(tmp_path)
        result = mutation_mod.mutation_run(cfg, tmp_path, SuccessRunner())
        assert result.success is True
        assert "Mutation report:" in (result.stdout or "")
    finally:
        for k, v in originals.items():
            if v is None:
                sys.modules.pop(k, None)
            else:
                sys.modules[k] = v
