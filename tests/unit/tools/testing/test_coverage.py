"""Coverage orchestration tests targeting the public coverage APIs."""

from __future__ import annotations

import json
import os
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Dict, Iterator, List

import pytest

import ml_playground.tools.testing.coverage as coverage_module
import ml_playground.tools.core.config as config_module
from ml_playground.tools.core.config import ToolsConfig
from ml_playground.tools.core.errors import ToolExecutionError
from ml_playground.tools.core.interfaces import OperationId, ToolResult
from ml_playground.tools.testing import coverage_helpers
from ml_playground.tools.utils.subprocess_utils import SubprocessRunner
from tests.unit.tools.fakes import (
    create_sample_source_file,
    write_manifest,
    write_coverage_file,
)


@pytest.fixture
def config() -> ToolsConfig:
    return ToolsConfig(
        testing=config_module.TestToolsConfig(
            timeout=300,
            coverage_threshold=80.0,
            parallel_workers=2,
        )
    )


def _cache_dir(root: Path) -> Path:
    cache_dir = root / ".cache"
    cache_dir.mkdir(parents=True, exist_ok=True)
    return cache_dir


def _coverage_dir(root: Path) -> Path:
    coverage_dir = _cache_dir(root) / "coverage"
    coverage_dir.mkdir(parents=True, exist_ok=True)
    return coverage_dir


def _write_matching_manifest(root: Path) -> None:
    fingerprint = coverage_helpers.compute_coverage_fingerprint(root)
    write_manifest(root, fingerprint=fingerprint)


def _prepare_cached_coverage(root: Path, payload: bytes = b"coverage-data") -> Path:
    create_sample_source_file(root)
    coverage_path = write_coverage_file(root, payload=payload)
    _write_matching_manifest(root)
    return coverage_path


@contextmanager
def override_env(name: str, value: str | None) -> Iterator[None]:
    previous = os.environ.get(name)
    if value is None:
        os.environ.pop(name, None)
    else:
        os.environ[name] = value
    try:
        yield
    finally:
        if previous is None:
            os.environ.pop(name, None)
        else:
            os.environ[name] = previous


class CoverageRunner(SubprocessRunner):
    """Protocol-compliant fake for exercising coverage flows."""

    def __init__(self) -> None:
        self.uv_calls: list[list[str]] = []
        self.pytest_calls: list[list[str]] = []
        self.coverage_run_should_fail = False
        self.fail_first_report = False
        self.report_failure_reason = "No source for code"
        self.json_payload: dict[str, Any] = {
            "totals": {
                "num_statements": 10,
                "covered_lines": 10,
                "num_branches": 2,
                "covered_branches": 2,
            },
            "files": {},
        }
        self._report_attempts = 0

    @property
    def report_attempts(self) -> int:
        return self._report_attempts

    def _success(self, operation_id: OperationId, stdout: str = "uv") -> ToolResult:
        return ToolResult(
            success=True,
            exit_code=0,
            stdout=stdout,
            stderr="",
            operation_id=operation_id,
        )

    def run_subprocess(
        self,
        command: List[str],
        *,
        cwd: str | Path | None = None,
        env: Dict[str, str] | None = None,
        timeout: int | None = None,
        operation_id: OperationId,
        capture_output: bool = True,
    ) -> ToolResult:
        return self._success(operation_id, stdout="subprocess")

    def run_pytest_command(
        self,
        args: List[str],
        *,
        cwd: str | Path | None = None,
        env: Dict[str, str] | None = None,
        timeout: int | None = None,
        operation_id: OperationId,
    ) -> ToolResult:
        self.pytest_calls.append(args)
        if env and "COVERAGE_FILE" in env:
            coverage_path = Path(env["COVERAGE_FILE"])
            coverage_path.parent.mkdir(parents=True, exist_ok=True)
            coverage_path.write_bytes(b"pytest-coverage")
        return self._success(operation_id, stdout="pytest")

    def run_uv_command(
        self,
        args: List[str],
        *,
        cwd: str | Path | None = None,
        env: Dict[str, str] | None = None,
        timeout: int | None = None,
        operation_id: OperationId,
        python: str | None = None,
        no_project: bool = False,
    ) -> ToolResult:
        self.uv_calls.append(args)

        if args[:2] == ["coverage", "run"]:
            if self.coverage_run_should_fail:
                return ToolResult(
                    success=False,
                    exit_code=1,
                    stdout="",
                    stderr="coverage run failed",
                    operation_id=operation_id,
                )
            if env and "COVERAGE_FILE" in env:
                coverage_path = Path(env["COVERAGE_FILE"])
                coverage_path.parent.mkdir(parents=True, exist_ok=True)
                coverage_path.write_bytes(b"coverage-run")
            return self._success(operation_id, stdout="coverage run")

        if args[:3] == ["coverage", "report", "-m"]:
            self._report_attempts += 1
            if self.fail_first_report and self._report_attempts == 1:
                return ToolResult(
                    success=False,
                    exit_code=1,
                    stdout="",
                    stderr=self.report_failure_reason,
                    operation_id=operation_id,
                )
            return self._success(operation_id, stdout="terminal report")

        if args[:2] == ["coverage", "combine"]:
            if env and "COVERAGE_FILE" in env:
                coverage_path = Path(env["COVERAGE_FILE"])
                coverage_path.parent.mkdir(parents=True, exist_ok=True)
                coverage_path.write_bytes(b"combined")
            return self._success(operation_id, stdout="combine")

        if args[:2] == ["coverage", "json"]:
            json_path = Path(args[args.index("-o") + 1])
            json_path.parent.mkdir(parents=True, exist_ok=True)
            json_path.write_text(json.dumps(self.json_payload), encoding="utf-8")
            return self._success(operation_id, stdout="json")

        if args[:2] == ["coverage", "html"]:
            html_dir = Path(args[args.index("-d") + 1])
            html_dir.mkdir(parents=True, exist_ok=True)
            (html_dir / "index.html").write_text("<html />", encoding="utf-8")
            return self._success(operation_id, stdout="html")

        if args[:2] == ["coverage", "xml"]:
            xml_path = Path(args[args.index("-o") + 1])
            xml_path.parent.mkdir(parents=True, exist_ok=True)
            xml_path.write_text("<xml />", encoding="utf-8")
            return self._success(operation_id, stdout="xml")

        return self._success(operation_id)


def test_run_coverage_test_writes_manifest(config: ToolsConfig, tmp_path: Path) -> None:
    create_sample_source_file(tmp_path)
    runner = CoverageRunner()

    result = coverage_module.run_coverage_test(
        config=config,
        root_path=tmp_path,
        args=["-k", "unit"],
        subprocess_runner=runner,
        cache_dir=_cache_dir(tmp_path),
    )

    assert result.success is True
    assert (_coverage_dir(tmp_path) / "coverage_manifest.json").exists()
    assert any(cmd[:2] == ["coverage", "run"] for cmd in runner.uv_calls)


def test_run_coverage_test_propagates_failures(
    config: ToolsConfig, tmp_path: Path
) -> None:
    create_sample_source_file(tmp_path)
    runner = CoverageRunner()
    runner.coverage_run_should_fail = True

    result = coverage_module.run_coverage_test(
        config=config,
        root_path=tmp_path,
        args=[],
        subprocess_runner=runner,
        cache_dir=_cache_dir(tmp_path),
    )

    assert result.success is False
    assert result.stderr == "coverage run failed"


def test_run_coverage_report_uses_cached_manifest(
    config: ToolsConfig, tmp_path: Path
) -> None:
    runner = CoverageRunner()
    _prepare_cached_coverage(tmp_path)

    result = coverage_module.run_coverage_report(
        config=config,
        root_path=tmp_path,
        args=["-k", "unit"],
        verbose=False,
        subprocess_runner=runner,
        cache_dir=_cache_dir(tmp_path),
    )

    assert result.success is True
    assert any(cmd[:3] == ["coverage", "report", "-m"] for cmd in runner.uv_calls)


def test_run_coverage_report_retries_after_no_source_error(
    config: ToolsConfig, tmp_path: Path
) -> None:
    runner = CoverageRunner()
    runner.fail_first_report = True
    _prepare_cached_coverage(tmp_path)

    result = coverage_module.run_coverage_report(
        config=config,
        root_path=tmp_path,
        args=[],
        verbose=True,
        subprocess_runner=runner,
        cache_dir=_cache_dir(tmp_path),
    )

    assert result.success is True
    assert runner.report_attempts >= 2
    assert "Generated terminal report after refreshing coverage data" in result.stdout


def test_run_coverage_report_returns_failure_when_regen_fails(
    config: ToolsConfig, tmp_path: Path
) -> None:
    runner = CoverageRunner()
    runner.fail_first_report = True
    runner.coverage_run_should_fail = True
    _prepare_cached_coverage(tmp_path)

    result = coverage_module.run_coverage_report(
        config=config,
        root_path=tmp_path,
        args=[],
        verbose=False,
        subprocess_runner=runner,
        cache_dir=_cache_dir(tmp_path),
    )

    assert result.success is False
    assert "coverage run failed" in (result.stderr or "")


def test_run_coverage_report_lists_artifacts(
    config: ToolsConfig, tmp_path: Path
) -> None:
    runner = CoverageRunner()
    _prepare_cached_coverage(tmp_path)

    result = coverage_module.run_coverage_report(
        config=config,
        root_path=tmp_path,
        args=["--skip-empty"],
        verbose=True,
        subprocess_runner=runner,
        cache_dir=_cache_dir(tmp_path),
    )

    assert result.success is True
    assert "Coverage artifacts:" in result.stdout


def test_run_coverage_report_handles_existing_json_without_regen(
    config: ToolsConfig, tmp_path: Path
) -> None:
    runner = CoverageRunner()
    coverage_dir = _coverage_dir(tmp_path)
    _prepare_cached_coverage(tmp_path)
    (coverage_dir / "coverage.json").write_text(
        json.dumps({"totals": {"num_statements": 1, "covered_lines": 1}}),
        encoding="utf-8",
    )

    result = coverage_module.run_coverage_report(
        config=config,
        root_path=tmp_path,
        args=[],
        verbose=False,
        subprocess_runner=runner,
        cache_dir=_cache_dir(tmp_path),
    )

    assert result.success is True
    assert "Coverage totals:" in result.stdout


def test_run_coverage_report_errors_on_ci_empty_file(
    config: ToolsConfig, tmp_path: Path
) -> None:
    runner = CoverageRunner()
    create_sample_source_file(tmp_path)
    write_coverage_file(tmp_path, payload=b"")
    _write_matching_manifest(tmp_path)

    with override_env("CI", "true"):
        with pytest.raises(ToolExecutionError):
            coverage_module.run_coverage_report(
                config=config,
                root_path=tmp_path,
                args=[],
                verbose=False,
                subprocess_runner=runner,
                cache_dir=_cache_dir(tmp_path),
            )


def test_run_coverage_threshold_enforces_limits(
    config: ToolsConfig, tmp_path: Path
) -> None:
    runner = CoverageRunner()
    runner.json_payload = {
        "totals": {
            "num_statements": 20,
            "covered_lines": 10,
            "num_branches": 6,
            "covered_branches": 2,
        },
        "files": {
            "src/pkg/foo.py": {
                "summary": {
                    "percent_covered": 50.0,
                    "num_branches": 2,
                    "covered_branches": 1,
                }
            }
        },
    }
    _prepare_cached_coverage(tmp_path)

    result = coverage_module.run_coverage_threshold(
        config=config,
        root_path=tmp_path,
        args=[],
        line_threshold=90.0,
        branch_threshold=80.0,
        verbose=False,
        learning_mode=False,
        verbosity_level=1,
        subprocess_runner=runner,
        cache_dir=_cache_dir(tmp_path),
        force_regen=False,
    )

    assert result.success is False
    assert "FAILURE" in result.stderr
    assert "Files below 100% coverage" in result.stdout


def test_run_coverage_combines_results_and_propagates_failure(
    config: ToolsConfig, tmp_path: Path
) -> None:
    runner = CoverageRunner()
    runner.json_payload = {
        "totals": {
            "num_statements": 10,
            "covered_lines": 9,
            "num_branches": 4,
            "covered_branches": 2,
        }
    }
    _prepare_cached_coverage(tmp_path)

    result = coverage_module.run_coverage(
        config=config,
        root_path=tmp_path,
        args=[],
        line_threshold=95.0,
        branch_threshold=90.0,
        verbose=False,
        learning_mode=False,
        verbosity_level=1,
        force_regen=False,
        subprocess_runner=runner,
        cache_dir=_cache_dir(tmp_path),
    )

    assert result.success is False
    assert result.exit_code == 1
    assert "FAILURE" in result.stderr


def test_run_coverage_supports_learning_mode(
    config: ToolsConfig, tmp_path: Path
) -> None:
    runner = CoverageRunner()
    _prepare_cached_coverage(tmp_path)

    result = coverage_module.run_coverage(
        config=config,
        root_path=tmp_path,
        args=[],
        line_threshold=10.0,
        branch_threshold=5.0,
        verbose=False,
        learning_mode=True,
        verbosity_level=2,
        force_regen=False,
        subprocess_runner=runner,
        cache_dir=_cache_dir(tmp_path),
    )

    assert result.success is True
    assert result.learning_info is not None
    assert result.learning_info.commands_executed
