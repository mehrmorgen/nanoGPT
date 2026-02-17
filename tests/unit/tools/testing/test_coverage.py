"""Coverage orchestration tests targeting the public coverage APIs."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List

import pytest  # type: ignore[import-not-found]

import ml_playground.tools.core.config as config_module
import ml_playground.tools.testing.coverage as coverage_module
from ml_playground.tools.core.config import ToolsConfig
from ml_playground.tools.core.errors import ToolExecutionError
from ml_playground.tools.core.interfaces import OperationId, ToolResult
from ml_playground.tools.testing import coverage_helpers
from ml_playground.tools.utils.subprocess_utils import SubprocessRunner
from tests.unit.tools.fakes import create_sample_source_file, write_manifest


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


def _write_coverage_json(root: Path, payload: dict[str, object]) -> Path:
    path = _coverage_dir(root) / "coverage.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    return path


def _default_payload() -> dict[str, object]:
    return {
        "totals": {
            "covered_lines": 10,
            "missing_lines": 0,
            "covered_branches": 2,
            "missing_branches": 0,
            "percent_covered": 100.0,
        },
        "files": {},
    }


def _write_matching_manifest(root: Path) -> None:
    fingerprint = coverage_helpers.compute_coverage_fingerprint(root)
    write_manifest(root, fingerprint=fingerprint)


class CoverageRunner(SubprocessRunner):
    """Protocol-compliant fake for exercising coverage flows."""

    def __init__(self) -> None:
        self.uv_calls: list[list[str]] = []
        self.pytest_calls: list[list[str]] = []
        self.slipcover_should_fail = False
        self.json_payload: dict[str, Any] = _default_payload()

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

        if args[:3] == ["python", "-m", "slipcover"]:
            if self.slipcover_should_fail:
                return ToolResult(
                    success=False,
                    exit_code=1,
                    stdout="",
                    stderr="slipcover failed",
                    operation_id=operation_id,
                )
            out_path = Path(args[args.index("--out") + 1])
            out_path.parent.mkdir(parents=True, exist_ok=True)
            out_path.write_text(json.dumps(self.json_payload), encoding="utf-8")
            return self._success(operation_id, stdout="slipcover")

        return self._success(operation_id)


def test_run_coverage_test_writes_manifest(config: ToolsConfig, tmp_path: Path) -> None:
    create_sample_source_file(tmp_path)
    runner = CoverageRunner()

    result = coverage_module.run_coverage_test(
        config=config,
        root_path=tmp_path,
        args=[],
        subprocess_runner=runner,
        cache_dir=_cache_dir(tmp_path),
    )

    assert result.success is True
    assert (_coverage_dir(tmp_path) / "coverage_manifest.json").exists()
    assert any(cmd[:3] == ["python", "-m", "slipcover"] for cmd in runner.uv_calls)


def test_run_coverage_test_propagates_failures(
    config: ToolsConfig, tmp_path: Path
) -> None:
    create_sample_source_file(tmp_path)
    runner = CoverageRunner()
    runner.slipcover_should_fail = True

    result = coverage_module.run_coverage_test(
        config=config,
        root_path=tmp_path,
        args=[],
        subprocess_runner=runner,
        cache_dir=_cache_dir(tmp_path),
    )

    assert result.success is False
    assert result.stderr == "slipcover failed"


def test_run_coverage_report_uses_cached_manifest(
    config: ToolsConfig, tmp_path: Path
) -> None:
    create_sample_source_file(tmp_path)
    _write_coverage_json(tmp_path, _default_payload())
    _write_matching_manifest(tmp_path)
    runner = CoverageRunner()

    result = coverage_module.run_coverage_report(
        config=config,
        root_path=tmp_path,
        args=[],
        verbose=False,
        subprocess_runner=runner,
        cache_dir=_cache_dir(tmp_path),
    )

    assert result.success is True
    assert "Generated JSON report" in result.stdout
    assert not runner.uv_calls


def test_run_coverage_report_force_regen_runs_slipcover(
    config: ToolsConfig, tmp_path: Path
) -> None:
    create_sample_source_file(tmp_path)
    runner = CoverageRunner()

    result = coverage_module.run_coverage_report(
        config=config,
        root_path=tmp_path,
        args=["-k", "fast"],
        verbose=True,
        subprocess_runner=runner,
        cache_dir=_cache_dir(tmp_path),
        force_regen=True,
    )

    assert result.success is True
    assert "Automatically ran coverage to generate coverage data." in result.stdout
    assert any(cmd[:3] == ["python", "-m", "slipcover"] for cmd in runner.uv_calls)


def test_run_coverage_report_raises_on_invalid_json(
    config: ToolsConfig, tmp_path: Path
) -> None:
    create_sample_source_file(tmp_path)
    _coverage_dir(tmp_path).joinpath("coverage.json").write_text(
        "not-json", encoding="utf-8"
    )
    _write_matching_manifest(tmp_path)

    with pytest.raises(ToolExecutionError):
        coverage_module.run_coverage_report(
            config=config,
            root_path=tmp_path,
            args=[],
            verbose=False,
            subprocess_runner=CoverageRunner(),
            cache_dir=_cache_dir(tmp_path),
        )


def test_run_coverage_threshold_passes(config: ToolsConfig, tmp_path: Path) -> None:
    create_sample_source_file(tmp_path)
    _write_coverage_json(tmp_path, _default_payload())
    _write_matching_manifest(tmp_path)

    result = coverage_module.run_coverage_threshold(
        config=config,
        root_path=tmp_path,
        args=[],
        line_threshold=90.0,
        branch_threshold=90.0,
        verbose=False,
        subprocess_runner=CoverageRunner(),
        cache_dir=_cache_dir(tmp_path),
    )

    assert result.success is True
    assert "Coverage totals:" in result.stdout
    assert "SUCCESS: Line coverage" in (result.stderr or "")


def test_run_coverage_threshold_fails_on_line_threshold(
    config: ToolsConfig, tmp_path: Path
) -> None:
    create_sample_source_file(tmp_path)
    payload = _default_payload()
    payload["totals"] = {
        "covered_lines": 5,
        "missing_lines": 5,
        "covered_branches": 2,
        "missing_branches": 0,
    }
    _write_coverage_json(tmp_path, payload)
    _write_matching_manifest(tmp_path)

    result = coverage_module.run_coverage_threshold(
        config=config,
        root_path=tmp_path,
        args=[],
        line_threshold=80.0,
        branch_threshold=0.0,
        verbose=False,
        subprocess_runner=CoverageRunner(),
        cache_dir=_cache_dir(tmp_path),
    )

    assert result.success is False
    assert "FAILURE: Line coverage" in (result.stderr or "")


def test_run_coverage_map_reports_undercovered_files(
    config: ToolsConfig, tmp_path: Path
) -> None:
    create_sample_source_file(tmp_path)
    file_path = tmp_path / "src" / "ml_playground" / "framework" / "core" / "gap.py"
    file_path.parent.mkdir(parents=True, exist_ok=True)
    file_path.write_text("x = 1", encoding="utf-8")

    payload = _default_payload()
    payload["files"] = {
        str(file_path): {
            "summary": {
                "percent_covered": 40.0,
                "covered_branches": 0,
                "missing_branches": 0,
            }
        }
    }
    _write_coverage_json(tmp_path, payload)
    _write_matching_manifest(tmp_path)

    result = coverage_module.run_coverage_map(
        config=config,
        root_path=tmp_path,
        args=[],
        verbose=False,
        subprocess_runner=CoverageRunner(),
        cache_dir=_cache_dir(tmp_path),
    )

    assert result.success is True
    assert "Coverage map (files below 100% coverage):" in result.stdout
    assert "gap.py" in result.stdout


def test_run_coverage_combines_report_and_threshold(
    config: ToolsConfig, tmp_path: Path
) -> None:
    create_sample_source_file(tmp_path)
    _write_coverage_json(tmp_path, _default_payload())
    _write_matching_manifest(tmp_path)

    result = coverage_module.run_coverage(
        config=config,
        root_path=tmp_path,
        args=[],
        line_threshold=90.0,
        branch_threshold=90.0,
        verbose=False,
        subprocess_runner=CoverageRunner(),
        cache_dir=_cache_dir(tmp_path),
    )

    assert result.success is True
    assert "Generated JSON report" in result.stdout
    assert "Coverage totals:" in result.stdout


def test_ensure_coverage_data_raises_on_generation_failure(
    config: ToolsConfig, tmp_path: Path
) -> None:
    create_sample_source_file(tmp_path)
    runner = CoverageRunner()
    runner.slipcover_should_fail = True

    with pytest.raises(ToolExecutionError):
        coverage_module.run_coverage_threshold(
            config=config,
            root_path=tmp_path,
            args=[],
            line_threshold=80.0,
            branch_threshold=80.0,
            verbose=False,
            subprocess_runner=runner,
            cache_dir=_cache_dir(tmp_path),
            force_regen=True,
        )


def test_read_coverage_thresholds_from_config(tmp_path: Path) -> None:
    pyproject = tmp_path / "pyproject.toml"
    pyproject.write_text(
        """
[tool.ml_playground.coverage.thresholds]
line_threshold = 88.0
branch_threshold = 77.0
""".strip(),
        encoding="utf-8",
    )

    thresholds = coverage_module._read_coverage_thresholds_from_config(tmp_path)
    assert thresholds["line_threshold"] == 88.0
    assert thresholds["branch_threshold"] == 77.0
