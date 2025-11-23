"""Coverage orchestration tests targeting the public coverage APIs."""

from __future__ import annotations

import json
import os
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Dict, Iterator, List, cast

import pytest  # type: ignore[import-not-found]

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


@contextmanager
def override_attr(obj: object, name: str, value: Any) -> Iterator[None]:
    original = getattr(obj, name)
    setattr(obj, name, value)
    try:
        yield
    finally:
        setattr(obj, name, original)


class CoverageRunner(SubprocessRunner):
    """Protocol-compliant fake for exercising coverage flows."""

    def __init__(self) -> None:
        self.uv_calls: list[list[str]] = []
        self.pytest_calls: list[list[str]] = []
        self.coverage_run_should_fail = False
        self.fail_first_report = False
        self.report_failure_reason = "No source for code"
        self.preserve_empty_coverage_file = False
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
                # Preserve empty files when flag is set (for CI empty file testing)
                if not (
                    self.preserve_empty_coverage_file
                    and coverage_path.exists()
                    and coverage_path.stat().st_size == 0
                ):
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
    assert "Generated terminal report" in result.stdout
    assert "Generated HTML report" in result.stdout
    assert "Generated JSON report" in result.stdout
    assert "Generated XML report" in result.stdout


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
    assert "Generated terminal report" in result.stdout


def test_run_coverage_report_raises_in_ci_when_file_empty(
    config: ToolsConfig, tmp_path: Path
) -> None:
    runner = CoverageRunner()
    coverage_dir = _coverage_dir(tmp_path)
    coverage_file = coverage_dir / "coverage.sqlite"
    coverage_file.write_bytes(b"")

    env = {"COVERAGE_FILE": str(coverage_file)}

    def fake_ensure(**_: object) -> tuple[list[str], list[str], dict[str, str]]:  # type: ignore[override]
        return [], [], env.copy()

    with override_attr(coverage_module, "_ensure_coverage_data", fake_ensure):
        with override_env("CI", "true"):
            with pytest.raises(ToolExecutionError, match="Coverage data file is empty"):
                coverage_module.run_coverage_report(
                    config=config,
                    root_path=tmp_path,
                    args=[],
                    verbose=False,
                    subprocess_runner=runner,
                    cache_dir=_cache_dir(tmp_path),
                )


def test_ensure_coverage_data_uses_stdout_for_generation_error(
    config: ToolsConfig, tmp_path: Path
) -> None:
    """_ensure_coverage_data should surface stdout when stderr is empty.

    We simulate a failure in _run_coverage_test_for_data that only
    populates stdout. The resulting ToolExecutionError.reason should
    contain that stdout text instead of a generic message.
    """

    def fake_run_for_data(**_: Any) -> tuple[ToolResult, list[str]]:  # type: ignore[override]
        return (
            ToolResult(
                success=False,
                exit_code=1,
                stdout="generation via stdout",
                stderr="",
                operation_id=OperationId(
                    namespace="tools", category="test", command="coverage-test"
                ),
            ),
            [],
        )

    runner = CoverageRunner()

    with override_attr(
        coverage_module, "_run_coverage_test_for_data", fake_run_for_data
    ):
        with pytest.raises(ToolExecutionError) as exc:
            coverage_module.run_coverage_threshold(
                config=config,
                root_path=tmp_path,
                args=[],
                line_threshold=0.0,
                branch_threshold=0.0,
                verbose=False,
                learning_mode=False,
                verbosity_level=1,
                subprocess_runner=runner,
                cache_dir=_cache_dir(tmp_path),
                force_regen=True,
            )

    assert "generation via stdout" in cast(ToolExecutionError, exc.value).reason


def test_ensure_coverage_data_uses_stdout_for_combine_error(
    config: ToolsConfig, tmp_path: Path
) -> None:
    """_ensure_coverage_data should surface stdout for combine failures.

    We simulate a failure in _combine_coverage_fragments that only
    populates stdout. The resulting ToolExecutionError.reason should
    contain that stdout text instead of a generic message.
    """

    def fake_combine(**_: Any) -> tuple[ToolResult, bool]:  # type: ignore[override]
        return (
            ToolResult(
                success=False,
                exit_code=1,
                stdout="combine via stdout",
                stderr="",
                operation_id=OperationId(
                    namespace="tools", category="test", command="coverage-combine"
                ),
            ),
            True,
        )

    runner = CoverageRunner()

    with override_attr(coverage_module, "_combine_coverage_fragments", fake_combine):
        with pytest.raises(ToolExecutionError) as exc:
            coverage_module.run_coverage_threshold(
                config=config,
                root_path=tmp_path,
                args=[],
                line_threshold=0.0,
                branch_threshold=0.0,
                verbose=False,
                learning_mode=False,
                verbosity_level=1,
                subprocess_runner=runner,
                cache_dir=_cache_dir(tmp_path),
                force_regen=True,
            )

    assert "combine via stdout" in cast(ToolExecutionError, exc.value).reason


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


def test_run_coverage_threshold_appends_json_errors(
    config: ToolsConfig, tmp_path: Path
) -> None:
    class JsonFailureRunner(CoverageRunner):
        def run_uv_command(  # type: ignore[override]
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
            if args[:2] == ["coverage", "json"]:
                json_path = Path(args[args.index("-o") + 1])
                json_path.parent.mkdir(parents=True, exist_ok=True)
                json_path.write_text(json.dumps(self.json_payload), encoding="utf-8")
                return ToolResult(
                    success=False,
                    exit_code=1,
                    stdout="",
                    stderr="json failed",
                    operation_id=operation_id,
                )
            return super().run_uv_command(
                args,
                cwd=cwd,
                env=env,
                timeout=timeout,
                operation_id=operation_id,
                python=python,
                no_project=no_project,
            )

    runner = JsonFailureRunner()
    runner.json_payload = {
        "totals": {
            "num_statements": 20,
            "covered_lines": 10,
            "num_branches": 2,
            "covered_branches": 1,
        },
        "files": {},
    }
    _prepare_cached_coverage(tmp_path)

    result = coverage_module.run_coverage_threshold(
        config=config,
        root_path=tmp_path,
        args=[],
        line_threshold=95.0,
        branch_threshold=0.0,
        verbose=False,
        learning_mode=False,
        verbosity_level=1,
        subprocess_runner=runner,
        cache_dir=_cache_dir(tmp_path),
        force_regen=False,
    )

    assert result.success is False
    stderr = result.stderr or ""
    assert "json failed" in stderr
    assert "FAILURE" in stderr


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
        branch_threshold=75.0,
        verbose=False,
        learning_mode=False,
        verbosity_level=1,
        force_regen=False,
        subprocess_runner=runner,
        cache_dir=_cache_dir(tmp_path),
    )

    assert result.success is False
    stderr = result.stderr or ""
    assert "FAILURE" in stderr


def test_run_coverage_report_raises_on_runner_exception(
    config: ToolsConfig, tmp_path: Path
) -> None:
    """run_coverage_report should wrap unexpected runner exceptions in ToolExecutionError."""

    class RaisingRunner(CoverageRunner):
        def run_uv_command(  # type: ignore[override]
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
            if args[:3] == ["coverage", "report", "-m"]:
                raise RuntimeError("runner boom")
            return super().run_uv_command(
                args,
                cwd=cwd,
                env=env,
                timeout=timeout,
                operation_id=operation_id,
                python=python,
                no_project=no_project,
            )

    runner = RaisingRunner()
    _prepare_cached_coverage(tmp_path)

    with pytest.raises(ToolExecutionError, match="Failed to generate terminal report"):
        coverage_module.run_coverage_report(
            config=config,
            root_path=tmp_path,
            args=[],
            verbose=False,
            subprocess_runner=runner,
            cache_dir=_cache_dir(tmp_path),
        )


def test_run_coverage_test_with_learning_mode(
    config: ToolsConfig, tmp_path: Path
) -> None:
    create_sample_source_file(tmp_path)
    runner = CoverageRunner()

    result = coverage_module.run_coverage_test(
        config=config,
        root_path=tmp_path,
        args=["-k", "unit"],
        subprocess_runner=runner,
        cache_dir=_cache_dir(tmp_path),
        learning_mode=True,
    )

    assert result.success is True
    assert result.learning_info is not None
    assert result.operation_id.command == "coverage-test"
    assert result.operation_id.category == "test"


def test_run_coverage_report_with_learning_mode(
    config: ToolsConfig, tmp_path: Path
) -> None:
    runner = CoverageRunner()
    _prepare_cached_coverage(tmp_path)

    result = coverage_module.run_coverage_report(
        config=config,
        root_path=tmp_path,
        args=[],
        verbose=False,
        subprocess_runner=runner,
        cache_dir=_cache_dir(tmp_path),
        learning_mode=True,
    )

    assert result.success is True
    assert result.learning_info is not None
    assert result.operation_id.command == "coverage-report"


def test_run_coverage_threshold_with_learning_mode(
    config: ToolsConfig, tmp_path: Path
) -> None:
    runner = CoverageRunner()
    _prepare_cached_coverage(tmp_path)

    result = coverage_module.run_coverage_threshold(
        config=config,
        root_path=tmp_path,
        args=[],
        line_threshold=0.0,
        branch_threshold=0.0,
        verbose=False,
        learning_mode=True,
        verbosity_level=1,
        subprocess_runner=runner,
        cache_dir=_cache_dir(tmp_path),
        force_regen=False,
    )

    assert result.success is True
    assert result.learning_info is not None
    assert result.operation_id.command == "coverage-threshold"


def test_ensure_coverage_data_combines_fragments_when_main_missing(
    config: ToolsConfig, tmp_path: Path
) -> None:
    """Should combine fragments if main coverage file is missing but fragments exist."""
    runner = CoverageRunner()
    create_sample_source_file(tmp_path)

    # Setup: manifest exists, but main coverage file is missing. Fragments exist.
    coverage_dir = _coverage_dir(tmp_path)
    coverage_file = coverage_dir / "coverage.sqlite"
    fragment = coverage_dir / "coverage.sqlite.1234"
    fragment.write_bytes(b"fragment-data")

    # Write manifest matching current state
    _write_matching_manifest(tmp_path)

    # Ensure main file is gone
    if coverage_file.exists():
        coverage_file.unlink()

    # Mock combine to "create" the main file
    original_run_uv = runner.run_uv_command

    def fake_run_uv(
        args: List[str],
        *,
        env: Dict[str, str] | None = None,
        operation_id: OperationId,
        **kwargs: Any,
    ) -> ToolResult:
        if args[:2] == ["coverage", "combine"]:
            if env and "COVERAGE_FILE" in env:
                Path(env["COVERAGE_FILE"]).write_bytes(b"combined-data")
            return ToolResult(
                success=True,
                exit_code=0,
                stdout="",
                stderr="",
                operation_id=operation_id,
            )
        return original_run_uv(args, env=env, operation_id=operation_id, **kwargs)

    runner.run_uv_command = fake_run_uv  # type: ignore[assignment]

    # We invoke via run_coverage_report which calls _ensure_coverage_data
    result = coverage_module.run_coverage_report(
        config=config,
        root_path=tmp_path,
        args=[],
        verbose=True,
        subprocess_runner=runner,
        cache_dir=_cache_dir(tmp_path),
    )

    assert result.success is True
    assert "Combined existing coverage fragments" in result.stdout
    assert coverage_file.exists()
    assert coverage_file.read_bytes() == b"combined-data"


def test_ensure_coverage_data_fails_on_initial_combine_error(
    config: ToolsConfig, tmp_path: Path
) -> None:
    """Should raise ToolExecutionError if initial fragment combination fails."""
    runner = CoverageRunner()

    # Force _combine_coverage_fragments to return a failure result
    def fake_combine(**_: Any) -> tuple[ToolResult, bool]:  # type: ignore[override]
        return (
            ToolResult(
                success=False,
                exit_code=1,
                stdout="",
                stderr="combine failed",
                operation_id=OperationId(
                    namespace="tools", category="test", command="coverage-combine"
                ),
            ),
            True,
        )

    with override_attr(coverage_module, "_combine_coverage_fragments", fake_combine):
        with pytest.raises(
            ToolExecutionError, match="Coverage fragment combination failed"
        ):
            coverage_module.run_coverage_report(
                config=config,
                root_path=tmp_path,
                args=[],
                verbose=False,
                subprocess_runner=runner,
                cache_dir=_cache_dir(tmp_path),
            )


def test_ensure_coverage_data_fails_on_second_combine_error(
    config: ToolsConfig, tmp_path: Path
) -> None:
    """Should raise ToolExecutionError if post-generation fragment combination fails."""
    runner = CoverageRunner()
    create_sample_source_file(tmp_path)

    # Mock the sequence:
    # 1. Initial combine -> (None, False) (nothing to combine yet)
    # 2. Generation -> success (real generation or mocked)
    # 3. Second combine -> (Failure, True)

    original_combine = coverage_module._combine_coverage_fragments

    call_count = 0

    def fake_combine(**kwargs: Any) -> tuple[ToolResult | None, bool]:
        nonlocal call_count
        call_count += 1
        if call_count == 2:
            return (
                ToolResult(
                    success=False,
                    exit_code=1,
                    stdout="",
                    stderr="second combine failed",
                    operation_id=OperationId(
                        namespace="tools", category="test", command="coverage-combine"
                    ),
                ),
                True,
            )
        return original_combine(**kwargs)

    with override_attr(coverage_module, "_combine_coverage_fragments", fake_combine):
        with pytest.raises(
            ToolExecutionError, match="Coverage fragment combination failed"
        ):
            coverage_module.run_coverage_report(
                config=config,
                root_path=tmp_path,
                args=[],
                verbose=False,
                subprocess_runner=runner,
                cache_dir=_cache_dir(tmp_path),
                force_regen=True,  # Force regen to ensure we hit the generation path
            )


def test_read_coverage_thresholds_handles_missing_config(tmp_path: Path) -> None:
    """Should return empty dict if pyproject.toml is missing."""
    thresholds = coverage_module._read_coverage_thresholds_from_config(tmp_path)
    assert thresholds == {}


def test_read_coverage_thresholds_handles_malformed_config(tmp_path: Path) -> None:
    """Should return empty dict if config parsing fails."""
    (tmp_path / "pyproject.toml").write_text("INVALID TOML [", encoding="utf-8")
    thresholds = coverage_module._read_coverage_thresholds_from_config(tmp_path)
    assert thresholds == {}


def test_read_coverage_thresholds_reads_correct_values(tmp_path: Path) -> None:
    """Should correctly extract thresholds from valid TOML."""
    toml_content = """
[tool.ml_playground.coverage.thresholds]
line_threshold = 85.5
branch_threshold = 75.0
"""
    (tmp_path / "pyproject.toml").write_text(toml_content, encoding="utf-8")

    thresholds = coverage_module._read_coverage_thresholds_from_config(tmp_path)
    assert thresholds["line_threshold"] == 85.5
    assert thresholds["branch_threshold"] == 75.0


def test_run_coverage_test_cleans_up_fragments(
    config: ToolsConfig, tmp_path: Path
) -> None:
    """Should remove existing coverage fragments before running."""
    runner = CoverageRunner()
    coverage_dir = _coverage_dir(tmp_path)

    # Create dummy fragments
    (coverage_dir / "coverage.sqlite.1").write_text("frag1", encoding="utf-8")
    (coverage_dir / "coverage.sqlite.2").write_text("frag2", encoding="utf-8")

    coverage_module.run_coverage_test(
        config=config,
        root_path=tmp_path,
        args=[],
        subprocess_runner=runner,
        cache_dir=_cache_dir(tmp_path),
    )

    assert not (coverage_dir / "coverage.sqlite.1").exists()
    assert not (coverage_dir / "coverage.sqlite.2").exists()


def test_run_coverage_threshold_raises_when_coverage_file_missing(
    config: ToolsConfig, tmp_path: Path
) -> None:
    """Should raise ToolExecutionError if coverage file is missing despite ensure success."""
    runner = CoverageRunner()

    # Mock _ensure_coverage_data to return success but do nothing (so file remains missing)
    def fake_ensure(**_: Any) -> tuple[list[str], list[str], dict[str, str]]:  # type: ignore[override]
        return [], [], {}

    with override_attr(coverage_module, "_ensure_coverage_data", fake_ensure):
        with pytest.raises(ToolExecutionError, match="Coverage data file not found"):
            coverage_module.run_coverage_threshold(
                config=config,
                root_path=tmp_path,
                args=[],
                subprocess_runner=runner,
                cache_dir=_cache_dir(tmp_path),
            )


def test_run_coverage_test_verbose_logging(config: ToolsConfig, tmp_path: Path) -> None:
    """Should log output when verbose is True."""
    runner = CoverageRunner()
    create_sample_source_file(tmp_path)

    # Capture output to verify verbose logging
    # This is tricky because the function returns ToolResult, but _run_coverage_test_for_data
    # (which uses verbose) is internal.
    # However, we can test run_coverage_report with verbose=True and force regen.

    result = coverage_module.run_coverage_report(
        config=config,
        root_path=tmp_path,
        args=[],
        verbose=True,
        subprocess_runner=runner,
        cache_dir=_cache_dir(tmp_path),
        force_regen=True,
    )

    assert result.success is True
    # We expect some verbose output if the runner produced stdout/stderr,
    # but our fake runner might not unless we configure it.
    # The default CoverageRunner returns "pytest" in stdout for run_pytest_command.
    # The code in _run_coverage_test_for_data appends stdout/stderr to notes if verbose.
    # And _ensure_coverage_data extends notes.
    # And run_coverage_report includes notes in output.

    assert "Automatically ran coverage" in result.stdout


def test_run_coverage_with_learning_mode_aggregates_info(
    config: ToolsConfig, tmp_path: Path
) -> None:
    """run_coverage should combine learning info from report and threshold steps."""
    runner = CoverageRunner()
    _prepare_cached_coverage(tmp_path)

    result = coverage_module.run_coverage(
        config=config,
        root_path=tmp_path,
        args=[],
        line_threshold=0.0,
        branch_threshold=0.0,
        verbose=False,
        learning_mode=True,
        verbosity_level=1,
        subprocess_runner=runner,
        cache_dir=_cache_dir(tmp_path),
    )

    assert result.success is True
    assert result.learning_info is not None
    # Check that we have commands from both phases
    cmds = [cmd for cmd in result.learning_info.commands_executed]
    assert any("coverage report" in cmd for cmd in cmds)
    assert any("coverage json" in cmd for cmd in cmds)


def test_coverage_threshold_handles_missing_totals(
    config: ToolsConfig, tmp_path: Path
) -> None:
    """Should fail gracefully when coverage totals are zero/missing."""
    runner = CoverageRunner()
    # Payload with zero statements/branches to trigger "data missing" paths
    runner.json_payload = {
        "totals": {
            "num_statements": 0,
            "covered_lines": 0,
            "num_branches": 0,
            "covered_branches": 0,
        },
        "files": {},
    }
    _prepare_cached_coverage(tmp_path)

    result = coverage_module.run_coverage_threshold(
        config=config,
        root_path=tmp_path,
        args=[],
        line_threshold=90.0,
        branch_threshold=90.0,
        subprocess_runner=runner,
        cache_dir=_cache_dir(tmp_path),
    )

    assert result.success is False
    assert "Line coverage totals missing" in result.stderr
    assert "Branch coverage data missing" in result.stderr


def test_run_coverage_test_verbose_captures_subprocess_output(
    config: ToolsConfig, tmp_path: Path
) -> None:
    """Should include subprocess stdout/stderr in verbose output."""

    # We need a runner that produces output and we need to trigger _run_coverage_test_for_data
    # which happens when we force regen.
    class NoisyRunner(CoverageRunner):
        def run_uv_command(self, args: List[str], **kwargs: Any) -> ToolResult:
            if args[:2] == ["coverage", "run"]:
                return ToolResult(
                    success=True,
                    exit_code=0,
                    stdout="coverage run stdout",
                    stderr="coverage run stderr",
                    operation_id=kwargs["operation_id"],
                )
            return super().run_uv_command(args, **kwargs)

    runner = NoisyRunner()
    create_sample_source_file(tmp_path)

    result = coverage_module.run_coverage_report(
        config=config,
        root_path=tmp_path,
        args=[],
        verbose=True,
        subprocess_runner=runner,
        cache_dir=_cache_dir(tmp_path),
        force_regen=True,
    )

    assert result.success is True
    assert "coverage run stdout" in result.stdout
    assert "coverage run stderr" in result.stdout


def test_ensure_coverage_data_fails_on_generation_error(
    config: ToolsConfig, tmp_path: Path
) -> None:
    """Should raise ToolExecutionError if coverage data generation fails."""
    runner = CoverageRunner()

    def fake_generate(**_: Any) -> tuple[ToolResult, list[str]]:  # type: ignore[override]
        return (
            ToolResult(
                success=False,
                exit_code=1,
                stdout="",
                stderr="generation failed",
                operation_id=OperationId(
                    namespace="tools", category="test", command="coverage-test"
                ),
            ),
            [],
        )

    with override_attr(coverage_module, "_run_coverage_test_for_data", fake_generate):
        with pytest.raises(ToolExecutionError, match="Coverage data generation failed"):
            coverage_module.run_coverage_report(
                config=config,
                root_path=tmp_path,
                args=[],
                verbose=False,
                subprocess_runner=runner,
                cache_dir=_cache_dir(tmp_path),
                force_regen=True,
            )
