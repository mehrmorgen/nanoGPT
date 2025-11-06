"""Coverage orchestration tests for TestingTools.

Uses shared fakes from tests/unit/tools/fakes.py and follows naming guidelines.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from ml_playground.tools import testing as testing_module
from ml_playground.tools.core import config as config_module
from ml_playground.tools.core.config import ToolsConfig
from ml_playground.tools.core.interfaces import OperationId, ToolResult
from tests.unit.tools.fakes import (
    RecordingRunner,
    MetricsRunner,
    CombineFailureRunner,
    ReportFailureRunner,
    write_coverage_file,
    write_manifest,
    create_sample_source_file,
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


def test_generate_coverage_via_pytest_success(
    config: ToolsConfig, tmp_path: Path
) -> None:
    runner = RecordingRunner()
    tools = testing_module.TestingTools(config, tmp_path, runner)

    result, notes = tools._generate_coverage_via_pytest(
        args=["-k", "unit"],
        verbose=True,
        operation_id=OperationId(namespace="tools", category="test", command="ensure"),
        executed_commands=[],
    )

    assert result is None
    assert runner.pytest_calls
    assert runner.pytest_calls[0]["args"][0] == "tests/unit"
    assert any("Coverage pipeline generated no data" in note for note in notes)


def test_generate_coverage_via_pytest_failure_propagates(
    config: ToolsConfig, tmp_path: Path
) -> None:
    from tests.unit.tools.fakes import PytestFailureRunner

    runner = PytestFailureRunner()
    tools = testing_module.TestingTools(config, tmp_path, runner)

    result, notes = tools._generate_coverage_via_pytest(
        args=[],
        verbose=False,
        operation_id=OperationId(namespace="tools", category="test", command="ensure"),
        executed_commands=[],
    )

    assert isinstance(result, ToolResult)
    assert result.success is False
    assert notes == []


def test_ensure_coverage_data_combine_failure_returns_toolresult(
    config: ToolsConfig, tmp_path: Path
) -> None:
    runner = CombineFailureRunner()
    tools = testing_module.TestingTools(config, tmp_path, runner)

    coverage_file = tools._coverage_file()
    coverage_file.parent.mkdir(parents=True, exist_ok=True)
    fragment = coverage_file.parent / f"{coverage_file.name}.frag"
    fragment.write_bytes(b"fragment")

    result, notes, env = tools._ensure_coverage_data(
        args=[],
        learning_mode=False,
        verbosity_level=1,
        verbose=False,
        operation_id=OperationId(namespace="tools", category="test", command="ensure"),
        executed_commands=[],
    )

    assert isinstance(result, ToolResult)
    assert result.success is False
    assert "combine failed" in ((result.stderr or "") + (result.stdout or ""))
    assert notes == []
    assert env["COVERAGE_FILE"].endswith("coverage.sqlite")
    assert any(call["args"][:2] == ["coverage", "combine"] for call in runner.uv_calls)


def test_ensure_coverage_data_uses_cached_manifest(
    config: ToolsConfig, tmp_path: Path
) -> None:
    runner = RecordingRunner()
    tools = testing_module.TestingTools(config, tmp_path, runner)

    coverage_file = tools._coverage_file()
    coverage_file.parent.mkdir(parents=True, exist_ok=True)
    coverage_file.write_bytes(b"cached")

    fingerprint = tools._compute_coverage_fingerprint()
    tools._write_coverage_manifest(fingerprint=fingerprint)

    result, notes, env = tools._ensure_coverage_data(
        args=["-k", "unit"],
        learning_mode=False,
        verbosity_level=1,
        verbose=False,
        operation_id=OperationId(namespace="tools", category="test", command="ensure"),
        executed_commands=[],
    )

    assert result is None
    assert notes == []
    assert env["COVERAGE_FILE"].endswith("coverage.sqlite")
    assert runner.uv_calls == []
    assert runner.pytest_calls == []


def test_ensure_coverage_data_combines_cached_fragments_without_regen(
    config: ToolsConfig, tmp_path: Path
) -> None:
    runner = RecordingRunner()
    tools = testing_module.TestingTools(config, tmp_path, runner)

    coverage_dir = tmp_path / ".cache" / "coverage"
    coverage_dir.mkdir(parents=True, exist_ok=True)
    fragment = coverage_dir / "coverage.sqlite.1"
    fragment.write_text("fragment", encoding="utf-8")

    fingerprint = tools._compute_coverage_fingerprint()
    tools._write_coverage_manifest(fingerprint=fingerprint)

    result, notes, _ = tools._ensure_coverage_data(
        args=[],
        learning_mode=False,
        verbosity_level=0,
        verbose=False,
        operation_id=OperationId(namespace="tools", category="test", command="ensure"),
        executed_commands=[],
    )

    assert result is None
    assert "Combined existing coverage fragments into coverage.sqlite." in notes
    assert any(call["args"][:2] == ["coverage", "combine"] for call in runner.uv_calls)


def test_ensure_coverage_data_regenerates_and_combines_fragments(
    config: ToolsConfig, tmp_path: Path
) -> None:
    class CoverageGeneratingTools(testing_module.TestingTools):
        def coverage_test(
            self,
            args: list[str],
            *,
            learning_mode: bool = False,
            verbosity_level: int = 1,
        ) -> ToolResult:  # type: ignore[override]
            coverage_file = self._coverage_file()
            coverage_file.parent.mkdir(parents=True, exist_ok=True)
            fragment = coverage_file.parent / f"{coverage_file.name}.fragment"
            fragment.write_text("generated", encoding="utf-8")
            coverage_file.write_bytes(b"coverage-data")
            return ToolResult(
                success=True,
                exit_code=0,
                stdout="coverage refreshed",
                stderr="",
                operation_id=OperationId(
                    namespace="tools", category="test", command="coverage"
                ),
            )

    runner = RecordingRunner()
    tools = CoverageGeneratingTools(config, tmp_path, runner)

    result, notes, _ = tools._ensure_coverage_data(
        args=["-k", "unit"],
        learning_mode=False,
        verbosity_level=1,
        verbose=True,
        operation_id=OperationId(namespace="tools", category="test", command="ensure"),
        executed_commands=[],
    )

    assert result is None
    assert any("Automatically ran coverage" in note for note in notes)
    assert "Combined coverage fragments into coverage.sqlite." in notes
    assert any(call["args"][:2] == ["coverage", "combine"] for call in runner.uv_calls)


def test_coverage_report_retries_after_no_source_error(
    config: ToolsConfig, tmp_path: Path
) -> None:
    class RetryRunner(RecordingRunner):
        def __init__(self) -> None:
            super().__init__()
            self.report_calls = 0

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
            self.uv_calls.append({"args": args, "env": env, "cwd": cwd})
            if args[:3] == ["coverage", "report", "-m"]:
                self.report_calls += 1
                if self.report_calls == 1:
                    return ToolResult(
                        success=False,
                        exit_code=1,
                        stdout="",
                        stderr="No source for code",
                        operation_id=operation_id,
                    )
            if args[:2] == ["coverage", "json"]:
                out_path = Path(args[args.index("-o") + 1])
                out_path.parent.mkdir(parents=True, exist_ok=True)
                out_path.write_text(json.dumps({"totals": {}}), encoding="utf-8")
            return super().run_uv_command(
                args,
                cwd=cwd,
                env=env,
                timeout=timeout,
                operation_id=operation_id,
                python=python,
                no_project=no_project,
            )

    runner = RetryRunner()
    tools = testing_module.TestingTools(config, tmp_path, runner)

    create_sample_source_file(tmp_path)
    write_coverage_file(tmp_path)
    fingerprint = tools._compute_coverage_fingerprint()
    write_manifest(tmp_path, fingerprint)

    result = tools.coverage_report([], verbose=True)

    assert result.success is True
    assert runner.report_calls == 2
    stdout_lines = result.stdout.splitlines()
    assert any(
        "Generated terminal report after refreshing coverage data" in line
        for line in stdout_lines
    )


def test_coverage_pipeline_regenerates_when_manifest_stale(
    config: ToolsConfig, tmp_path: Path
) -> None:
    runner = RecordingRunner()
    tools = testing_module.TestingTools(config, tmp_path, runner)

    coverage_dir = tmp_path / ".cache" / "coverage"
    coverage_dir.mkdir(parents=True, exist_ok=True)
    coverage_file = coverage_dir / "coverage.sqlite"
    coverage_file.write_bytes(b"stale")

    create_sample_source_file(tmp_path)
    stale_manifest = tools._compute_coverage_fingerprint()
    write_manifest(tmp_path, fingerprint="different" + stale_manifest)

    result, notes, env = tools._ensure_coverage_data(
        args=["-k", "unit"],
        learning_mode=False,
        verbosity_level=1,
        verbose=True,
        operation_id=OperationId(namespace="tools", category="test", command="ensure"),
        executed_commands=[],
    )

    assert result is None
    assert any(
        "Automatically ran coverage" in note or "Combined coverage fragments" in note
        for note in notes
    )
    assert env["COVERAGE_FILE"].endswith("coverage.sqlite")
    assert any(
        call["args"][0] == "coverage" and call["args"][1] == "run"
        for call in runner.uv_calls
    )


def test_coverage_pipeline_falls_back_to_pytest_when_coverage_run_empty(
    config: ToolsConfig, tmp_path: Path
) -> None:
    class EmptyCoverageRunner(RecordingRunner):
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
        ) -> testing_module.ToolResult:
            result = super().run_uv_command(
                args,
                cwd=cwd,
                env=env,
                timeout=timeout,
                operation_id=operation_id,
                python=python,
                no_project=no_project,
            )
            if args[:2] == ["coverage", "run"] and env is not None:
                coverage_path = Path(env["COVERAGE_FILE"])  # type: ignore[index]
                coverage_path.write_bytes(b"")
            return result

    runner = EmptyCoverageRunner()
    tools = testing_module.TestingTools(config, tmp_path, runner)

    result, notes, _ = tools._ensure_coverage_data(
        args=[],
        learning_mode=False,
        verbosity_level=1,
        verbose=True,
        operation_id=OperationId(namespace="tools", category="test", command="ensure"),
        executed_commands=[],
    )

    assert result is None
    assert any("Coverage pipeline generated no data" in note for note in notes)
    assert runner.pytest_calls


def test_ensure_coverage_data_returns_generation_failure(
    config: ToolsConfig, tmp_path: Path
) -> None:
    class CoverageRunFailureRunner(RecordingRunner):
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
            if args[:2] == ["coverage", "run"]:
                return ToolResult(
                    success=False,
                    exit_code=1,
                    stdout="",
                    stderr="coverage run failed",
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

    runner = CoverageRunFailureRunner()
    tools = testing_module.TestingTools(config, tmp_path, runner)

    result, notes, env = tools._ensure_coverage_data(
        args=["-k", "unit"],
        learning_mode=False,
        verbosity_level=1,
        verbose=False,
        operation_id=OperationId(namespace="tools", category="test", command="ensure"),
        executed_commands=[],
    )

    assert isinstance(result, ToolResult)
    assert result.success is False
    assert "coverage run failed" in (result.stderr or "")
    assert notes == []
    assert env["COVERAGE_FILE"].endswith("coverage.sqlite")


def test_coverage_combined_result_reports_failures_from_components(
    config: ToolsConfig, tmp_path: Path
) -> None:
    class ThresholdFailRunner(MetricsRunner):
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
        ) -> testing_module.ToolResult:
            result = super().run_uv_command(
                args,
                cwd=cwd,
                env=env,
                timeout=timeout,
                operation_id=operation_id,
                python=python,
                no_project=no_project,
            )
            if args[:2] == ["coverage", "json"]:
                out_path = Path(args[args.index("-o") + 1])
                out_path.parent.mkdir(parents=True, exist_ok=True)
                out_path.write_text(
                    json.dumps(
                        {
                            "totals": {
                                "num_statements": 10,
                                "covered_lines": 6,
                                "num_branches": 4,
                                "covered_branches": 2,
                            },
                            "files": {},
                        }
                    ),
                    encoding="utf-8",
                )
            return result

    runner = ThresholdFailRunner()
    tools = testing_module.TestingTools(config, tmp_path, runner)

    create_sample_source_file(tmp_path)
    write_coverage_file(tmp_path)
    fingerprint = tools._compute_coverage_fingerprint()
    write_manifest(tmp_path, fingerprint)

    result = tools.coverage(
        [],
        line_threshold=80.0,
        branch_threshold=70.0,
        verbose=False,
    )

    assert result.success is False
    assert result.exit_code == 1
    assert "FAILURE" in result.stderr
    assert "Coverage totals" in result.stdout or result.stdout


def test_coverage_report_records_regen_failure(
    config: ToolsConfig, tmp_path: Path
) -> None:
    class RegenFailureRunner(RecordingRunner):
        def __init__(self) -> None:
            super().__init__()
            self._report_calls = 0

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
            if args[:3] == ["coverage", "report", "-m"]:
                self._report_calls += 1
                if self._report_calls == 1:
                    return ToolResult(
                        success=False,
                        exit_code=1,
                        stdout="",
                        stderr="No source for code",
                        operation_id=operation_id,
                    )
            if args[:2] == ["coverage", "run"]:
                return ToolResult(
                    success=False,
                    exit_code=1,
                    stdout="",
                    stderr="coverage regen failed",
                    operation_id=operation_id,
                )
            if args[:2] == ["coverage", "json"]:
                out_path = Path(args[args.index("-o") + 1])
                out_path.write_text(
                    json.dumps(
                        {
                            "totals": {
                                "num_statements": 10,
                                "covered_lines": 10,
                                "num_branches": 2,
                                "covered_branches": 2,
                            }
                        }
                    ),
                    encoding="utf-8",
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
            return super().run_uv_command(
                args,
                cwd=cwd,
                env=env,
                timeout=timeout,
                operation_id=operation_id,
                python=python,
                no_project=no_project,
            )

    runner = RegenFailureRunner()
    tools = testing_module.TestingTools(config, tmp_path, runner)

    coverage_dir = tmp_path / ".cache" / "coverage"
    coverage_dir.mkdir(parents=True, exist_ok=True)
    (coverage_dir / "coverage.sqlite").write_bytes(b"data")

    create_sample_source_file(tmp_path)
    fingerprint = tools._compute_coverage_fingerprint()
    write_manifest(tmp_path, fingerprint)

    coverage_json = coverage_dir / "coverage.json"
    coverage_json.write_text(
        json.dumps({"totals": {"num_statements": 1, "covered_lines": 1}}),
        encoding="utf-8",
    )

    result = tools.coverage_report([], verbose=False)

    assert result.success is False
    assert "coverage regen failed" in (result.stderr or "")
    assert "[FAILED] terminal report" in result.stdout


def test_coverage_report_handles_existing_json_without_regen(
    config: ToolsConfig, tmp_path: Path
) -> None:
    runner = MetricsRunner()
    tools = testing_module.TestingTools(config, tmp_path, runner)

    coverage_dir = tmp_path / ".cache" / "coverage"
    coverage_dir.mkdir(parents=True, exist_ok=True)
    write_coverage_file(tmp_path)
    fingerprint = tools._compute_coverage_fingerprint()
    write_manifest(tmp_path, fingerprint)

    coverage_json = coverage_dir / "coverage.json"
    coverage_json.write_text(
        json.dumps({"totals": {"num_statements": 1, "covered_lines": 1}}),
        encoding="utf-8",
    )

    result = tools.coverage_report([], verbose=False)

    assert result.success is True
    assert "Coverage totals:" in result.stdout
    assert "coverage json -o" in result.stdout


def test_coverage_report_success_lists_artifacts(
    config: ToolsConfig, tmp_path: Path
) -> None:
    runner = MetricsRunner()
    tools = testing_module.TestingTools(config, tmp_path, runner)

    coverage_dir = tmp_path / ".cache" / "coverage"
    coverage_dir.mkdir(parents=True, exist_ok=True)
    write_coverage_file(tmp_path)
    create_sample_source_file(tmp_path)
    fingerprint = tools._compute_coverage_fingerprint()
    write_manifest(tmp_path, fingerprint)

    (coverage_dir / "coverage.json").write_text(
        json.dumps(
            {
                "totals": {
                    "num_statements": 12,
                    "covered_lines": 12,
                    "num_branches": 4,
                    "covered_branches": 4,
                },
                "files": {
                    "src/pkg/foo.py": {
                        "summary": {
                            "percent_covered_display": "100.00",
                            "num_branches": 2,
                            "covered_branches": 2,
                        }
                    }
                },
            }
        ),
        encoding="utf-8",
    )

    result = tools.coverage_report([], verbose=True)

    assert result.success is True
    assert "Coverage totals:" in result.stdout
    assert "Coverage artifacts:" in result.stdout


def test_coverage_report_verbose_includes_executed_commands_and_artifacts(
    config: ToolsConfig, tmp_path: Path
) -> None:
    runner = MetricsRunner()
    tools = testing_module.TestingTools(config, tmp_path, runner)

    coverage_dir = tmp_path / ".cache" / "coverage"
    coverage_dir.mkdir(parents=True, exist_ok=True)
    write_coverage_file(tmp_path)
    create_sample_source_file(tmp_path)
    fingerprint = tools._compute_coverage_fingerprint()
    write_manifest(tmp_path, fingerprint)

    html_dir = coverage_dir / "htmlcov"
    html_dir.mkdir(exist_ok=True)
    (coverage_dir / "coverage.xml").write_text("<xml />", encoding="utf-8")

    result = tools.coverage_report(["--skip-empty"], verbose=True)

    assert result.success is True
    assert "coverage report -m" in result.stdout
    assert "coverage html" in result.stdout
    assert "Coverage artifacts:" in result.stdout
    assert "coverage.xml" in result.stdout
    assert "htmlcov" in result.stdout


def test_coverage_report_records_first_failed_command(
    config: ToolsConfig, tmp_path: Path
) -> None:
    runner = ReportFailureRunner()
    tools = testing_module.TestingTools(config, tmp_path, runner)

    coverage_dir = tmp_path / ".cache" / "coverage"
    coverage_dir.mkdir(parents=True, exist_ok=True)
    write_coverage_file(tmp_path)
    create_sample_source_file(tmp_path)
    fingerprint = tools._compute_coverage_fingerprint()
    write_manifest(tmp_path, fingerprint)

    coverage_payload = {
        "totals": {
            "num_statements": 10,
            "covered_lines": 9,
            "num_branches": 2,
            "covered_branches": 1,
        },
        "files": {},
    }
    (coverage_dir / "coverage.json").write_text(
        json.dumps(coverage_payload), encoding="utf-8"
    )

    result = tools.coverage_report([], verbose=False)

    assert result.success is False
    assert "terminal report failed" in result.stderr


def test_coverage_learning_mode_combines_commands(
    config: ToolsConfig, tmp_path: Path
) -> None:
    runner = MetricsRunner()
    tools = testing_module.TestingTools(config, tmp_path, runner)

    write_coverage_file(tmp_path)
    create_sample_source_file(tmp_path)
    fingerprint = tools._compute_coverage_fingerprint()
    write_manifest(tmp_path, fingerprint)

    result = tools.coverage(
        [],
        line_threshold=10.0,
        branch_threshold=5.0,
        verbose=False,
        learning_mode=True,
        verbosity_level=2,
    )

    assert result.learning_info.commands_executed
    assert result.learning_info.explanations
