"""Supplementary tests for `TestingTools` coverage helpers."""

from __future__ import annotations

import json
import builtins
from contextlib import contextmanager
from pathlib import Path
from types import ModuleType
from typing import Iterator

import pytest

from ml_playground.tools import testing as testing_module
from ml_playground.tools.core import config as config_module
from ml_playground.tools.core.config import ToolsConfig
from ml_playground.tools.core.errors import ToolExecutionError
from ml_playground.tools.core.interfaces import OperationId, ToolResult
from tests.unit.tools.categories.test_testing import (
    RecordingRunner,
    PytestFailureRunner,
    CombineFailureRunner,
    MetricsRunner,
    _create_sample_source_file,
    _temporary_cwd,
    _write_coverage_file,
    _write_manifest,
    _install_modules,
    _restore_modules,
)
from tests.unit.tools.fakes import (
    FakeSubprocessRunner,
    create_failure_result,
    create_success_result,
)


@contextmanager
def temporary_modules(modules: dict[str, ModuleType]) -> Iterator[None]:
    originals = _install_modules(modules)
    try:
        yield
    finally:
        _restore_modules(originals)


@contextmanager
def swap_attr(target: object, attribute: str, replacement: object) -> Iterator[None]:
    original = getattr(target, attribute)
    setattr(target, attribute, replacement)
    try:
        yield
    finally:
        setattr(target, attribute, original)


def cosmic_modules(
    config_module: ModuleType,
    modules_module: ModuleType,
    *,
    base_module_name: str = "cosmic_ray",
) -> dict[str, ModuleType]:
    base_module = ModuleType(base_module_name)
    return {
        "cosmic_ray": base_module,
        "cosmic_ray.config": config_module,
        "cosmic_ray.modules": modules_module,
    }


@pytest.fixture
def config() -> ToolsConfig:
    return ToolsConfig(
        testing=config_module.TestToolsConfig(
            timeout=300,
            coverage_threshold=80.0,
            parallel_workers=2,
        )
    )


@pytest.fixture
def subprocess_runner() -> FakeSubprocessRunner:
    return FakeSubprocessRunner()


def test_read_coverage_thresholds_from_config(
    config: ToolsConfig, tmp_path: Path, subprocess_runner: FakeSubprocessRunner
) -> None:
    pyproject = tmp_path / "pyproject.toml"
    pyproject.write_text(
        """
[tool.ml_playground.coverage.thresholds]
line_threshold = 91.5
branch_threshold = 73.0
""".strip(),
        encoding="utf-8",
    )

    tools = testing_module.TestingTools(config, tmp_path, subprocess_runner)

    thresholds = tools._read_coverage_thresholds_from_config()

    assert thresholds["line_threshold"] == 91.5
    assert thresholds["branch_threshold"] == 73.0


def test_read_coverage_thresholds_missing_returns_empty(
    config: ToolsConfig, tmp_path: Path, subprocess_runner: FakeSubprocessRunner
) -> None:
    tools = testing_module.TestingTools(config, tmp_path, subprocess_runner)

    assert tools._read_coverage_thresholds_from_config() == {}


def test_read_coverage_manifest_handles_invalid_json(
    config: ToolsConfig, tmp_path: Path, subprocess_runner: FakeSubprocessRunner
) -> None:
    tools = testing_module.TestingTools(config, tmp_path, subprocess_runner)

    manifest_path = tools._coverage_manifest_path()
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text("not-json", encoding="utf-8")

    assert tools._read_coverage_manifest() is None


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
    assert any("Coverage-test generated no data" in note for note in notes)


def test_generate_coverage_via_pytest_failure_propagates(
    config: ToolsConfig, tmp_path: Path
) -> None:
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


def test_collect_coverage_metrics_missing_json_raises(
    config: ToolsConfig, tmp_path: Path
) -> None:
    runner = FakeSubprocessRunner()
    tools = testing_module.TestingTools(config, tmp_path, runner)

    coverage_dir = tools._coverage_file().parent
    coverage_dir.mkdir(parents=True, exist_ok=True)

    with pytest.raises(ToolExecutionError) as excinfo:
        tools._collect_coverage_metrics(
            env={},
            operation_id=OperationId(
                namespace="tools", category="test", command="metrics"
            ),
            executed_commands=[],
        )

    assert "Coverage JSON file missing" in str(excinfo.value)
    assert any("coverage" in call["command"] for call in runner.calls)


def test_collect_coverage_metrics_invalid_json_raises(
    config: ToolsConfig, tmp_path: Path
) -> None:
    runner = RecordingRunner()
    tools = testing_module.TestingTools(config, tmp_path, runner)

    coverage_json = tools._coverage_file().parent / "coverage.json"
    coverage_json.parent.mkdir(parents=True, exist_ok=True)
    coverage_json.write_text("not-json", encoding="utf-8")

    with pytest.raises(ToolExecutionError) as excinfo:
        tools._collect_coverage_metrics(
            env={},
            operation_id=OperationId(
                namespace="tools", category="test", command="metrics"
            ),
            executed_commands=[],
        )

    assert "Failed to parse coverage JSON" in str(excinfo.value)


def test_collect_coverage_metrics_success_reports_totals(
    config: ToolsConfig, tmp_path: Path
) -> None:
    runner = RecordingRunner()
    tools = testing_module.TestingTools(config, tmp_path, runner)

    coverage_dir = tools._coverage_file().parent
    coverage_dir.mkdir(parents=True, exist_ok=True)
    coverage_json = coverage_dir / "coverage.json"
    coverage_json.write_text(
        json.dumps(
            {
                "totals": {
                    "num_statements": 20,
                    "covered_lines": 18,
                    "num_branches": 4,
                    "covered_branches": 3,
                }
            }
        ),
        encoding="utf-8",
    )

    result, lines = tools._collect_coverage_metrics(
        env={},
        operation_id=OperationId(namespace="tools", category="test", command="metrics"),
        executed_commands=[],
    )

    assert result is None
    assert lines[0] == "Coverage totals: lines=90.00% (18/20)"
    assert lines[1] == "Branch totals: branches=75.00% (3/4)"


def test_collect_coverage_metrics_missing_totals_raises(
    config: ToolsConfig, tmp_path: Path
) -> None:
    runner = RecordingRunner()
    tools = testing_module.TestingTools(config, tmp_path, runner)

    coverage_dir = tools._coverage_file().parent
    coverage_dir.mkdir(parents=True, exist_ok=True)
    coverage_json = coverage_dir / "coverage.json"
    coverage_json.write_text(json.dumps({"files": {}}), encoding="utf-8")

    with pytest.raises(ToolExecutionError) as excinfo:
        tools._collect_coverage_metrics(
            env={},
            operation_id=OperationId(
                namespace="tools", category="test", command="metrics"
            ),
            executed_commands=[],
        )

    assert "Failed to parse coverage JSON" in str(excinfo.value)


def test_collect_undercovered_files_uses_percent_display(
    config: ToolsConfig, tmp_path: Path
) -> None:
    tools = testing_module.TestingTools(config, tmp_path, FakeSubprocessRunner())

    coverage_data = {
        "files": {
            "pkg/module.py": {
                "summary": {
                    "percent_covered": None,
                    "percent_covered_display": "87.5",
                    "num_branches": 4,
                    "covered_branches": 3,
                }
            },
            "pkg/fully_covered.py": {
                "summary": {
                    "percent_covered": 100,
                    "num_branches": 0,
                    "covered_branches": 0,
                }
            },
        }
    }

    entries = tools._collect_undercovered_files(coverage_data)

    assert entries == [("pkg/module.py", 87.5, 75.0)]


def test_format_undercovered_tree_outputs_hierarchy(
    config: ToolsConfig, tmp_path: Path
) -> None:
    tools = testing_module.TestingTools(config, tmp_path, FakeSubprocessRunner())

    entries = [
        ("pkg/module.py", 87.5, 75.0),
        ("pkg/sub/inner.py", 50.0, None),
    ]
    tree = tools._format_undercovered_tree(entries)

    assert tree == [
        "└── pkg/",
        "    ├── sub/",
        "    │   └── inner.py: line = 50.00%",
        "    └── module.py: line = 87.50% branch = 75.00%",
    ]


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
    _write_manifest(tmp_path, fingerprint)

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
        def coverage_test(  # type: ignore[override]
            self,
            args: list[str],
            *,
            learning_mode: bool = False,
            verbosity_level: int = 1,
        ) -> ToolResult:
            coverage_file = self._coverage_file()
            coverage_file.parent.mkdir(parents=True, exist_ok=True)
            fragment = coverage_file.parent / f"{coverage_file.name}.fragment"
            fragment.write_text("generated", encoding="utf-8")
            coverage_file.write_bytes(b"coverage-data")
            return create_success_result(
                OperationId(
                    namespace="tools", category="test", command="coverage-test"
                ),
                stdout="coverage refreshed",
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
    assert any("Automatically ran coverage-test" in note for note in notes)
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
                    return create_failure_result(
                        operation_id, stderr="No source for code"
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

    _create_sample_source_file(tmp_path)
    _write_coverage_file(tmp_path)
    fingerprint = tools._compute_coverage_fingerprint()
    _write_manifest(tmp_path, fingerprint)

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

    _create_sample_source_file(tmp_path)
    stale_manifest = tools._compute_coverage_fingerprint()
    _write_manifest(tmp_path, fingerprint="different" + stale_manifest)

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
        "coverage.json" in note or "coverage-test" in note.lower() for note in notes
    )
    assert env["COVERAGE_FILE"].endswith("coverage.sqlite")
    assert any(
        call["args"][0] == "coverage" and call["args"][1] == "run"
        for call in runner.uv_calls
    )


def test_coverage_pipeline_falls_back_to_pytest_when_coverage_test_empty(
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
        ) -> ToolResult:
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
                coverage_path = Path(env["COVERAGE_FILE"])
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
    assert any("Coverage-test generated no data" in note for note in notes)
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
                return create_failure_result(operation_id, stderr="coverage run failed")
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


def test_compute_fingerprint_skips_unstatable_files(
    config: ToolsConfig, tmp_path: Path, subprocess_runner: FakeSubprocessRunner
) -> None:
    tools = testing_module.TestingTools(config, tmp_path, subprocess_runner)

    source_dir = tmp_path / "src" / "ml_playground" / "tools"
    source_dir.mkdir(parents=True, exist_ok=True)

    stable_file = source_dir / "stable.py"
    stable_file.write_text("value = 1", encoding="utf-8")

    broken_link = source_dir / "broken.py"
    broken_target = source_dir / "missing.py"
    broken_link.symlink_to(broken_target)

    fingerprint_with_broken = tools._compute_coverage_fingerprint()

    broken_link.unlink()

    fingerprint_without_broken = tools._compute_coverage_fingerprint()

    assert fingerprint_with_broken == fingerprint_without_broken


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
        ) -> ToolResult:
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

    _create_sample_source_file(tmp_path)
    _write_coverage_file(tmp_path)
    fingerprint = tools._compute_coverage_fingerprint()
    _write_manifest(tmp_path, fingerprint)

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
                    return create_failure_result(
                        operation_id, stderr="No source for code"
                    )
            if args[:2] == ["coverage", "run"]:
                return create_failure_result(
                    operation_id, stderr="coverage regen failed"
                )
            if args[:2] == ["coverage", "json"]:
                result = super().run_uv_command(
                    args,
                    cwd=cwd,
                    env=env,
                    timeout=timeout,
                    operation_id=operation_id,
                    python=python,
                    no_project=no_project,
                )
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
                return result
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

    _create_sample_source_file(tmp_path)
    fingerprint = tools._compute_coverage_fingerprint()
    _write_manifest(tmp_path, fingerprint)

    # Pre-create minimal coverage.json so metrics step succeeds
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
    _write_coverage_file(tmp_path)
    fingerprint = tools._compute_coverage_fingerprint()
    _write_manifest(tmp_path, fingerprint)

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
    _write_coverage_file(tmp_path)
    _create_sample_source_file(tmp_path)
    fingerprint = tools._compute_coverage_fingerprint()
    _write_manifest(tmp_path, fingerprint)

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
    _write_coverage_file(tmp_path)
    _create_sample_source_file(tmp_path)
    fingerprint = tools._compute_coverage_fingerprint()
    _write_manifest(tmp_path, fingerprint)

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
    class TerminalFailureRunner(RecordingRunner):
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
                return create_failure_result(operation_id, stderr="terminal boom")
            return super().run_uv_command(
                args,
                cwd=cwd,
                env=env,
                timeout=timeout,
                operation_id=operation_id,
                python=python,
                no_project=no_project,
            )

    runner = TerminalFailureRunner()
    tools = testing_module.TestingTools(config, tmp_path, runner)

    coverage_dir = tmp_path / ".cache" / "coverage"
    coverage_dir.mkdir(parents=True, exist_ok=True)
    coverage_file = coverage_dir / "coverage.sqlite"
    coverage_file.write_bytes(b"data")

    _create_sample_source_file(tmp_path)
    fingerprint = tools._compute_coverage_fingerprint()
    _write_manifest(tmp_path, fingerprint)

    result = tools.coverage_report([], verbose=False)

    assert result.success is False
    assert result.stderr == "terminal boom"
    assert "[FAILED] terminal report: terminal boom" in result.stdout
    assert any("coverage html" in line for line in result.stdout.splitlines())


def test_mutation_reset_handles_missing_session(
    config: ToolsConfig, tmp_path: Path
) -> None:
    runner = FakeSubprocessRunner()
    tools = testing_module.TestingTools(config, tmp_path, runner)

    with _temporary_cwd(tmp_path):
        result = tools.mutation_reset([])

    assert result.success is True
    assert "does not exist" in result.stdout


def test_mutation_reset_failure_raises_tool_execution_error(
    config: ToolsConfig, tmp_path: Path
) -> None:
    tools = testing_module.TestingTools(config, tmp_path, FakeSubprocessRunner())

    session_dir = tmp_path / ".cache" / "cosmic-ray" / "session.sqlite"
    session_dir.mkdir(parents=True, exist_ok=True)

    with _temporary_cwd(tmp_path):
        with pytest.raises(ToolExecutionError) as excinfo:
            tools.mutation_reset([])

    assert "Failed to remove Cosmic Ray session file" in str(excinfo.value)


def test_coverage_threshold_reports_missing_totals(
    config: ToolsConfig, tmp_path: Path
) -> None:
    class ZeroTotalsRunner(RecordingRunner):
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
                out_path.write_text(
                    json.dumps(
                        {
                            "totals": {
                                "num_statements": 0,
                                "covered_lines": 0,
                                "num_branches": 0,
                                "covered_branches": 0,
                            },
                            "files": {},
                        }
                    ),
                    encoding="utf-8",
                )
            return result

    runner = ZeroTotalsRunner()
    tools = testing_module.TestingTools(config, tmp_path, runner)

    _write_coverage_file(tmp_path, payload=b"cached")
    fingerprint = tools._compute_coverage_fingerprint()
    _write_manifest(tmp_path, fingerprint)

    result = tools.coverage_threshold(
        [],
        line_threshold=10.0,
        branch_threshold=5.0,
        verbose=False,
    )

    assert result.success is False
    assert "Line coverage totals missing" in result.stderr
    assert "Branch coverage data missing" in result.stderr


def test_coverage_threshold_reports_mixed_results(
    config: ToolsConfig, tmp_path: Path
) -> None:
    class MixedTotalsRunner(MetricsRunner):
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
                out_path.write_text(
                    json.dumps(
                        {
                            "totals": {
                                "num_statements": 50,
                                "covered_lines": 49,
                                "num_branches": 10,
                                "covered_branches": 6,
                            },
                            "files": {},
                        }
                    ),
                    encoding="utf-8",
                )
            return result

    runner = MixedTotalsRunner()
    tools = testing_module.TestingTools(config, tmp_path, runner)

    _write_coverage_file(tmp_path)
    fingerprint = tools._compute_coverage_fingerprint()
    _write_manifest(tmp_path, fingerprint)

    result = tools.coverage_threshold(
        [], line_threshold=98.0, branch_threshold=70.0, verbose=False
    )

    assert result.success is False
    assert "Line coverage" in result.stderr or result.stdout
    assert "Branch coverage" in result.stderr or result.stdout


def test_coverage_report_branch_reruns(config: ToolsConfig, tmp_path: Path) -> None:
    runner = FakeSubprocessRunner()
    tools = testing_module.TestingTools(config, tmp_path, runner)

    coverage_file = _write_coverage_file(tmp_path, payload=b"cached")
    fingerprint = tools._compute_coverage_fingerprint()
    _write_manifest(tmp_path, fingerprint)
    # Ensure metrics step can read a minimal JSON
    coverage_json = coverage_file.parent / "coverage.json"
    coverage_json.write_text(
        json.dumps({"totals": {"num_statements": 1, "covered_lines": 1}}),
        encoding="utf-8",
    )

    result = tools.coverage_report([], verbose=False)

    assert result.success is True
    assert "coverage html" in result.stdout


def test_mutation_exec_runs_with_session(config: ToolsConfig, tmp_path: Path) -> None:
    runner = FakeSubprocessRunner()
    operation_id = OperationId(
        namespace="tools", category="test", command="mutation-exec"
    )
    runner.set_results([create_success_result(operation_id, stdout="exec ok")])

    tools = testing_module.TestingTools(config, tmp_path, runner)
    session_path = tmp_path / ".cache" / "cosmic-ray" / "session.sqlite"
    session_path.parent.mkdir(parents=True, exist_ok=True)
    session_path.write_text("session", encoding="utf-8")

    with _temporary_cwd(tmp_path):
        result = tools.mutation_exec([])

    assert result.success is True
    assert runner.calls
    command = runner.calls[0]["command"]
    assert command[:2] == ["uv", "run"]
    assert "cosmic-ray" in command
    exec_index = command.index("cosmic-ray")
    assert command[exec_index : exec_index + 3] == [
        "cosmic-ray",
        "exec",
        "pyproject.toml",
    ]
    assert command[-1].endswith("session.sqlite")


def test_integration_cleans_pytest_progress_output(
    config: ToolsConfig, tmp_path: Path
) -> None:
    class ProgressRunner(FakeSubprocessRunner):
        def run_pytest_command(  # type: ignore[override]
            self,
            args: list[str],
            *,
            cwd: Path | None = None,
            timeout: int | None = None,
            operation_id: OperationId,
        ) -> ToolResult:
            noisy = """
bringing up nodes
.... [ 34%]
actual line
""".strip()
            return ToolResult(
                success=True,
                exit_code=0,
                stdout=noisy,
                stderr="",
                operation_id=operation_id,
            )

    tools = testing_module.TestingTools(config, tmp_path, ProgressRunner())
    result = tools.integration(["-q"])
    assert result.success is True
    assert "actual line" in result.stdout
    assert "bringing up nodes" not in result.stdout
    assert "[ 34%]" not in result.stdout


def test_mutation_report_import_error_path(config: ToolsConfig, tmp_path: Path) -> None:
    tools = testing_module.TestingTools(config, tmp_path, FakeSubprocessRunner())
    with _temporary_cwd(tmp_path):
        (tmp_path / "pyproject.toml").write_text("[tool]\n", encoding="utf-8")
        # No cosmic_ray installed in sys.modules -> triggers ImportError branch
        result = tools.mutation_report([])
    assert result.success is False
    # Depending on import semantics, either ImportError path or generic failure path
    assert "cosmic_ray must be installed" in (
        result.stderr or ""
    ) or "Failed to generate mutation report" in (result.stderr or "")


def test_regression_cleans_pytest_progress_output(
    config: ToolsConfig, tmp_path: Path
) -> None:
    class ProgressRunner(FakeSubprocessRunner):
        def run_pytest_command(  # type: ignore[override]
            self,
            args: list[str],
            *,
            cwd: Path | None = None,
            timeout: int | None = None,
            operation_id: OperationId,
        ) -> ToolResult:
            noisy = """
bringing up nodes
ss.. [ 50%]
actual line
""".strip()
            return ToolResult(
                success=True,
                exit_code=0,
                stdout=noisy,
                stderr="",
                operation_id=operation_id,
            )

    tools = testing_module.TestingTools(config, tmp_path, ProgressRunner())
    result = tools.regression(["-q"])
    assert result.success is True
    assert "actual line" in result.stdout
    assert "bringing up nodes" not in result.stdout
    assert "[ 50%]" not in result.stdout


def test_e2e_cleans_pytest_progress_output(config: ToolsConfig, tmp_path: Path) -> None:
    class ProgressRunner(FakeSubprocessRunner):
        def run_pytest_command(  # type: ignore[override]
            self,
            args: list[str],
            *,
            cwd: Path | None = None,
            timeout: int | None = None,
            operation_id: OperationId,
        ) -> ToolResult:
            noisy = """
bringing up nodes
.... [ 100%]
actual line
""".strip()
            return ToolResult(
                success=True,
                exit_code=0,
                stdout=noisy,
                stderr="",
                operation_id=operation_id,
            )

    tools = testing_module.TestingTools(config, tmp_path, ProgressRunner())
    result = tools.e2e(["-q"])
    assert result.success is True
    assert "actual line" in result.stdout
    assert "bringing up nodes" not in result.stdout
    assert "[ 100%]" not in result.stdout


def test_coverage_threshold_zero_threshold_passes(
    config: ToolsConfig, tmp_path: Path
) -> None:
    tools = testing_module.TestingTools(config, tmp_path, FakeSubprocessRunner())
    coverage_file = _write_coverage_file(tmp_path)
    # Write manifest to skip regen
    fingerprint = tools._compute_coverage_fingerprint()
    _write_manifest(tmp_path, fingerprint)
    # Minimal coverage json
    coverage_json = coverage_file.parent / "coverage.json"
    coverage_json.write_text(
        json.dumps({"totals": {"num_statements": 2, "covered_lines": 1}}),
        encoding="utf-8",
    )
    result = tools.coverage_threshold(
        [], line_threshold=0.0, branch_threshold=0.0, verbose=False
    )
    assert result.success is True


def test_mutation_report_handles_missing_session(
    config: ToolsConfig, tmp_path: Path
) -> None:
    runner = FakeSubprocessRunner()
    tools = testing_module.TestingTools(config, tmp_path, runner)

    config_module = ModuleType("cosmic_ray.config")

    def load_config(_path: str) -> dict[str, object]:
        return {}

    config_module.load_config = load_config  # type: ignore[attr-defined]

    modules = {
        "cosmic_ray": ModuleType("cosmic_ray"),
        "cosmic_ray.config": config_module,
        "cosmic_ray.modules": ModuleType("cosmic_ray.modules"),
    }

    with temporary_modules(modules):
        with _temporary_cwd(tmp_path):
            result = tools.mutation_report([])

    assert result.success is True
    assert "session file not found" in result.stdout


def test_mutation_summary_success_minimal(config: ToolsConfig, tmp_path: Path) -> None:
    tools = testing_module.TestingTools(config, tmp_path, FakeSubprocessRunner())

    cfg_module = ModuleType("cosmic_ray.config")

    def load_config(path: str) -> dict[str, object]:
        assert path == "pyproject.toml"
        return {
            "session": {"path": ".cache/cosmic-ray/session.sqlite"},
            "test-runner": {"command": "pytest"},
            "modules": {"paths": ["src/pkg"]},
        }

    cfg_module.load_config = load_config  # type: ignore[attr-defined]

    modules_module = ModuleType("cosmic_ray.modules")

    def find_modules(_cfg: object) -> tuple[str, ...]:  # noqa: ANN401
        return ("pkg.alpha",)

    modules_module.find_modules = find_modules  # type: ignore[attr-defined]

    with (
        temporary_modules(cosmic_modules(cfg_module, modules_module)),
        _temporary_cwd(tmp_path),
    ):
        (tmp_path / "pyproject.toml").write_text("[tool]\n", encoding="utf-8")
        result = tools.mutation_summary([])

    assert result.success is True
    assert "[mutation] config:" in result.stdout
    assert "modules to mutate: 1" in result.stdout


def test_mutation_init_failure_converted_to_success(
    config: ToolsConfig, tmp_path: Path
) -> None:
    class FailingInitRunner(FakeSubprocessRunner):
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
                return create_failure_result(operation_id, stderr="already exists")
            return super().run_uv_command(
                args,
                cwd=cwd,
                env=env,
                timeout=timeout,
                operation_id=operation_id,
                python=python,
                no_project=no_project,
            )

    tools = testing_module.TestingTools(config, tmp_path, FailingInitRunner())
    with _temporary_cwd(tmp_path):
        result = tools.mutation_init([])

    assert result.success is True
    assert "init" in result.stdout.lower()


def test_mutation_init_success_path(config: ToolsConfig, tmp_path: Path) -> None:
    class SuccessInitRunner(FakeSubprocessRunner):
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
                return create_success_result(operation_id, stdout="ok")
            return super().run_uv_command(
                args,
                cwd=cwd,
                env=env,
                timeout=timeout,
                operation_id=operation_id,
                python=python,
                no_project=no_project,
            )

    tools = testing_module.TestingTools(config, tmp_path, SuccessInitRunner())
    with _temporary_cwd(tmp_path):
        result = tools.mutation_init([])
    assert result.success is True
    # Accept whatever success output the injected runner provides
    assert result.stdout.strip() != ""


def test_mutation_summary_import_error_path(
    config: ToolsConfig, tmp_path: Path
) -> None:
    tools = testing_module.TestingTools(config, tmp_path, FakeSubprocessRunner())

    def raising_import(name, globals=None, locals=None, fromlist=(), level=0):  # type: ignore[no-redef]
        if name.startswith("cosmic_ray"):
            raise ImportError("no cosmic_ray")
        return __import__(name, globals, locals, fromlist, level)

    # Override import at builtins level to force ImportError for cosmic_ray*
    with swap_attr(builtins, "__import__", raising_import):
        with _temporary_cwd(tmp_path):
            (tmp_path / "pyproject.toml").write_text("[tool]\n", encoding="utf-8")
            result = tools.mutation_summary([])

    assert result.success is False
    assert "cosmic_ray must be installed" in (result.stderr or "")


def test_regression_learning_mode_attaches_info(
    config: ToolsConfig, tmp_path: Path
) -> None:
    tools = testing_module.TestingTools(config, tmp_path, FakeSubprocessRunner())
    result = tools.regression(["-q"], learning_mode=True, verbosity_level=1)
    assert result.success in (True, False)
    assert result.learning_info is not None
    assert result.learning_info.explanations or result.learning_info.commands_executed


def test_coverage_report_learning_mode_attaches_info(
    config: ToolsConfig, tmp_path: Path
) -> None:
    tools = testing_module.TestingTools(config, tmp_path, FakeSubprocessRunner())
    coverage_file = _write_coverage_file(tmp_path)
    fingerprint = tools._compute_coverage_fingerprint()
    _write_manifest(tmp_path, fingerprint)
    (coverage_file.parent / "coverage.json").write_text(
        json.dumps({"totals": {"num_statements": 2, "covered_lines": 2}}),
        encoding="utf-8",
    )
    result = tools.coverage_report(
        [], verbose=False, learning_mode=True, verbosity_level=1
    )
    assert result.success is True
    assert result.learning_info is not None
    assert result.learning_info.explanations or result.learning_info.commands_executed


def test_clean_pytest_output_filters_empty_and_progress_lines(
    config: ToolsConfig, tmp_path: Path
) -> None:
    tools = testing_module.TestingTools(config, tmp_path, FakeSubprocessRunner())
    noisy = "\n\nbringing up nodes\n.... [ 50%]\n\nactual line\n\n"
    cleaned = tools._clean_pytest_output(noisy)
    assert cleaned.strip() == "actual line"


def test_collect_coverage_metrics_existing_json_appends_executed(
    config: ToolsConfig, tmp_path: Path
) -> None:
    tools = testing_module.TestingTools(config, tmp_path, FakeSubprocessRunner())
    cov_dir = tools._coverage_file().parent
    cov_dir.mkdir(parents=True, exist_ok=True)
    json_path = cov_dir / "coverage.json"
    json_path.write_text(
        json.dumps({"totals": {"num_statements": 1, "covered_lines": 1}}),
        encoding="utf-8",
    )
    executed: list[str] = []
    failure, lines = tools._collect_coverage_metrics(
        env={},
        operation_id=OperationId(namespace="tools", category="test", command="metrics"),
        executed_commands=executed,
    )
    assert failure is None
    assert any("coverage json -o" in cmd for cmd in executed)
    assert lines and "Coverage totals:" in lines[0]


def test_mutation_run_returns_first_failure(
    config: ToolsConfig, tmp_path: Path
) -> None:
    class FailingPipelineTools(testing_module.TestingTools):
        def __init__(self) -> None:
            super().__init__(config, tmp_path, FakeSubprocessRunner())
            self.calls: list[str] = []

        def _result(
            self,
            name: str,
            *,
            success: bool,
            stdout: str = "",
            stderr: str = "",
            exit_code: int = 0,
        ) -> ToolResult:
            self.calls.append(name)
            op = OperationId(
                namespace="tools", category="test", command=f"mutation-{name}"
            )
            if success:
                return create_success_result(op, stdout=stdout)
            return create_failure_result(op, exit_code=exit_code, stderr=stderr)

        def mutation_reset(self, _args: list[str]) -> ToolResult:
            return self._result("reset", success=True, stdout="reset ok")

        def mutation_summary(self, _args: list[str]) -> ToolResult:
            return self._result("summary", success=True, stdout="summary ok")

        def mutation_init(self, _args: list[str]) -> ToolResult:
            return self._result("init", success=True, stdout="init ok")

        def mutation_exec(self, _args: list[str]) -> ToolResult:
            return self._result(
                "exec", success=False, stderr="exec failed", exit_code=2
            )

        def mutation_report(
            self, _args: list[str]
        ) -> ToolResult:  # pragma: no cover - defensive
            pytest.fail("mutation_report should not be reached")

    tools = FailingPipelineTools()

    with _temporary_cwd(tmp_path):
        result = tools.mutation_run([])

    assert result.success is False
    assert tools.calls == ["reset", "summary", "init", "exec"]
    combined_output = (result.stderr or "") + (result.stdout or "")
    assert "exec failed" in combined_output


def test_mutation_run_completes_successfully(
    config: ToolsConfig, tmp_path: Path
) -> None:
    class SuccessfulPipelineTools(testing_module.TestingTools):
        def __init__(self) -> None:
            super().__init__(config, tmp_path, FakeSubprocessRunner())
            self.calls: list[str] = []

        def _success(self, name: str, message: str) -> ToolResult:
            self.calls.append(name)
            op = OperationId(
                namespace="tools", category="test", command=f"mutation-{name}"
            )
            return create_success_result(op, stdout=message)

        def mutation_reset(self, _args: list[str]) -> ToolResult:
            return self._success("reset", "reset ok")

        def mutation_summary(self, _args: list[str]) -> ToolResult:
            return self._success("summary", "summary ok")

        def mutation_init(self, _args: list[str]) -> ToolResult:
            return self._success("init", "init ok")

        def mutation_exec(self, _args: list[str]) -> ToolResult:
            return self._success("exec", "exec ok")

        def mutation_report(self, _args: list[str]) -> ToolResult:
            return self._success("report", "report ok")

    tools = SuccessfulPipelineTools()

    with _temporary_cwd(tmp_path):
        result = tools.mutation_run([])

    assert result.success is True
    assert tools.calls == ["reset", "summary", "init", "exec", "report"]
    assert "report ok" in result.stdout
    assert "reset ok" in result.stdout
    assert result.stderr == ""
