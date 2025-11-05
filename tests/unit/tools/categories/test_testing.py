"""Unit tests for `TestingTools` coverage and test commands."""

from __future__ import annotations

import json
import os
import sys
from contextlib import contextmanager
from pathlib import Path
from types import ModuleType

import pytest

from ml_playground.tools import testing as testing_module
from ml_playground.tools.core import config as config_module
from ml_playground.tools.core.errors import ToolExecutionError
from ml_playground.tools.core.config import ToolsConfig
from ml_playground.tools.core.interfaces import OperationId, ToolResult
from tests.unit.tools.fakes import (
    FakeSubprocessRunner,
    create_failure_result,
    create_success_result,
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


@pytest.fixture
def root_path(tmp_path: Path) -> Path:
    return tmp_path


@pytest.fixture
def subprocess_runner() -> FakeSubprocessRunner:
    return FakeSubprocessRunner()


@pytest.fixture
def testing_tools(
    config: ToolsConfig, root_path: Path, subprocess_runner: FakeSubprocessRunner
) -> testing_module.TestingTools:
    return testing_module.TestingTools(config, root_path, subprocess_runner)


@contextmanager
def _temporary_cwd(path: Path):
    current = Path.cwd()
    os.chdir(path)
    try:
        yield
    finally:
        os.chdir(current)


def _install_modules(modules: dict[str, ModuleType]) -> dict[str, ModuleType | None]:
    originals: dict[str, ModuleType | None] = {}
    for name, module in modules.items():
        originals[name] = sys.modules.get(name)
        sys.modules[name] = module
    return originals


def _restore_modules(originals: dict[str, ModuleType | None]) -> None:
    for name, module in originals.items():
        if module is None:
            sys.modules.pop(name, None)
        else:
            sys.modules[name] = module


class TestTestingToolsInit:
    def test_init(
        self,
        testing_tools: testing_module.TestingTools,
        config: ToolsConfig,
        root_path: Path,
    ) -> None:
        assert testing_tools.config == config
        assert testing_tools.root_path == root_path
        assert testing_tools.cache_dir == root_path / ".cache"
        assert testing_tools.category == "test"


class TestUnitTests:
    def test_unit_success(
        self,
        testing_tools: testing_module.TestingTools,
        subprocess_runner: FakeSubprocessRunner,
    ) -> None:
        operation_id = OperationId(namespace="tools", category="test", command="unit")
        expected_result = create_success_result(operation_id, "unit output")
        subprocess_runner.set_results([expected_result])

        result = testing_tools.unit(["--verbose"])

        assert result.success is True
        assert result.exit_code == 0
        assert str(result.operation_id) == "tools.test.unit"
        assert len(subprocess_runner.calls) == 1
        assert "tests/unit" in subprocess_runner.calls[0]["command"]
        assert "--verbose" in subprocess_runner.calls[0]["command"]

    def test_unit_failure(
        self,
        testing_tools: testing_module.TestingTools,
        subprocess_runner: FakeSubprocessRunner,
    ) -> None:
        operation_id = OperationId(namespace="tools", category="test", command="unit")
        expected_result = create_failure_result(operation_id, 1, "", "failed")
        subprocess_runner.set_results([expected_result])

        result = testing_tools.unit([])

        assert result.success is False
        assert result.exit_code == 1


class TestIntegrationTests:
    def test_integration_success(
        self,
        testing_tools: testing_module.TestingTools,
        subprocess_runner: FakeSubprocessRunner,
    ) -> None:
        operation_id = OperationId(
            namespace="tools", category="test", command="integration"
        )
        expected_result = create_success_result(operation_id, "integration output")
        subprocess_runner.set_results([expected_result])

        result = testing_tools.integration([])

        assert result.success is True
        assert str(result.operation_id) == "tools.test.integration"
        assert len(subprocess_runner.calls) == 1
        command = subprocess_runner.calls[0]["command"]
        assert "-m" in command and "integration" in command and "--no-cov" in command


class TestE2ETests:
    def test_e2e_success(
        self,
        testing_tools: testing_module.TestingTools,
        subprocess_runner: FakeSubprocessRunner,
    ) -> None:
        operation_id = OperationId(namespace="tools", category="test", command="e2e")
        expected_result = create_success_result(operation_id, "e2e output")
        subprocess_runner.set_results([expected_result])

        result = testing_tools.e2e([])

        assert result.success is True
        assert "tests/e2e" in subprocess_runner.calls[0]["command"]


class TestAcceptanceTests:
    def test_acceptance_success(
        self,
        testing_tools: testing_module.TestingTools,
        subprocess_runner: FakeSubprocessRunner,
    ) -> None:
        operation_id = OperationId(
            namespace="tools", category="test", command="acceptance"
        )
        expected_result = create_success_result(operation_id, "acceptance output")
        subprocess_runner.set_results([expected_result])

        result = testing_tools.acceptance([])

        assert result.success is True
        assert "tests/acceptance" in subprocess_runner.calls[0]["command"]


class TestPropertyTests:
    def test_property_success(
        self,
        testing_tools: testing_module.TestingTools,
        subprocess_runner: FakeSubprocessRunner,
    ) -> None:
        operation_id = OperationId(
            namespace="tools", category="test", command="property"
        )
        expected_result = create_success_result(operation_id, "property output")
        subprocess_runner.set_results([expected_result])

        result = testing_tools.property_tests([])

        assert result.success is True
        assert "tests/property" in subprocess_runner.calls[0]["command"]


class TestAllTests:
    def test_all_success(
        self,
        testing_tools: testing_module.TestingTools,
        subprocess_runner: FakeSubprocessRunner,
    ) -> None:
        operation_id = OperationId(namespace="tools", category="test", command="all")
        expected_result = create_success_result(operation_id, "all output")
        subprocess_runner.set_results([expected_result])

        result = testing_tools.all_tests([])

        assert result.success is True
        command = subprocess_runner.calls[0]["command"]
        for suite in ("tests/unit", "tests/property", "tests/regression"):
            assert suite in command


class TestCoverageTest:
    def test_coverage_test_success(
        self,
        testing_tools: testing_module.TestingTools,
        subprocess_runner: FakeSubprocessRunner,
    ) -> None:
        operation_id = OperationId(
            namespace="tools", category="test", command="coverage-test"
        )
        expected_result = create_success_result(operation_id, "coverage output")
        subprocess_runner.set_results([expected_result])

        result = testing_tools.coverage_test([])

        assert result.success is True
        command = subprocess_runner.calls[0]["command"]
        assert (
            "coverage" in command
            and "tests/unit" in command
            and "tests/property" in command
        )


class TestClean:
    def test_clean_success(self, testing_tools: testing_module.TestingTools) -> None:
        result = testing_tools.clean([])

        assert result.success is True
        assert "Cleaned" in result.stdout or "No artifacts to clean" in result.stdout


def test_coverage_threshold_auto_generates_and_reports_totals(
    config: ToolsConfig, tmp_path: Path
) -> None:
    runner = RecordingRunner()
    tools = testing_module.TestingTools(config, tmp_path, runner)

    coverage_file = tmp_path / ".cache" / "coverage" / "coverage.sqlite"
    if coverage_file.exists():
        coverage_file.unlink()

    result = tools.coverage_threshold(
        [], line_threshold=80.0, branch_threshold=70.0, verbose=False
    )

    assert result.success is True
    assert runner.pytest_calls

    stdout_lines = result.stdout.splitlines()
    assert stdout_lines[0].startswith("Executed: coverage json -o ")
    assert "Executed: uv run tools test coverage-test" in stdout_lines
    assert any("Automatically ran coverage-test" in line for line in stdout_lines)
    assert _extract_tree(stdout_lines) == []
    assert "✅ SUCCESS" in result.stderr


def _create_sample_source_file(root_path: Path) -> Path:
    source_dir = root_path / "src" / "ml_playground" / "tools"
    source_dir.mkdir(parents=True, exist_ok=True)
    sample_file = source_dir / "sample_module.py"
    sample_file.write_text("value = 0", encoding="utf-8")
    return sample_file


def _write_manifest(root_path: Path, fingerprint: str) -> Path:
    manifest_path = root_path / ".cache" / "coverage" / "coverage_manifest.json"
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text(json.dumps({"fingerprint": fingerprint}), encoding="utf-8")
    return manifest_path


def _write_coverage_file(root_path: Path, payload: bytes = b"data") -> Path:
    coverage_path = root_path / ".cache" / "coverage" / "coverage.sqlite"
    coverage_path.parent.mkdir(parents=True, exist_ok=True)
    coverage_path.write_bytes(payload)
    return coverage_path


def test_coverage_threshold_reuses_cached_data_when_fingerprint_matches(
    config: ToolsConfig, tmp_path: Path
) -> None:
    runner = RecordingRunner()
    tools = testing_module.TestingTools(config, tmp_path, runner)

    sample_file = _create_sample_source_file(tmp_path)
    _write_coverage_file(tmp_path)
    initial_fingerprint = tools._compute_coverage_fingerprint()
    manifest_path = _write_manifest(tmp_path, initial_fingerprint)

    sample_file.write_text("value = 1", encoding="utf-8")

    result = tools.coverage_threshold([], line_threshold=10.0, branch_threshold=5.0)

    assert result.success is True
    assert any(call["args"][:2] == ["coverage", "run"] for call in runner.uv_calls)
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert manifest["fingerprint"] == tools._compute_coverage_fingerprint()


def test_coverage_threshold_force_regen_overrides_cached_data(
    config: ToolsConfig, tmp_path: Path
) -> None:
    runner = RecordingRunner()
    tools = testing_module.TestingTools(config, tmp_path, runner)

    _create_sample_source_file(tmp_path)
    _write_coverage_file(tmp_path)
    fingerprint = tools._compute_coverage_fingerprint()
    manifest_path = _write_manifest(tmp_path, fingerprint)

    result = tools.coverage_threshold(
        [], line_threshold=10.0, branch_threshold=5.0, force_regen=True
    )

    assert result.success is True
    assert any(call["args"][:2] == ["coverage", "run"] for call in runner.uv_calls)
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert manifest["fingerprint"] == tools._compute_coverage_fingerprint()


def test_ensure_coverage_data_returns_none_when_cache_valid(
    config: ToolsConfig, tmp_path: Path
) -> None:
    """Skip regeneration when coverage cache and fingerprint already match."""
    runner = RecordingRunner()
    tools = testing_module.TestingTools(config, tmp_path, runner)

    coverage_file = tools._coverage_file()
    coverage_file.parent.mkdir(parents=True, exist_ok=True)
    coverage_file.write_bytes(b"existing")
    fingerprint = tools._compute_coverage_fingerprint()
    tools._write_coverage_manifest(fingerprint=fingerprint)

    result, notes, env = tools._ensure_coverage_data(
        args=[],
        learning_mode=False,
        verbosity_level=0,
        verbose=False,
        operation_id=OperationId(namespace="tools", category="test", command="ensure"),
        executed_commands=[],
    )

    assert result is None
    assert notes == []
    assert env["COVERAGE_FILE"] == str(coverage_file)
    assert runner.pytest_calls == []
    assert runner.uv_calls == []


def test_coverage_combines_report_and_threshold_success(
    config: ToolsConfig, tmp_path: Path
) -> None:
    runner = MetricsRunner()
    tools = testing_module.TestingTools(config, tmp_path, runner)

    result = tools.coverage(
        [],
        line_threshold=50.0,
        branch_threshold=10.0,
        verbose=False,
    )

    assert result.success is True
    assert result.exit_code == 0
    assert "Coverage totals:" in result.stdout
    assert any(call["args"][:2] == ["coverage", "run"] for call in runner.uv_calls)


def test_coverage_threshold_json_failure_returns_error(
    config: ToolsConfig, tmp_path: Path
) -> None:
    runner = FailingJsonRunner()
    tools = testing_module.TestingTools(config, tmp_path, runner)

    with pytest.raises(ToolExecutionError) as excinfo:
        tools.coverage_threshold([], line_threshold=10.0, branch_threshold=5.0)

    assert "Failed to generate coverage JSON report" in str(excinfo.value)


def test_ensure_coverage_data_fails_when_no_artifacts_generated(
    config: ToolsConfig, tmp_path: Path
) -> None:
    class NoCoverageRunner(RecordingRunner):
        def run_pytest_command(  # type: ignore[override]
            self,
            args: list[str],
            *,
            cwd: Path | None = None,
            env: dict[str, str] | None = None,
            timeout: int | None = None,
            operation_id: OperationId,
        ) -> ToolResult:
            self.pytest_calls.append({"args": args, "env": env, "cwd": cwd})
            if env and "COVERAGE_FILE" in env:
                Path(env["COVERAGE_FILE"]).unlink(missing_ok=True)
            return create_success_result(operation_id, stdout="pytest")

    runner = NoCoverageRunner()
    tools = testing_module.TestingTools(config, tmp_path, runner)

    coverage_file = tools._coverage_file()
    if coverage_file.exists():
        coverage_file.unlink()

    result, notes, _ = tools._ensure_coverage_data(
        args=[],
        learning_mode=False,
        verbosity_level=0,
        verbose=True,
        operation_id=OperationId(namespace="tools", category="test", command="ensure"),
        executed_commands=[],
    )

    assert isinstance(result, ToolResult)
    assert result.success is False
    assert "Coverage data not produced automatically" in result.stderr
    assert any("Coverage-test generated no data" in note for note in notes)


def test_coverage_report_verbose_lists_artifacts(
    config: ToolsConfig, tmp_path: Path
) -> None:
    """Include coverage artifacts when verbose output is requested."""
    runner = MetricsRunner()
    tools = testing_module.TestingTools(config, tmp_path, runner)

    coverage_dir = tmp_path / ".cache" / "coverage"
    coverage_dir.mkdir(parents=True, exist_ok=True)
    (coverage_dir / "coverage.sqlite").write_bytes(b"data")

    _create_sample_source_file(tmp_path)
    fingerprint = tools._compute_coverage_fingerprint()
    _write_manifest(tmp_path, fingerprint)

    # Pre-create artifacts that should appear in verbose output
    (coverage_dir / "coverage.json").write_text("{}", encoding="utf-8")
    (coverage_dir / "coverage.xml").write_text("<xml />", encoding="utf-8")
    htmlcov = coverage_dir / "htmlcov"
    htmlcov.mkdir(exist_ok=True)
    (htmlcov / "index.html").write_text("<html></html>", encoding="utf-8")

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

    result = tools.coverage_report([], verbose=True)

    assert result.success is True
    assert "Coverage artifacts:" in result.stdout
    assert "coverage.xml" in result.stdout
    assert "htmlcov" in result.stdout


def test_coverage_report_regenerates_after_missing_source(
    config: ToolsConfig, tmp_path: Path
) -> None:
    """Regenerate coverage when initial report fails due to missing source."""

    class MissingSourceRunner(RecordingRunner):
        """Runner that first fails coverage report with missing source before succeeding."""

        def __init__(self) -> None:
            super().__init__()
            self._attempts: dict[str, int] = {}

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
            key = " ".join(args)
            current = self._attempts.get(key, 0)
            self._attempts[key] = current + 1

            if args[:2] == ["coverage", "report"] and current == 0:
                return create_failure_result(operation_id, stderr="No source for code")

            return super().run_uv_command(
                args,
                cwd=cwd,
                env=env,
                timeout=timeout,
                operation_id=operation_id,
                python=python,
                no_project=no_project,
            )

    runner = MissingSourceRunner()
    tools = testing_module.TestingTools(config, tmp_path, runner)

    _create_sample_source_file(tmp_path)
    _write_coverage_file(tmp_path)
    fingerprint = tools._compute_coverage_fingerprint()
    _write_manifest(tmp_path, fingerprint)

    result = tools.coverage_report([])

    assert result.success is True
    # First attempt should fail, triggering internal regeneration before retrying
    report_attempts = runner._attempts.get("coverage report -m", 0)
    assert report_attempts >= 1


def test_coverage_report_handles_missing_json_data(
    config: ToolsConfig, tmp_path: Path
) -> None:
    """When coverage JSON is missing totals, the command should raise ToolExecutionError."""

    class MissingTotalsRunner(RecordingRunner):
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
                out_path.write_text(json.dumps({"files": {}}), encoding="utf-8")
            return result

    runner = MissingTotalsRunner()
    tools = testing_module.TestingTools(config, tmp_path, runner)

    (tmp_path / ".cache" / "coverage").mkdir(parents=True, exist_ok=True)

    with pytest.raises(ToolExecutionError) as excinfo:
        tools.coverage_report([])

    assert "Failed to parse coverage JSON" in str(excinfo.value)


def test_coverage_report_failure_returns_first_error(
    config: ToolsConfig, tmp_path: Path
) -> None:
    runner = ReportFailureRunner()
    tools = testing_module.TestingTools(config, tmp_path, runner)

    (tmp_path / ".cache" / "coverage").mkdir(parents=True, exist_ok=True)
    _write_coverage_file(tmp_path)
    _create_sample_source_file(tmp_path)
    fingerprint = tools._compute_coverage_fingerprint()
    _write_manifest(tmp_path, fingerprint)
    coverage_dir = tmp_path / ".cache" / "coverage"
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

    result = tools.coverage_report([])

    assert result.success is False
    assert "terminal report failed" in result.stderr


def test_coverage_learning_mode_combines_commands(
    config: ToolsConfig, tmp_path: Path
) -> None:
    runner = MetricsRunner()
    tools = testing_module.TestingTools(config, tmp_path, runner)

    _write_coverage_file(tmp_path)
    _create_sample_source_file(tmp_path)
    fingerprint = tools._compute_coverage_fingerprint()
    _write_manifest(tmp_path, fingerprint)

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


class RecordingRunner:
    """Baseline fake runner collecting pytest/uv invocations."""

    def __init__(self) -> None:
        self.pytest_calls: list[dict[str, object]] = []
        self.uv_calls: list[dict[str, object]] = []

    def run_subprocess(
        self,
        command: list[str],
        *,
        cwd: Path | None = None,
        env: dict[str, str] | None = None,
        timeout: int | None = None,
        operation_id: OperationId,
        capture_output: bool = True,
    ) -> ToolResult:
        return create_success_result(operation_id, stdout="subprocess")

    def run_pytest_command(
        self,
        args: list[str],
        *,
        cwd: Path | None = None,
        env: dict[str, str] | None = None,
        timeout: int | None = None,
        operation_id: OperationId,
    ) -> ToolResult:
        self.pytest_calls.append({"args": args, "env": env, "cwd": cwd})
        coverage_path = Path(env["COVERAGE_FILE"])
        coverage_path.parent.mkdir(parents=True, exist_ok=True)
        coverage_path.write_bytes(b"data")
        return create_success_result(operation_id, stdout="pytest")

    def run_uv_command(
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
        if args[:2] == ["coverage", "json"]:
            out_path = Path(args[args.index("-o") + 1])
            out_path.parent.mkdir(parents=True, exist_ok=True)
            out_path.write_text(
                json.dumps(
                    {
                        "totals": {
                            "num_statements": 100,
                            "covered_lines": 100,
                            "num_branches": 10,
                            "covered_branches": 10,
                        },
                        "files": {},
                    }
                )
            )
        if args[:2] == ["coverage", "combine"]:
            coverage_path = Path(env["COVERAGE_FILE"])
            coverage_path.parent.mkdir(parents=True, exist_ok=True)
            coverage_path.write_bytes(b"combined")
        return create_success_result(operation_id, stdout="uv")


class MetricsRunner(RecordingRunner):
    def __init__(self) -> None:
        super().__init__()
        self._payload = {
            "totals": {
                "num_statements": 10,
                "covered_lines": 9,
                "num_branches": 4,
                "covered_branches": 3,
            },
            "files": {
                "src/ml_playground/tools/a.py": {
                    "summary": {
                        "percent_covered_display": "90.00",
                        "num_branches": 2,
                        "covered_branches": 1,
                    }
                },
                "src/ml_playground/tools/nested/b.py": {
                    "summary": {
                        "percent_covered": 75.0,
                        "num_branches": 2,
                        "covered_branches": 2,
                    }
                },
            },
        }

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
            out_path.write_text(json.dumps(self._payload))
        return result


class MutationExecRunner(RecordingRunner):
    def __init__(self) -> None:
        super().__init__()
        self.commands: list[list[str]] = []

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
        self.commands.append(args)
        return create_success_result(operation_id, stdout="cosmic-ray exec")


class FailingJsonRunner(RecordingRunner):
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
        if args[:2] == ["coverage", "json"]:
            return create_failure_result(operation_id, stderr="json failed")
        return create_success_result(operation_id, stdout="ok")


class CombineFailureRunner(RecordingRunner):
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
        if args[:2] == ["coverage", "combine"]:
            return create_failure_result(operation_id, stderr="combine failed")
        return create_success_result(operation_id, stdout="ok")


class InitFailureRunner(RecordingRunner):
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
        return create_failure_result(operation_id, stderr="init failed")


class ReportFailureRunner(RecordingRunner):
    """Runner that fails during coverage report generation."""

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
            return create_failure_result(operation_id, stderr="terminal report failed")
        return create_success_result(operation_id, stdout="ok")


class CoverageTestFailureRunner(RecordingRunner):
    """Runner that fails when invoking coverage-test."""

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
        if args[:2] == ["coverage", "run"]:
            return create_failure_result(operation_id, stderr="coverage run failed")
        return create_success_result(operation_id, stdout="ok")


class PytestFailureRunner(RecordingRunner):
    def run_pytest_command(  # type: ignore[override]
        self,
        args: list[str],
        *,
        cwd: Path | None = None,
        env: dict[str, str] | None = None,
        timeout: int | None = None,
        operation_id: OperationId,
    ) -> ToolResult:
        self.pytest_calls.append({"args": args, "env": env, "cwd": cwd})
        return create_failure_result(operation_id, stderr="pytest failed")


class LowCoverageRunner(RecordingRunner):
    """Runner that emits low coverage JSON with undercovered tree."""

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
                            "num_statements": 100,
                            "covered_lines": 80,
                            "num_branches": 20,
                            "covered_branches": 14,
                        },
                        "files": {
                            "src/ml_playground/tools/categories/alpha.py": {
                                "summary": {
                                    "percent_covered_display": "82.00",
                                    "num_branches": 4,
                                    "covered_branches": 3,
                                }
                            },
                            "src/ml_playground/tools/categories/beta.py": {
                                "summary": {
                                    "percent_covered_display": "70.00",
                                    "num_branches": 6,
                                    "covered_branches": 3,
                                }
                            },
                        },
                    }
                )
            )
        return result


def _extract_tree(lines: list[str]) -> list[str]:
    if "Files below 100% coverage:" not in lines:
        return []
    start = lines.index("Files below 100% coverage:") + 1
    end = start
    while end < len(lines) and lines[end]:
        end += 1
    return lines[start:end]


def test_coverage_threshold_fallback_runs_pytest_when_no_data(
    config: ToolsConfig, tmp_path: Path
) -> None:
    class NoDataRunner(RecordingRunner):
        def run_uv_command(
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
            if args[:2] == ["coverage", "json"]:
                json_path = Path(args[args.index("-o") + 1])
                json_path.parent.mkdir(parents=True, exist_ok=True)
                json_path.write_text(json.dumps({"totals": {}}))
            return create_success_result(operation_id, stdout="coverage run")

    runner = NoDataRunner()
    tools = testing_module.TestingTools(config, tmp_path, runner)

    coverage_file = tmp_path / ".cache" / "coverage" / "coverage.sqlite"
    if coverage_file.exists():
        coverage_file.unlink()

    result = tools.coverage_threshold([], verbose=True)

    assert result.success is True
    assert runner.pytest_calls
    stdout_lines = result.stdout.splitlines()
    assert stdout_lines[0].startswith("Executed: coverage json -o ")
    assert "Executed: uv run tools test coverage-test" in stdout_lines
    assert any("Coverage-test generated no data" in line for line in stdout_lines)
    assert any(line.startswith("Executed: pytest ") for line in stdout_lines)
    assert _extract_tree(stdout_lines) == []


def test_coverage_threshold_fails_when_totals_low(
    config: ToolsConfig, tmp_path: Path
) -> None:
    runner = LowCoverageRunner()
    tools = testing_module.TestingTools(config, tmp_path, runner)

    coverage_dir = tmp_path / ".cache" / "coverage"
    coverage_dir.mkdir(parents=True, exist_ok=True)
    (coverage_dir / "coverage.sqlite").write_bytes(b"data")

    result = tools.coverage_threshold(
        [],
        line_threshold=95.0,
        branch_threshold=90.0,
        verbose=True,
    )

    assert result.success is False
    stdout_lines = result.stdout.splitlines()
    assert stdout_lines[0].startswith("Executed: coverage json -o ")
    tree_lines = _extract_tree(stdout_lines)
    assert tree_lines[:2] == ["└── src/", "    └── ml_playground/"]
    assert any("alpha.py: line = 82.00%" in line for line in tree_lines)
    assert any("beta.py: line = 70.00%" in line for line in tree_lines)
    assert "❌ FAILURE" in result.stderr


class TestCoverageHelpers:
    def test_clean_pytest_output_filters_progress(
        self, testing_tools: testing_module.TestingTools
    ) -> None:
        raw_output = "bringing up nodes\n.... [ 25%]\nPASSED in 0.01s\n"
        cleaned = testing_tools._clean_pytest_output(raw_output)

        assert "bringing up nodes" not in cleaned
        assert "[ 25%]" not in cleaned
        assert cleaned == "PASSED in 0.01s"

    def test_collect_coverage_metrics_success(
        self, config: ToolsConfig, tmp_path: Path
    ) -> None:
        runner = MetricsRunner()
        tools = testing_module.TestingTools(config, tmp_path, runner)
        coverage_file = tools._coverage_file()
        env = {"COVERAGE_FILE": str(coverage_file)}
        op_id = OperationId(namespace="tools", category="test", command="metrics")

        failure, lines = tools._collect_coverage_metrics(
            env, op_id, executed_commands=[]
        )

        assert failure is None
        assert lines[0].startswith("Coverage totals:")

        coverage_json = json.loads(
            (coverage_file.parent / "coverage.json").read_text(encoding="utf-8")
        )
        undercovered = tools._collect_undercovered_files(coverage_json)
        paths = {path for path, *_ in undercovered}
        assert "src/ml_playground/tools/a.py" in paths
        assert "src/ml_playground/tools/nested/b.py" in paths

        tree = tools._format_undercovered_tree(undercovered)
        assert tree[0].startswith("└── src/")

    def test_collect_coverage_metrics_handles_missing_branch_totals(
        self, config: ToolsConfig, tmp_path: Path
    ) -> None:
        class BranchlessMetricsRunner(RecordingRunner):
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
                                    "num_statements": 10,
                                    "covered_lines": 10,
                                    "num_branches": 0,
                                    "covered_branches": 0,
                                },
                                "files": {},
                            }
                        ),
                        encoding="utf-8",
                    )
                return result

        runner = BranchlessMetricsRunner()
        tools = testing_module.TestingTools(config, tmp_path, runner)
        coverage_file = tools._coverage_file()
        coverage_file.parent.mkdir(parents=True, exist_ok=True)
        env = {"COVERAGE_FILE": str(coverage_file)}

        failure, lines = tools._collect_coverage_metrics(
            env,
            OperationId(namespace="tools", category="test", command="metrics"),
            executed_commands=[],
        )

        assert failure is None
        assert any("Branch totals: not available" in line for line in lines)

    def test_collect_coverage_metrics_failure(
        self, config: ToolsConfig, tmp_path: Path
    ) -> None:
        runner = FailingJsonRunner()
        tools = testing_module.TestingTools(config, tmp_path, runner)
        coverage_file = tools._coverage_file()
        env = {"COVERAGE_FILE": str(coverage_file)}
        op_id = OperationId(namespace="tools", category="test", command="metrics")

        failure, _ = tools._collect_coverage_metrics(env, op_id)

        assert isinstance(failure, ToolResult)
        assert failure.success is False

    def test_format_helpers(self, testing_tools: testing_module.TestingTools) -> None:
        success_line = testing_tools._format_coverage_status(
            metric="line", percentage=100.0, threshold=90.0, passed=True
        )
        failure_line = testing_tools._format_coverage_status(
            metric="branch", percentage=65.0, threshold=70.0, passed=False
        )
        assert "✅" in success_line
        assert "❌" in failure_line

        invocation = testing_tools._format_tool_invocation("unit", ["--verbose"])
        assert invocation == "Executed: uv run tools test unit --verbose"

        command = testing_tools._format_command(["pytest", "tests/unit"])
        assert command == "Executed: pytest tests/unit"

    def test_collect_undercovered_files_parses_branch_percentage(
        self, testing_tools: testing_module.TestingTools
    ) -> None:
        coverage_data = {
            "files": {
                "pkg/file.py": {
                    "summary": {
                        "percent_covered_display": "88.00",
                        "num_branches": 5,
                        "covered_branches": 4,
                    }
                }
            }
        }

        entries = testing_tools._collect_undercovered_files(coverage_data)
        assert entries == [("pkg/file.py", 88.0, 80.0)]

    def test_coverage_env_creates_directories(
        self, testing_tools: testing_module.TestingTools
    ) -> None:
        coverage_file = testing_tools._coverage_file()
        env = testing_tools._coverage_env()

        assert Path(env["COVERAGE_FILE"]) == coverage_file
        assert (coverage_file.parent).exists()

    def test_read_coverage_thresholds_from_config(
        self, config: ToolsConfig, tmp_path: Path
    ) -> None:
        pyproject = tmp_path / "pyproject.toml"
        pyproject.write_text(
            """
            [tool.ml_playground.coverage.thresholds]
            line_threshold = 95.0
            branch_threshold = 80.0
            """
        )

        tools = testing_module.TestingTools(config, tmp_path, RecordingRunner())
        thresholds = tools._read_coverage_thresholds_from_config()

        assert thresholds == {"line_threshold": 95.0, "branch_threshold": 80.0}

    def test_read_coverage_thresholds_missing_file(
        self, config: ToolsConfig, tmp_path: Path
    ) -> None:
        tools = testing_module.TestingTools(config, tmp_path, RecordingRunner())
        assert tools._read_coverage_thresholds_from_config() == {}

    def test_ensure_coverage_data_returns_when_cache_present(
        self, config: ToolsConfig, tmp_path: Path
    ) -> None:
        tools = testing_module.TestingTools(config, tmp_path, RecordingRunner())
        coverage_file = tools._coverage_file()
        coverage_file.parent.mkdir(parents=True, exist_ok=True)
        coverage_file.write_bytes(b"data")

        result, notes, env = tools._ensure_coverage_data(
            args=[],
            learning_mode=False,
            verbosity_level=0,
            verbose=False,
            operation_id=OperationId(
                namespace="tools", category="test", command="ensure"
            ),
        )

        assert result is None
        assert any("Automatically ran coverage-test" in note for note in notes)
        assert env["COVERAGE_FILE"] == str(coverage_file)

    def test_ensure_coverage_data_fails_when_combine_fails(
        self, config: ToolsConfig, tmp_path: Path
    ) -> None:
        runner = CombineFailureRunner()
        tools = testing_module.TestingTools(config, tmp_path, runner)
        coverage_dir = tmp_path / ".cache" / "coverage"
        coverage_dir.mkdir(parents=True, exist_ok=True)
        (coverage_dir / "coverage.sqlite.fragment").write_text("fragment")

        result, _, _ = tools._ensure_coverage_data(
            args=[],
            learning_mode=False,
            verbosity_level=0,
            verbose=False,
            operation_id=OperationId(
                namespace="tools", category="test", command="combine"
            ),
        )

        assert isinstance(result, ToolResult)
        assert result.success is False

    def test_run_coverage_test_for_data_propagates_failures(
        self, config: ToolsConfig, tmp_path: Path
    ) -> None:
        class CoverageFailureTools(testing_module.TestingTools):
            def coverage_test(  # type: ignore[override]
                self,
                args: list[str],
                *,
                learning_mode: bool = False,
                verbosity_level: int = 1,
            ) -> ToolResult:
                return create_failure_result(
                    OperationId(
                        namespace="tools", category="test", command="coverage-test"
                    ),
                    stderr="coverage failed",
                )

        tools = CoverageFailureTools(config, tmp_path, RecordingRunner())
        result, notes = tools._run_coverage_test_for_data(
            args=[],
            verbosity_level=0,
            verbose=False,
            operation_id=OperationId(
                namespace="tools", category="test", command="coverage-test"
            ),
        )

        assert isinstance(result, ToolResult)
        assert result.success is False

    def test_generate_coverage_via_pytest_failure(
        self, config: ToolsConfig, tmp_path: Path
    ) -> None:
        runner = PytestFailureRunner()
        tools = testing_module.TestingTools(config, tmp_path, runner)
        result, notes = tools._generate_coverage_via_pytest(
            args=[],
            verbose=True,
            operation_id=OperationId(
                namespace="tools", category="test", command="pytest"
            ),
        )

        assert isinstance(result, ToolResult)
        assert result.success is False
        assert notes == []


def test_ensure_coverage_data_combines_fragments(
    config: ToolsConfig, tmp_path: Path
) -> None:
    runner = RecordingRunner()
    tools = testing_module.TestingTools(config, tmp_path, runner)
    coverage_dir = tmp_path / ".cache" / "coverage"
    coverage_dir.mkdir(parents=True, exist_ok=True)
    fragment = coverage_dir / "coverage.sqlite.fragment"
    fragment.write_text("old")

    result, notes, env = tools._ensure_coverage_data(
        args=[],
        learning_mode=False,
        verbosity_level=0,
        verbose=True,
        operation_id=OperationId(namespace="tools", category="test", command="combine"),
    )

    assert result is None
    assert any(call["args"][:2] == ["coverage", "combine"] for call in runner.uv_calls)
    manifest_path = tmp_path / ".cache" / "coverage" / "coverage_manifest.json"
    assert manifest_path.exists()


class TestMutationCommands:
    """Mutation tooling helpers should operate without monkeypatching."""

    def test_mutation_reset_removes_session_file(
        self,
        config: ToolsConfig,
        tmp_path: Path,
    ) -> None:
        """`mutation_reset` should delete an existing Cosmic Ray session file."""
        runner = RecordingRunner()
        tools = testing_module.TestingTools(config, tmp_path, runner)

        session_file = tmp_path / ".cache" / "cosmic-ray" / "session.sqlite"
        session_file.parent.mkdir(parents=True, exist_ok=True)
        session_file.write_bytes(b"session")

        with _temporary_cwd(tmp_path):
            result = tools.mutation_reset([])

        assert result.success is True
        assert "Removed Cosmic Ray session" in result.stdout
        assert not session_file.exists()

    def test_mutation_reset_handles_missing_file(
        self,
        config: ToolsConfig,
        tmp_path: Path,
    ) -> None:
        """`mutation_reset` should report when no session file exists."""
        runner = RecordingRunner()
        tools = testing_module.TestingTools(config, tmp_path, runner)

        with _temporary_cwd(tmp_path):
            result = tools.mutation_reset([])

        assert result.success is True
        assert "does not exist" in result.stdout

    def test_mutation_summary_reports_configuration(
        self,
        config: ToolsConfig,
        tmp_path: Path,
    ) -> None:
        """`mutation_summary` should load configuration and list modules."""
        runner = RecordingRunner()
        tools = testing_module.TestingTools(config, tmp_path, runner)

        (tmp_path / "pyproject.toml").write_text(
            "[tool.cosmic-ray]\n", encoding="utf-8"
        )

        config_module = ModuleType("cosmic_ray.config")

        def fake_load_config(path: str) -> dict[str, object]:
            assert path == "pyproject.toml"
            return {
                "session": {"path": ".cache/cosmic-ray/custom.sqlite"},
                "test-runner": {"command": "pytest -q"},
                "modules": {"paths": ["ml_playground.core", "ml_playground.tools"]},
            }

        config_module.load_config = fake_load_config  # type: ignore[attr-defined]

        modules_module = ModuleType("cosmic_ray.modules")

        def fake_find_modules(cfg: object) -> list[str]:
            return ["ml_playground.core", "ml_playground.tools"]

        modules_module.find_modules = fake_find_modules  # type: ignore[attr-defined]

        originals = _install_modules(
            {
                "cosmic_ray": ModuleType("cosmic_ray"),
                "cosmic_ray.config": config_module,
                "cosmic_ray.modules": modules_module,
            }
        )

        try:
            with _temporary_cwd(tmp_path):
                result = tools.mutation_summary([])
        finally:
            _restore_modules(originals)

        assert result.success is True
        assert "[mutation] config: pyproject.toml" in result.stdout
        assert "modules to mutate: 2" in result.stdout

    def test_mutation_summary_import_error(
        self,
        config: ToolsConfig,
        tmp_path: Path,
    ) -> None:
        """`mutation_summary` should fail gracefully when cosmic_ray is missing."""
        runner = RecordingRunner()
        tools = testing_module.TestingTools(config, tmp_path, runner)

        (tmp_path / "pyproject.toml").write_text(
            "[tool.cosmic-ray]\n", encoding="utf-8"
        )

        for name in list(sys.modules.keys()):
            if name.startswith("cosmic_ray"):
                sys.modules.pop(name)

        with _temporary_cwd(tmp_path):
            result = tools.mutation_summary([])

        assert result.success is False
        assert (
            "cosmic_ray must be installed" in result.stderr
            or "Failed to generate mutation summary" in result.stderr
        )

    def test_mutation_init_returns_success_on_failure(
        self,
        config: ToolsConfig,
        tmp_path: Path,
    ) -> None:
        """`mutation_init` should convert init failure into success message."""
        tools = testing_module.TestingTools(config, tmp_path, InitFailureRunner())

        with _temporary_cwd(tmp_path):
            result = tools.mutation_init([])

        assert result.success is True
        assert result.exit_code == 0
        assert "Cosmic Ray init skipped" in result.stdout

    def test_mutation_exec_requires_session_file(
        self,
        config: ToolsConfig,
        tmp_path: Path,
    ) -> None:
        """`mutation_exec` should run cosmic-ray exec when session exists."""
        runner = MutationExecRunner()
        tools = testing_module.TestingTools(config, tmp_path, runner)

        session_file = tmp_path / ".cache" / "cosmic-ray" / "session.sqlite"
        session_file.parent.mkdir(parents=True, exist_ok=True)
        session_file.write_text("session", encoding="utf-8")

        with _temporary_cwd(tmp_path):
            result = tools.mutation_exec([])

        assert result.success is True
        assert runner.commands
        exec_cmd = runner.commands[-1]
        assert exec_cmd[:3] == ["cosmic-ray", "exec", "pyproject.toml"]
        assert exec_cmd[3].endswith(str(session_file.relative_to(tmp_path)))

    def test_mutation_exec_missing_session_raises(
        self,
        config: ToolsConfig,
        tmp_path: Path,
    ) -> None:
        """`mutation_exec` should raise when the session DB is absent."""
        runner = MutationExecRunner()
        tools = testing_module.TestingTools(config, tmp_path, runner)

        with _temporary_cwd(tmp_path):
            with pytest.raises(ToolExecutionError) as exc_info:
                tools.mutation_exec([])

        assert "Cosmic Ray session file not found" in str(exc_info.value)
