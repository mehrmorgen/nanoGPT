"""Unit tests for CI tools category.

Tests the CITools class functionality including quality gates and coverage operations
using fakes instead of mocks. Mutation testing moved to TestingTools.
"""

import pytest
from pathlib import Path
from typing import Any, List

from ml_playground.tools.ci.ci import CITools
from ml_playground.tools.core.interfaces import ToolResult
from ml_playground.tools.core.config import ToolsConfig
from ml_playground.tools.core.interfaces import OperationId
from ml_playground.tools.core.errors import ToolExecutionError
from tests.unit.tools.fakes import (
    FakeSubprocessRunner,
    create_success_result,
    create_failure_result,
)


class TestCIToolsInit:
    """Test CITools initialization."""

    def test_init(self, tmp_path: Path):
        """Test CITools initializes correctly."""
        config = ToolsConfig()
        fake_runner = FakeSubprocessRunner()
        ci_tools = CITools(config, tmp_path, subprocess_runner=fake_runner)

        assert ci_tools.config == config
        assert ci_tools.root_path == tmp_path
        assert ci_tools.category == "ci"
        assert ci_tools._subprocess_runner == fake_runner


@pytest.fixture
def ci_tools(tmp_path: Path) -> CITools:
    """Create CITools instance with fake subprocess runner."""
    config = ToolsConfig()
    fake_runner = FakeSubprocessRunner()
    return CITools(config, tmp_path, subprocess_runner=fake_runner)


@pytest.fixture
def fake_runner(ci_tools: CITools) -> FakeSubprocessRunner:
    """Get the fake subprocess runner from CI tools."""
    return ci_tools._subprocess_runner


class TestQualityGate:
    """Test quality gate functionality."""

    def test_quality_gate_success(
        self, ci_tools: CITools, fake_runner: FakeSubprocessRunner
    ):
        """Test successful quality gate execution."""
        # Configure fake runner to return success for pre-commit
        operation_id = OperationId(
            namespace="tools", category="ci", command="quality-gate"
        )
        success_result = create_success_result(operation_id, "All checks passed")
        fake_runner.set_results([success_result])

        result = ci_tools.quality_gate([])

        assert result.success is True
        assert result.exit_code == 0
        # Only one call (pre-commit) should be made now
        assert len(fake_runner.calls) == 1
        assert "Quality Gate Summary:" in (result.stdout or "")
        assert "- pre-commit: PASS" in (result.stdout or "")

    def test_quality_gate_aggregates_stderr(
        self, ci_tools: CITools, fake_runner: FakeSubprocessRunner
    ) -> None:
        operation_id = OperationId(
            namespace="tools", category="ci", command="quality-gate"
        )
        pre_commit = create_success_result(operation_id, "ok")
        pre_commit.stderr = "pre-commit warning"
        fake_runner.set_results([pre_commit])

        result = ci_tools.quality_gate([])

        assert result.success is True
        # Stderr should include pre-commit stderr directly
        assert "pre-commit warning" in (result.stderr or "")

    def test_quality_gate_precommit_failure(
        self, ci_tools: CITools, fake_runner: FakeSubprocessRunner
    ):
        """Test quality gate with pre-commit failure."""
        # Configure fake runner to return failure for pre-commit
        operation_id = OperationId(
            namespace="tools", category="ci", command="quality-gate"
        )
        failure_result = create_failure_result(operation_id, 1, "", "Pre-commit failed")
        fake_runner.set_results([failure_result])

        result = ci_tools.quality_gate([])

        assert result.success is False
        assert result.exit_code == 1
        assert "Pre-commit failed" in result.stderr
        # Should stop after first failure
        assert len(fake_runner.calls) == 1

    def test_quality_gate_success_summary_contains(
        self, ci_tools: CITools, fake_runner: FakeSubprocessRunner
    ) -> None:
        operation_id = OperationId(
            namespace="tools", category="ci", command="quality-gate"
        )
        precommit = create_success_result(operation_id, "precommit ok")
        fake_runner.set_results([precommit])

        result = ci_tools.quality_gate(["--verbose"])

        assert result.success is True
        assert result.exit_code == 0
        assert "Quality Gate Summary:" in (result.stdout or "")
        assert "- pre-commit: PASS" in (result.stdout or "")
        # Accept verbose flag position differences (e.g., 'pre-commit run -v --config ...')
        assert "pre-commit run" in (result.stdout or "")
        assert "--config" in (result.stdout or "")
        assert len(fake_runner.calls) == 1

    def test_precommit_stdout_is_appended_to_summary(
        self, ci_tools: CITools, fake_runner: FakeSubprocessRunner
    ) -> None:
        operation_id = OperationId(
            namespace="tools", category="ci", command="quality-gate"
        )
        fake_runner.set_results([create_success_result(operation_id, "pre ok")])

        result = ci_tools.quality_gate([])

        assert result.success is True
        assert result.exit_code == 0
        assert "Pre-commit output:\npre ok" in (result.stdout or "")
        assert len(fake_runner.calls) == 1

    def test_quality_gate_with_args(
        self, ci_tools: CITools, fake_runner: FakeSubprocessRunner
    ):
        """Test quality gate with additional arguments."""
        operation_id = OperationId(
            namespace="tools", category="ci", command="quality-gate"
        )
        success_result = create_success_result(operation_id, "Success with args")
        fake_runner.set_results([success_result])

        result = ci_tools.quality_gate(["--verbose"])

        assert result.success is True
        # Only one pre-commit call should be made
        assert len(fake_runner.calls) == 1


class TestQualityFast:
    """Test fast quality checks functionality."""

    def test_quality_fast_success(
        self, ci_tools: CITools, fake_runner: FakeSubprocessRunner
    ):
        """Test successful fast quality checks."""
        operation_id = OperationId(
            namespace="tools", category="ci", command="quality-fast"
        )
        success_result = create_success_result(operation_id, "Fast checks passed")
        # quality_fast runs 3 hooks: ruff, ruff-format, mdformat
        fake_runner.set_results([success_result, success_result, success_result])

        result = ci_tools.quality_fast([])

        assert result.success is True
        assert result.exit_code == 0
        assert len(fake_runner.calls) == 3  # One call per hook

    def test_quality_fast_hook_failure(
        self, ci_tools: CITools, fake_runner: FakeSubprocessRunner
    ):
        """Test fast quality checks with hook failure."""
        operation_id = OperationId(
            namespace="tools", category="ci", command="quality-fast"
        )
        failure_result = create_failure_result(operation_id, 1, "", "Hook failed")
        fake_runner.set_results([failure_result])

        result = ci_tools.quality_fast([])

        assert result.success is False
        assert result.exit_code == 1
        assert "Hook failed" in result.stderr


class TestQualityExt:
    """Test extended quality validation functionality."""

    def test_quality_ext_success(self, ci_tools: CITools):
        """Test successful extended quality validation (mutation testing moved to testing tools)."""
        # Create a fake runner for this specific test
        fake_runner = FakeSubprocessRunner()
        ci_tools._subprocess_runner = fake_runner

        # Mock the quality_gate method to return success
        operation_id = OperationId(
            namespace="tools", category="ci", command="quality-ext"
        )
        success_result = create_success_result(
            operation_id, "Extended validation passed"
        )

        # Set up the fake runner to return success for all subprocess calls
        fake_runner.set_results([success_result] * 10)  # Enough results for all calls

        # Mock the internal method calls by replacing them temporarily
        original_quality_gate = ci_tools.quality_gate

        def fake_quality_gate(args: List[str]) -> ToolResult:
            return success_result

        ci_tools.quality_gate = fake_quality_gate

        try:
            result = ci_tools.quality_ext([])
            assert result.success is True
            assert result.exit_code == 0
        finally:
            # Restore original methods
            ci_tools.quality_gate = original_quality_gate

    def test_quality_ext_quality_gate_failure(self, ci_tools: CITools):
        """Test extended quality validation with quality gate failure."""
        operation_id = OperationId(
            namespace="tools", category="ci", command="quality-gate"
        )
        failure_result = create_failure_result(
            operation_id, 1, "", "Quality gate failed"
        )

        # Mock quality_gate to return failure
        def fake_quality_gate(args: List[str]) -> ToolResult:
            return failure_result

        original_quality_gate = ci_tools.quality_gate
        ci_tools.quality_gate = fake_quality_gate

        try:
            result = ci_tools.quality_ext([])
            assert result.success is False
            assert result.exit_code == 1
            assert "Quality gate failed" in result.stderr
        finally:
            ci_tools.quality_gate = original_quality_gate


class TestCoverageBadgeAugmented:
    """Additional coverage badge scenarios."""

    def test_coverage_badge_generation_failure(
        self, ci_tools: CITools, fake_runner: FakeSubprocessRunner, tmp_path: Path
    ) -> None:
        ci_tools.cache_dir = tmp_path / ".cache"
        coverage_dir = ci_tools.cache_dir / "coverage"
        coverage_dir.mkdir(parents=True)
        json_path = coverage_dir / "coverage.json"

        # Write malformed JSON and ensure error is handled gracefully
        json_path.write_text("{not-json}")

        result = ci_tools.coverage_badge([])

        assert result.success is False
        assert "Failed to generate coverage badge" in result.stderr


class TestCoverageBadge:
    """Test coverage badge generation functionality."""

    def test_coverage_badge_with_existing_json(
        self, ci_tools: CITools, fake_runner: FakeSubprocessRunner, tmp_path: Path
    ):
        """Test coverage badge generation with existing coverage JSON."""
        # Create a fake coverage JSON file
        coverage_dir = tmp_path / ".cache" / "coverage"
        coverage_dir.mkdir(parents=True)
        json_path = coverage_dir / "coverage.json"
        json_path.write_text('{"totals": {"percent_covered": 85.5}}')

        # Set the cache_dir to use our tmp_path
        ci_tools.cache_dir = tmp_path / ".cache"

        result = ci_tools.coverage_badge([])

        assert result.success is True
        assert "85.5% coverage" in result.stdout
        # No subprocess calls since badge generation is now direct
        assert len(fake_runner.calls) == 0

    def test_coverage_badge_without_json(
        self, ci_tools: CITools, fake_runner: FakeSubprocessRunner, tmp_path: Path
    ):
        """Test coverage badge generation without existing coverage JSON."""
        operation_id = OperationId(
            namespace="tools", category="ci", command="coverage-badge"
        )
        success_result = create_success_result(operation_id, "Coverage generated")
        fake_runner.set_results([success_result])  # For coverage generation

        # Set the cache_dir to use our tmp_path
        ci_tools.cache_dir = tmp_path / ".cache"

        # Create the coverage JSON that would be generated by the coverage command
        coverage_dir = tmp_path / ".cache" / "coverage"
        coverage_dir.mkdir(parents=True)
        json_path = coverage_dir / "coverage.json"

        # Mock the coverage generation to create the JSON file
        def mock_run_uv_command(cmd, **kwargs):
            if "coverage" in cmd and "json" in cmd:
                json_path.write_text('{"totals": {"percent_covered": 75.0}}')
            return success_result

        ci_tools._subprocess_runner.run_uv_command = mock_run_uv_command

        result = ci_tools.coverage_badge([])

        assert result.success is True
        assert "75.0% coverage" in result.stdout
        # Should call coverage generation first
        assert len(fake_runner.calls) == 0  # Our mock doesn't use the fake_runner

    def test_coverage_badge_fails_when_generation_fails(
        self, ci_tools: CITools, fake_runner: FakeSubprocessRunner, tmp_path: Path
    ) -> None:
        operation_id = OperationId(
            namespace="tools", category="ci", command="coverage-badge"
        )
        failure = create_failure_result(operation_id, 1, stderr="coverage json failed")
        fake_runner.set_results([failure])
        ci_tools.cache_dir = tmp_path / ".cache"

        with pytest.raises(ToolExecutionError) as excinfo:
            ci_tools.coverage_badge([])

        assert "Failed to generate coverage JSON for badge creation" in str(
            excinfo.value
        )
        assert fake_runner.calls  # ensure command executed

    def test_coverage_badge_respects_configured_output_dir(
        self, tmp_path: Path
    ) -> None:
        config = ToolsConfig()
        config.ci.badge_output_dir = Path("artifacts/badges")
        fake_runner = FakeSubprocessRunner()
        ci_tools = CITools(config, tmp_path, subprocess_runner=fake_runner)

        coverage_dir = tmp_path / ".cache" / "coverage"
        coverage_dir.mkdir(parents=True)
        json_path = coverage_dir / "coverage.json"
        json_path.write_text('{"totals": {"percent_covered": 88.2}}')

        result = ci_tools.coverage_badge([])

        assert result.success is True
        expected_badge = (
            tmp_path / config.ci.badge_output_dir / "coverage.svg"
        ).resolve()
        assert expected_badge.exists()
        assert "88.2% coverage" in result.stdout


# Mutation testing moved to TestingTools


class TestQualityCILocal:
    """Test local CI execution functionality."""

    def test_quality_ci_local_success(self, ci_tools: CITools):
        """Test successful local CI execution."""
        # Create a fake subprocess runner that simulates successful subprocess.run
        fake_runner = FakeSubprocessRunner()
        ci_tools._subprocess_runner = fake_runner

        # Mock subprocess.run directly since quality_ci_local uses it
        import subprocess

        original_run = subprocess.run

        def fake_subprocess_run(*args: Any, **kwargs: Any) -> Any:
            # Create a mock result object
            class MockResult:
                def __init__(self) -> None:
                    self.returncode = 0
                    self.stdout = "CI passed"
                    self.stderr = ""

            return MockResult()

        subprocess.run = fake_subprocess_run

        try:
            result = ci_tools.quality_ci_local([])
            assert result.success is True
            assert result.exit_code == 0
        finally:
            subprocess.run = original_run

    def test_quality_ci_local_generic_failure(self, ci_tools: CITools):
        import subprocess
        from ml_playground.tools.core.errors import ToolExecutionError

        original_run = subprocess.run

        def fake_run(*args, **kwargs):
            raise OSError("act not installed")

        subprocess.run = fake_run

        try:
            with pytest.raises(ToolExecutionError) as exc_info:
                ci_tools.quality_ci_local([])
            assert "Failed to execute act command" in str(exc_info.value)
        finally:
            subprocess.run = original_run

    def test_quality_ci_local_failure(self, ci_tools: CITools):
        """Test local CI execution failure."""
        import subprocess

        original_run = subprocess.run

        def fake_subprocess_run(*args: Any, **kwargs: Any) -> Any:
            class MockResult:
                def __init__(self) -> None:
                    self.returncode = 1
                    self.stdout = ""
                    self.stderr = "CI failed"

            return MockResult()

        subprocess.run = fake_subprocess_run

        try:
            result = ci_tools.quality_ci_local([])
            assert result.success is False
            assert result.exit_code == 1
        finally:
            subprocess.run = original_run

    def test_quality_ci_local_with_cache_binding(self, ci_tools: CITools):
        """Test local CI execution with cache binding."""
        import subprocess

        original_run = subprocess.run

        def fake_subprocess_run(*args: Any, **kwargs: Any) -> Any:
            class MockResult:
                def __init__(self) -> None:
                    self.returncode = 0
                    self.stdout = "CI with cache binding"
                    self.stderr = ""

            return MockResult()

        subprocess.run = fake_subprocess_run

        try:
            result = ci_tools.quality_ci_local(["--cache-binding"])
            assert result.success is True
            assert result.exit_code == 0
        finally:
            subprocess.run = original_run

    def test_quality_ci_local_timeout(self, ci_tools: CITools):
        """Test local CI execution with timeout."""
        import subprocess
        from ml_playground.tools.core.errors import ToolExecutionError

        original_run = subprocess.run

        def fake_subprocess_run(*args: Any, **kwargs: Any) -> Any:
            raise subprocess.TimeoutExpired("act", 300)

        subprocess.run = fake_subprocess_run

        try:
            with pytest.raises(ToolExecutionError) as exc_info:
                ci_tools.quality_ci_local([])
            assert "timed out after 900 seconds" in str(exc_info.value)
        finally:
            subprocess.run = original_run
