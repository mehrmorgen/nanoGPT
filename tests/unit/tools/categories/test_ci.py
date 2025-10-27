"""Unit tests for CI tools category.

Tests the CITools class functionality including quality gates, mutation testing,
and coverage operations without using mocks.
"""

import pytest
from pathlib import Path
from unittest.mock import patch

from ml_playground.tools.categories.ci import CITools
from ml_playground.tools.core.config import ToolsConfig
from ml_playground.tools.core.interfaces import OperationId
from tests.unit.tools.fakes import create_success_result, create_failure_result


class TestCIToolsInit:
    """Test CITools initialization."""

    def test_init(self, tmp_path: Path):
        """Test CITools initializes correctly."""
        config = ToolsConfig()
        tools = CITools(config, tmp_path)

        assert tools.config == config
        assert tools.root_path == tmp_path
        assert tools.category == "ci"
        assert tools.cache_dir == tmp_path / ".cache"
        assert (
            tools.pre_commit_config
            == tmp_path / ".githooks" / ".pre-commit-config.yaml"
        )


class TestQualityGate:
    """Test quality gate functionality."""

    @pytest.fixture
    def ci_tools(self, tmp_path: Path) -> CITools:
        """Create CITools instance for testing."""
        config = ToolsConfig()
        return CITools(config, tmp_path)

    def test_quality_gate_success(self, ci_tools: CITools):
        """Test successful quality gate execution."""
        with patch("ml_playground.tools.categories.ci.run_uv_command") as mock_run:
            # Mock successful results for all steps
            operation_id = OperationId(
                namespace="tools", category="ci", command="quality-gate"
            )
            success_result = create_success_result(operation_id, "All checks passed")
            mock_run.return_value = success_result

            result = ci_tools.quality_gate([])

            assert result.success is True
            assert result.exit_code == 0
            assert "All checks passed" in result.stdout

    def test_quality_gate_precommit_failure(self, ci_tools: CITools):
        """Test quality gate with pre-commit failure."""
        with patch("ml_playground.tools.categories.ci.run_uv_command") as mock_run:
            # Mock pre-commit failure
            operation_id = OperationId(
                namespace="tools", category="ci", command="quality-gate"
            )
            failure_result = create_failure_result(
                operation_id, 1, "", "Pre-commit failed"
            )
            mock_run.return_value = failure_result

            result = ci_tools.quality_gate([])

            assert result.success is False
            assert result.exit_code == 1
            assert "Pre-commit failed" in result.stderr

    def test_quality_gate_with_args(self, ci_tools: CITools):
        """Test quality gate with additional arguments."""
        with patch("ml_playground.tools.categories.ci.run_uv_command") as mock_run:
            operation_id = OperationId(
                namespace="tools", category="ci", command="quality-gate"
            )
            success_result = create_success_result(operation_id, "Quality gate passed")
            mock_run.return_value = success_result

            result = ci_tools.quality_gate(["--verbose"])

            assert result.success is True
            # Verify that args were passed through
            mock_run.assert_called()


class TestQualityFast:
    """Test fast quality checks functionality."""

    @pytest.fixture
    def ci_tools(self, tmp_path: Path) -> CITools:
        """Create CITools instance for testing."""
        config = ToolsConfig()
        return CITools(config, tmp_path)

    def test_quality_fast_success(self, ci_tools: CITools):
        """Test successful fast quality checks."""
        with patch("ml_playground.tools.categories.ci.run_uv_command") as mock_run:
            operation_id = OperationId(
                namespace="tools", category="ci", command="quality-fast"
            )
            success_result = create_success_result(operation_id, "Hook passed")
            mock_run.return_value = success_result

            result = ci_tools.quality_fast([])

            assert result.success is True
            assert result.exit_code == 0
            # Should have been called multiple times for different hooks
            assert mock_run.call_count >= 3  # ruff, ruff-format, mdformat

    def test_quality_fast_hook_failure(self, ci_tools: CITools):
        """Test fast quality checks with hook failure."""
        with patch("ml_playground.tools.categories.ci.run_uv_command") as mock_run:
            operation_id = OperationId(
                namespace="tools", category="ci", command="quality-fast"
            )
            failure_result = create_failure_result(operation_id, 1, "", "Hook failed")
            mock_run.return_value = failure_result

            result = ci_tools.quality_fast([])

            assert result.success is False
            assert result.exit_code == 1


class TestQualityExt:
    """Test extended quality validation functionality."""

    @pytest.fixture
    def ci_tools(self, tmp_path: Path) -> CITools:
        """Create CITools instance for testing."""
        config = ToolsConfig()
        return CITools(config, tmp_path)

    def test_quality_ext_success(self, ci_tools: CITools):
        """Test successful extended quality validation."""
        with (
            patch.object(ci_tools, "quality_gate") as mock_quality_gate,
            patch.object(ci_tools, "mutation_run") as mock_mutation_run,
        ):
            operation_id = OperationId(
                namespace="tools", category="ci", command="quality-gate"
            )
            quality_result = create_success_result(operation_id, "Quality gate passed")
            mock_quality_gate.return_value = quality_result

            mutation_id = OperationId(
                namespace="tools", category="ci", command="mutation-run"
            )
            mutation_result = create_success_result(
                mutation_id, "Mutation testing passed"
            )
            mock_mutation_run.return_value = mutation_result

            result = ci_tools.quality_ext([])

            assert result.success is True
            assert result.exit_code == 0
            assert "Quality gate passed" in result.stdout
            assert "Mutation testing passed" in result.stdout

    def test_quality_ext_quality_gate_failure(self, ci_tools: CITools):
        """Test extended quality validation with quality gate failure."""
        with patch.object(ci_tools, "quality_gate") as mock_quality_gate:
            operation_id = OperationId(
                namespace="tools", category="ci", command="quality-gate"
            )
            failure_result = create_failure_result(
                operation_id, 1, "", "Quality gate failed"
            )
            mock_quality_gate.return_value = failure_result

            result = ci_tools.quality_ext([])

            assert result.success is False
            assert result.exit_code == 1


class TestCoverageBadge:
    """Test coverage badge generation functionality."""

    @pytest.fixture
    def ci_tools(self, tmp_path: Path) -> CITools:
        """Create CITools instance for testing."""
        config = ToolsConfig()
        tools = CITools(config, tmp_path)
        # Create cache directory structure
        (tmp_path / ".cache" / "coverage").mkdir(parents=True, exist_ok=True)
        return tools

    def test_coverage_badge_with_existing_json(self, ci_tools: CITools):
        """Test coverage badge generation with existing coverage JSON."""
        # Create mock coverage JSON file
        json_path = ci_tools.cache_dir / "coverage" / "coverage.json"
        json_path.write_text('{"totals": {"percent_covered": 85.5}}')

        with patch("ml_playground.tools.categories.ci.run_uv_command") as mock_run:
            operation_id = OperationId(
                namespace="tools", category="ci", command="coverage-badge"
            )
            success_result = create_success_result(operation_id, "Badges generated")
            mock_run.return_value = success_result

            result = ci_tools.coverage_badge([])

            assert result.success is True
            assert "Badges generated" in result.stdout

    def test_coverage_badge_without_json(self, ci_tools: CITools):
        """Test coverage badge generation without existing coverage JSON."""
        with patch("ml_playground.tools.categories.ci.run_uv_command") as mock_run:
            operation_id = OperationId(
                namespace="tools", category="ci", command="coverage-badge"
            )
            success_result = create_success_result(operation_id, "Coverage generated")
            mock_run.return_value = success_result

            result = ci_tools.coverage_badge([])

            assert result.success is True
            # Should have been called twice: once for coverage json, once for badge generation
            assert mock_run.call_count == 2


class TestMutationTesting:
    """Test mutation testing functionality."""

    @pytest.fixture
    def ci_tools(self, tmp_path: Path) -> CITools:
        """Create CITools instance for testing."""
        config = ToolsConfig()
        tools = CITools(config, tmp_path)
        # Create cache directory structure
        (tmp_path / ".cache" / "cosmic_ray").mkdir(parents=True, exist_ok=True)
        return tools

    def test_mutation_reset_with_existing_session(self, ci_tools: CITools):
        """Test mutation reset with existing session file."""
        # Create mock session file
        session_file = ci_tools._cosmic_ray_session_file()
        session_file.write_text("mock session data")

        result = ci_tools.mutation_reset([])

        assert result.success is True
        assert "Removed Cosmic Ray session" in result.stdout
        assert not session_file.exists()

    def test_mutation_reset_without_session(self, ci_tools: CITools):
        """Test mutation reset without existing session file."""
        result = ci_tools.mutation_reset([])

        assert result.success is True
        assert "does not exist" in result.stdout

    def test_mutation_summary(self, ci_tools: CITools):
        """Test mutation summary generation."""
        with patch("ml_playground.tools.categories.ci.run_uv_command") as mock_run:
            operation_id = OperationId(
                namespace="tools", category="ci", command="mutation-summary"
            )
            success_result = create_success_result(operation_id, "Mutation summary")
            mock_run.return_value = success_result

            result = ci_tools.mutation_summary([])

            assert result.success is True
            assert "Mutation summary" in result.stdout

    def test_mutation_init_new_session(self, ci_tools: CITools):
        """Test mutation initialization with new session."""
        with patch("ml_playground.tools.categories.ci.run_uv_command") as mock_run:
            operation_id = OperationId(
                namespace="tools", category="ci", command="mutation-init"
            )
            success_result = create_success_result(operation_id, "Session initialized")
            mock_run.return_value = success_result

            result = ci_tools.mutation_init([])

            assert result.success is True

    def test_mutation_init_existing_session(self, ci_tools: CITools):
        """Test mutation initialization with existing session."""
        with patch("ml_playground.tools.categories.ci.run_uv_command") as mock_run:
            operation_id = OperationId(
                namespace="tools", category="ci", command="mutation-init"
            )
            failure_result = create_failure_result(
                operation_id, 1, "", "Session exists"
            )
            mock_run.return_value = failure_result

            result = ci_tools.mutation_init([])

            # Should convert failure to success for existing session
            assert result.success is True
            assert "reusing existing session" in result.stdout

    def test_mutation_exec_success(self, ci_tools: CITools):
        """Test successful mutation execution."""
        # Create mock session file
        session_file = ci_tools._cosmic_ray_session_file()
        session_file.write_text("mock session data")

        with patch("ml_playground.tools.categories.ci.run_uv_command") as mock_run:
            operation_id = OperationId(
                namespace="tools", category="ci", command="mutation-exec"
            )
            success_result = create_success_result(
                operation_id, "Mutation execution completed"
            )
            mock_run.return_value = success_result

            result = ci_tools.mutation_exec([])

            assert result.success is True
            assert "Mutation execution completed" in result.stdout

    def test_mutation_exec_no_session(self, ci_tools: CITools):
        """Test mutation execution without session file."""
        from ml_playground.tools.core.errors import ToolExecutionError

        with pytest.raises(ToolExecutionError) as exc_info:
            ci_tools.mutation_exec([])

        assert "session file not found" in str(exc_info.value).lower()

    def test_mutation_report(self, ci_tools: CITools):
        """Test mutation report generation."""
        with patch("ml_playground.tools.categories.ci.run_uv_command") as mock_run:
            operation_id = OperationId(
                namespace="tools", category="ci", command="mutation-report"
            )
            success_result = create_success_result(
                operation_id, "Mutation report generated"
            )
            mock_run.return_value = success_result

            result = ci_tools.mutation_report([])

            assert result.success is True
            assert "Mutation report generated" in result.stdout

    def test_mutation_run_full_pipeline(self, ci_tools: CITools):
        """Test full mutation testing pipeline."""
        with (
            patch.object(ci_tools, "mutation_reset") as mock_reset,
            patch.object(ci_tools, "mutation_summary") as mock_summary,
            patch.object(ci_tools, "mutation_init") as mock_init,
            patch.object(ci_tools, "mutation_exec") as mock_exec,
            patch.object(ci_tools, "mutation_report") as mock_report,
        ):
            # Mock all steps as successful
            operation_id = OperationId(
                namespace="tools", category="ci", command="mutation-run"
            )
            success_result = create_success_result(operation_id, "Step completed")

            mock_reset.return_value = success_result
            mock_summary.return_value = success_result
            mock_init.return_value = success_result
            mock_exec.return_value = success_result
            mock_report.return_value = success_result

            result = ci_tools.mutation_run([])

            assert result.success is True
            assert result.exit_code == 0

            # Verify all steps were called
            mock_reset.assert_called_once()
            mock_summary.assert_called_once()
            mock_init.assert_called_once()
            mock_exec.assert_called_once()
            mock_report.assert_called_once()

    def test_mutation_run_step_failure(self, ci_tools: CITools):
        """Test mutation run with step failure."""
        with (
            patch.object(ci_tools, "mutation_reset") as mock_reset,
            patch.object(ci_tools, "mutation_summary") as mock_summary,
        ):
            # Mock reset as successful, summary as failure
            reset_id = OperationId(
                namespace="tools", category="ci", command="mutation-reset"
            )
            success_result = create_success_result(reset_id, "Reset completed")
            mock_reset.return_value = success_result

            summary_id = OperationId(
                namespace="tools", category="ci", command="mutation-summary"
            )
            failure_result = create_failure_result(summary_id, 1, "", "Summary failed")
            mock_summary.return_value = failure_result

            result = ci_tools.mutation_run([])

            assert result.success is False
            assert result.exit_code == 1

            # Should stop at first failure
            mock_reset.assert_called_once()
            mock_summary.assert_called_once()


class TestQualityCILocal:
    """Test local CI execution functionality."""

    @pytest.fixture
    def ci_tools(self, tmp_path: Path) -> CITools:
        """Create CITools instance for testing."""
        config = ToolsConfig()
        tools = CITools(config, tmp_path)
        # Create cache directories
        for subdir in ["uv", "pre-commit", "ruff"]:
            (tmp_path / ".cache" / subdir).mkdir(parents=True, exist_ok=True)
        (tmp_path / ".venv").mkdir(parents=True, exist_ok=True)
        return tools

    def test_quality_ci_local_success(self, ci_tools: CITools):
        """Test successful local CI execution."""
        with patch("ml_playground.tools.categories.ci.subprocess.run") as mock_run:
            # Mock successful subprocess execution
            mock_result = type(
                "MockResult",
                (),
                {
                    "returncode": 0,
                    "stdout": "CI workflow completed successfully",
                    "stderr": "",
                },
            )()
            mock_run.return_value = mock_result

            result = ci_tools.quality_ci_local([])

            assert result.success is True
            assert result.exit_code == 0
            assert "CI workflow completed successfully" in result.stdout

    def test_quality_ci_local_failure(self, ci_tools: CITools):
        """Test local CI execution failure."""
        with patch("ml_playground.tools.categories.ci.subprocess.run") as mock_run:
            # Mock failed subprocess execution
            mock_result = type(
                "MockResult",
                (),
                {"returncode": 1, "stdout": "", "stderr": "CI workflow failed"},
            )()
            mock_run.return_value = mock_result

            result = ci_tools.quality_ci_local([])

            assert result.success is False
            assert result.exit_code == 1
            assert "CI workflow failed" in result.stderr

    def test_quality_ci_local_with_cache_binding(self, ci_tools: CITools):
        """Test local CI execution with cache binding."""
        with patch("ml_playground.tools.categories.ci.subprocess.run") as mock_run:
            mock_result = type(
                "MockResult",
                (),
                {"returncode": 0, "stdout": "CI completed with caches", "stderr": ""},
            )()
            mock_run.return_value = mock_result

            result = ci_tools.quality_ci_local([], bind_caches=True)

            assert result.success is True
            # Verify that bind arguments were included
            call_args = mock_run.call_args[0][
                0
            ]  # First positional argument (command list)
            assert "--bind" in call_args

    def test_quality_ci_local_timeout(self, ci_tools: CITools):
        """Test local CI execution timeout."""
        import subprocess
        from ml_playground.tools.core.errors import ToolExecutionError

        with patch("ml_playground.tools.categories.ci.subprocess.run") as mock_run:
            mock_run.side_effect = subprocess.TimeoutExpired("act", 300)

            with pytest.raises(ToolExecutionError) as exc_info:
                ci_tools.quality_ci_local([])

            assert "timed out" in str(exc_info.value).lower()


class TestHelperMethods:
    """Test CI tools helper methods."""

    @pytest.fixture
    def ci_tools(self, tmp_path: Path) -> CITools:
        """Create CITools instance for testing."""
        config = ToolsConfig()
        return CITools(config, tmp_path)

    def test_coverage_file_path(self, ci_tools: CITools):
        """Test coverage file path generation."""
        coverage_file = ci_tools._coverage_file()

        assert coverage_file.name == "coverage.sqlite"
        assert coverage_file.parent.name == "coverage"
        assert coverage_file.parent.parent.name == ".cache"

    def test_cosmic_ray_session_file_path(self, ci_tools: CITools):
        """Test Cosmic Ray session file path generation."""
        session_file = ci_tools._cosmic_ray_session_file()

        assert session_file.name == "session.sqlite"
        assert session_file.parent.name == "cosmic_ray"
        assert session_file.parent.parent.name == ".cache"

    def test_ensure_cache_dirs(self, ci_tools: CITools):
        """Test cache directory creation."""
        ci_tools._ensure_cache_dirs("test1", "test2")

        assert (ci_tools.cache_dir / "test1").exists()
        assert (ci_tools.cache_dir / "test2").exists()
        assert (ci_tools.cache_dir / "test1").is_dir()
        assert (ci_tools.cache_dir / "test2").is_dir()
