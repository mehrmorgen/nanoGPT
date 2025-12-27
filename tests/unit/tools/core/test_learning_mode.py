"""Unit tests for learning mode engine.

Tests the LearningModeEngine class functionality including verbosity levels,
command explanations, and output formatting without using mocks.
"""

from ml_playground.tools.core.interfaces import LearningInfo, ToolResult
from ml_playground.tools.core.learning_mode import LearningModeEngine, VerbosityLevel


class TestVerbosityLevel:
    """Test VerbosityLevel enum."""

    def test_verbosity_level_values(self):
        """Test that verbosity levels have correct integer values."""
        assert VerbosityLevel.MINIMAL.value == 0
        assert VerbosityLevel.STANDARD.value == 1
        assert VerbosityLevel.COMPREHENSIVE.value == 2


class TestLearningModeEngine:
    """Test LearningModeEngine functionality."""

    def test_engine_initialization(self):
        """Test engine initializes with default verbosity."""
        engine = LearningModeEngine()
        assert engine.verbosity == VerbosityLevel.STANDARD

    def test_engine_initialization_with_verbosity(self):
        """Test engine initializes with custom verbosity."""
        engine = LearningModeEngine(VerbosityLevel.COMPREHENSIVE)
        assert engine.verbosity == VerbosityLevel.COMPREHENSIVE

    def test_explain_command_minimal_verbosity(self):
        """Test command explanation with minimal verbosity."""
        engine = LearningModeEngine(VerbosityLevel.MINIMAL)

        result = engine.explain_command(
            command="unit",
            context="Running unit tests",
            category="test",
            executed_commands=["pytest tests/unit"],
        )

        assert isinstance(result, LearningInfo)
        assert result.commands_executed == ["pytest tests/unit"]
        assert len(result.explanations) == 1
        assert "individual components work correctly" in result.explanations[0]
        assert result.best_practices == []  # Minimal mode has no best practices
        assert result.related_concepts == []  # Minimal mode has no related concepts

    def test_explain_command_minimal_omits_context(self):
        """Minimal verbosity does not add context line."""
        engine = LearningModeEngine(VerbosityLevel.MINIMAL)
        result = engine.explain_command(
            command="unit",
            context="should not appear",
            category="test",
            executed_commands=[],
        )
        assert all("Context:" not in exp for exp in result.explanations)

    def test_explain_command_standard_verbosity(self):
        """Test command explanation with standard verbosity."""
        engine = LearningModeEngine(VerbosityLevel.STANDARD)

        result = engine.explain_command(
            command="unit",
            context="Running unit tests for validation",
            category="test",
            executed_commands=["pytest tests/unit -v"],
        )

        assert isinstance(result, LearningInfo)
        assert result.commands_executed == ["pytest tests/unit -v"]
        assert len(result.explanations) >= 3  # Standard has multiple explanations
        assert "Context: Running unit tests for validation" in result.explanations
        assert len(result.best_practices) > 0  # Standard mode has best practices
        assert len(result.related_concepts) > 0  # Standard mode has related concepts

    def test_explain_command_comprehensive_verbosity(self):
        """Test command explanation with comprehensive verbosity."""
        engine = LearningModeEngine(VerbosityLevel.COMPREHENSIVE)

        result = engine.explain_command(
            command="unit",
            context="Running comprehensive unit tests",
            category="test",
            executed_commands=["pytest tests/unit --cov"],
        )

        assert isinstance(result, LearningInfo)
        assert result.commands_executed == ["pytest tests/unit --cov"]
        assert len(result.explanations) >= 5  # Comprehensive has most explanations
        assert "Context: Running comprehensive unit tests" in result.explanations
        assert len(result.best_practices) > 0
        assert len(result.related_concepts) > 0

    def test_explain_command_quality_category(self):
        """Test command explanation for quality tools."""
        engine = LearningModeEngine(VerbosityLevel.STANDARD)

        result = engine.explain_command(
            command="lint",
            context="Checking code quality",
            category="quality",
            executed_commands=["ruff check ."],
        )

        assert isinstance(result, LearningInfo)
        assert result.commands_executed == ["ruff check ."]
        assert any("style violations" in exp for exp in result.explanations)
        assert len(result.best_practices) > 0
        assert len(result.related_concepts) > 0

    def test_format_output_with_empty_learning_info(self):
        """No learning info sections are rendered when empty."""
        engine = LearningModeEngine(VerbosityLevel.STANDARD)
        result = ToolResult.create(
            success=True,
            exit_code=0,
            namespace="tools",
            category="test",
            command="unit",
        )
        output = engine.format_output(result, learning_enabled=True)
        assert "Explanation" not in output
        assert "Best practices" not in output
        assert "Related concepts" not in output

    def test_explain_command_environment_category(self):
        """Test command explanation for environment tools."""
        engine = LearningModeEngine(VerbosityLevel.STANDARD)

        result = engine.explain_command(
            command="setup",
            context="Setting up development environment",
            category="env",
            executed_commands=["uv venv", "uv sync --all-groups"],
        )

        assert isinstance(result, LearningInfo)
        assert result.commands_executed == ["uv venv", "uv sync --all-groups"]
        assert any("virtual environment" in exp for exp in result.explanations)
        assert len(result.best_practices) > 0
        assert len(result.related_concepts) > 0

    def test_explain_command_ci_category(self):
        """Test command explanation for CI tools."""
        engine = LearningModeEngine(VerbosityLevel.STANDARD)

        result = engine.explain_command(
            command="quality-gate",
            context="Running comprehensive quality checks",
            category="ci",
            executed_commands=["pre-commit run --all-files"],
        )

        assert isinstance(result, LearningInfo)
        assert result.commands_executed == ["pre-commit run --all-files"]
        assert any("quality" in exp.lower() for exp in result.explanations)
        assert len(result.best_practices) > 0
        assert len(result.related_concepts) > 0

    def test_explain_command_agentic_category(self):
        """Test command explanation for agentic tools."""
        engine = LearningModeEngine(VerbosityLevel.STANDARD)

        result = engine.explain_command(
            command="batch-review",
            context="Running batch operations for AI",
            category="agentic",
            executed_commands=["batch-quality-checks"],
        )

        assert isinstance(result, LearningInfo)
        assert result.commands_executed == ["batch-quality-checks"]
        assert any("ai" in exp.lower() for exp in result.explanations)
        assert len(result.best_practices) > 0
        assert len(result.related_concepts) > 0

    def test_explain_command_unknown_command(self):
        """Test command explanation for unknown command."""
        engine = LearningModeEngine(VerbosityLevel.STANDARD)

        result = engine.explain_command(
            command="unknown",
            context="Testing unknown command",
            category="test",
            executed_commands=["unknown-command"],
        )

        assert isinstance(result, LearningInfo)
        assert result.commands_executed == ["unknown-command"]
        # Should still provide context even for unknown commands
        assert "Context: Testing unknown command" in result.explanations

    def test_format_output_success_no_learning(self):
        """Test output formatting for successful result without learning mode."""
        engine = LearningModeEngine()

        tool_result = ToolResult.create(
            success=True,
            exit_code=0,
            namespace="tools",
            category="test",
            command="unit",
            stdout="All tests passed",
            stderr="",
        )

        output = engine.format_output(tool_result, learning_enabled=False)

        assert "✓ tools.test.unit completed successfully" in output
        assert "All tests passed" in output
        assert "💡 Explanation:" not in output  # No learning info

    def test_format_output_failure_no_learning(self):
        """Test output formatting for failed result without learning mode."""
        engine = LearningModeEngine()

        tool_result = ToolResult.create(
            success=False,
            exit_code=1,
            namespace="tools",
            category="quality",
            command="lint",
            stdout="",
            stderr="Found 3 errors",
        )

        output = engine.format_output(tool_result, learning_enabled=False)

        assert "✗ tools.quality.lint failed (exit code: 1)" in output
        assert "Found 3 errors" in output
        assert "💡 Explanation:" not in output  # No learning info

    def test_format_output_with_learning_info(self):
        """Test output formatting with learning information."""
        engine = LearningModeEngine()

        learning_info = LearningInfo(
            commands_executed=["pytest tests/unit"],
            explanations=["Unit tests verify individual components"],
            best_practices=["Write tests before implementing features"],
            related_concepts=["Test-Driven Development"],
        )

        tool_result = ToolResult.create(
            success=True,
            exit_code=0,
            namespace="tools",
            category="test",
            command="unit",
            stdout="Tests completed",
            learning_info=learning_info,
        )

        output = engine.format_output(tool_result, learning_enabled=True)

        assert "✓ tools.test.unit completed successfully" in output
        assert "Tests completed" in output
        assert "📋 Commands executed:" in output
        assert "pytest tests/unit" in output
        assert "💡 Explanation:" in output
        assert "Unit tests verify individual components" in output
        assert "✨ Best practices:" in output
        assert "Write tests before implementing features" in output
        assert "🔗 Related concepts:" in output
        assert "Test-Driven Development" in output

    def test_comprehensive_includes_context(self):
        """Comprehensive verbosity appends context to explanations."""
        engine = LearningModeEngine(VerbosityLevel.COMPREHENSIVE)
        result = engine.explain_command(
            command="integration",
            context="Full stack verification",
            category="test",
            executed_commands=[],
        )
        assert any("Full stack verification" in exp for exp in result.explanations)

    def test_standard_context_and_default_commands_list(self) -> None:
        """Standard verbosity appends context and defaults executed_commands list."""
        engine = LearningModeEngine(VerbosityLevel.STANDARD)
        info = engine.explain_command(
            command="lint",
            context="Pre-commit hook",
            category="quality",
            executed_commands=None,
        )

        assert info.commands_executed == []
        assert any("Pre-commit hook" in exp for exp in info.explanations)

    def test_standard_no_context_skips_append(self) -> None:
        """Standard verbosity without context does not append context line."""
        engine = LearningModeEngine(VerbosityLevel.STANDARD)
        info = engine.explain_command(
            command="lint",
            context="",
            category="quality",
            executed_commands=["uv run tools quality lint"],
        )

        assert info.explanations  # standard_explanation present
        assert not any(exp.startswith("Context:") for exp in info.explanations)

    def test_standard_missing_content_still_handles_context_flag(self) -> None:
        """Standard verbosity with missing base content still evaluates context branch."""
        engine = LearningModeEngine(VerbosityLevel.STANDARD)
        info = engine.explain_command(
            command="nonexistent-command",
            context="Context only",
            category="quality",
            executed_commands=["uv run tools quality nonexistent-command"],
        )

        assert info.explanations == ["Context: Context only"]

    def test_comprehensive_explanations_include_context(self) -> None:
        """Comprehensive verbosity includes comprehensive_explanation and context."""
        engine = LearningModeEngine(VerbosityLevel.COMPREHENSIVE)
        info = engine.explain_command(
            command="lint",
            context="CI pipeline",
            category="quality",
            executed_commands=["uv run tools quality lint"],
        )

        assert any("lint" in exp.lower() for exp in info.explanations)
        assert any("CI pipeline" in exp for exp in info.explanations)

    def test_comprehensive_explanations_from_content_and_context(self) -> None:
        """Comprehensive branch emits content plus context."""
        engine = LearningModeEngine(VerbosityLevel.COMPREHENSIVE)
        info = engine.explain_command(
            command="lint",
            context="Pipeline context",
            category="quality",
            executed_commands=["uv run tools quality lint"],
        )

        assert any("Linting is the process" in exp for exp in info.explanations)
        assert info.explanations[-1] == "Context: Pipeline context"

    def test_comprehensive_explanations_from_content_without_context(self) -> None:
        """Comprehensive branch emits content and omits context when none provided."""
        engine = LearningModeEngine(VerbosityLevel.COMPREHENSIVE)
        info = engine.explain_command(
            command="lint",
            context="",
            category="quality",
            executed_commands=["uv run tools quality lint"],
        )

        assert any("Linting is the process" in exp for exp in info.explanations)
        assert not any(exp.startswith("Context:") for exp in info.explanations)

    def test_comprehensive_explanations_graceful_when_missing_content(self) -> None:
        """Comprehensive verbosity tolerates missing content and still adds context."""
        engine = LearningModeEngine(VerbosityLevel.COMPREHENSIVE)
        info = engine.explain_command(
            command="nonexistent-command",
            context="Debugging session",
            category="quality",
            executed_commands=["uv run tools quality nonexistent-command"],
        )

        assert info.explanations  # should include context even without base content
        assert info.explanations[-1].startswith("Context: Debugging session")

    def test_minimal_verbosity_skips_context_even_when_provided(self) -> None:
        """Minimal verbosity never appends context lines."""
        engine = LearningModeEngine(VerbosityLevel.MINIMAL)
        info = engine.explain_command(
            command="lint",
            context="Should not appear",
            category="quality",
            executed_commands=["uv run tools quality lint"],
        )

        assert all("Context:" not in exp for exp in info.explanations)

    def test_comprehensive_no_content_no_context_results_empty(self) -> None:
        """Comprehensive branch with no content and no context yields empty explanations."""
        engine = LearningModeEngine(VerbosityLevel.COMPREHENSIVE)
        info = engine.explain_command(
            command="nonexistent-command",
            context="",
            category="quality",
            executed_commands=["uv run tools quality nonexistent-command"],
        )

        assert info.explanations == []

    def test_category_best_practices_public_accessor(self):
        """Test category-specific best practices via public accessor."""
        engine = LearningModeEngine(VerbosityLevel.COMPREHENSIVE)
        quality_practices = engine.get_category_best_practices("quality")
        assert quality_practices
        assert any("lint" in practice.lower() for practice in quality_practices)

        test_practices = engine.get_category_best_practices("test")
        assert test_practices
        assert any("test" in practice.lower() for practice in test_practices)

        assert engine.get_category_best_practices("unknown") == []

    def test_learning_info_structure(self):
        """Test that LearningInfo has correct structure."""
        learning_info = LearningInfo()

        # Test default values
        assert learning_info.commands_executed == []
        assert learning_info.explanations == []
        assert learning_info.best_practices == []
        assert learning_info.related_concepts == []

        # Test with data
        learning_info = LearningInfo(
            commands_executed=["test command"],
            explanations=["test explanation"],
            best_practices=["test practice"],
            related_concepts=["test concept"],
        )

        assert learning_info.commands_executed == ["test command"]
        assert learning_info.explanations == ["test explanation"]
        assert learning_info.best_practices == ["test practice"]
        assert learning_info.related_concepts == ["test concept"]
