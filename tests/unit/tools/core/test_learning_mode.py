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

    def test_category_best_practices(self):
        """Test category-specific best practices generation."""
        engine = LearningModeEngine(VerbosityLevel.COMPREHENSIVE)

        # Test quality category
        quality_practices = engine.get_category_best_practices("quality")
        assert len(quality_practices) > 0
        assert any("linting" in practice.lower() for practice in quality_practices)

        # Test testing category
        test_practices = engine.get_category_best_practices("test")
        assert len(test_practices) > 0
        assert any(
            "tdd" in practice.lower() or "test-driven" in practice.lower()
            for practice in test_practices
        )

        # Test unknown category
        unknown_practices = engine.get_category_best_practices("unknown")
        assert unknown_practices == []

    def test_educational_content_coverage(self):
        """Test that educational content covers expected commands."""
        engine = LearningModeEngine()

        # Test that we have content for key testing commands
        test_commands = [
            "unit",
            "integration",
            "e2e",
            "coverage",
        ]
        for command in test_commands:
            content_key = f"test.{command}"
            assert content_key in engine.educational_content
            content = engine.educational_content[content_key]
            assert "minimal_explanation" in content
            assert "standard_explanation" in content
            assert "comprehensive_explanation" in content

        # Test that we have content for key quality commands
        quality_commands = [
            "lint",
            "format",
            "deadcode",
            "basedpyright",
            "mypy",
            "typecheck",
        ]
        for command in quality_commands:
            content_key = f"quality.{command}"
            assert content_key in engine._educational_content
            content = engine._educational_content[content_key]
            assert "minimal_explanation" in content
            assert "standard_explanation" in content
            assert "comprehensive_explanation" in content

        # Test that we have content for key environment commands
        env_commands = [
            "setup",
            "sync",
            "verify",
            "clean",
            "info",
            "ai-guidelines",
            "tensorboard",
            "gguf-help",
        ]
        for command in env_commands:
            content_key = f"env.{command}"
            assert content_key in engine._educational_content
            content = engine._educational_content[content_key]
            assert "minimal_explanation" in content
            assert "standard_explanation" in content
            assert "comprehensive_explanation" in content

        # Test that we have content for key CI commands
        ci_commands = [
            "quality-gate",
            "quality-fast",
            "quality-ext",
            "quality-ci-local",
            "coverage-badge",
            "mutation-reset",
            "mutation-summary",
            "mutation-init",
            "mutation-exec",
            "mutation-report",
            "mutation-run",
        ]
        for command in ci_commands:
            content_key = f"ci.{command}"
            assert content_key in engine._educational_content
            content = engine._educational_content[content_key]
            assert "minimal_explanation" in content
            assert "standard_explanation" in content
            assert "comprehensive_explanation" in content

        # Test that we have content for key agentic commands
        agentic_commands = [
            "guidelines-setup",
            "batch-review",
            "workflow-helper",
            "batch-quality",
            "batch-validate",
            "workflow-status",
        ]
        for command in agentic_commands:
            content_key = f"agentic.{command}"
            assert content_key in engine._educational_content
            content = engine._educational_content[content_key]
            assert "minimal_explanation" in content
            assert "standard_explanation" in content
            assert "comprehensive_explanation" in content

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
