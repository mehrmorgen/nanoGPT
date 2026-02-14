"""Learning mode engine for educational tool explanations.

This module provides the LearningModeEngine class that generates educational
content for tool operations, including command explanations, best practices,
and related concepts at different verbosity levels.
"""

from enum import Enum
from typing import Dict, List, Optional, cast
from .interfaces import LearningInfo


class VerbosityLevel(Enum):
    """Learning mode verbosity levels.

    Defines the amount of educational content to provide:
    - MINIMAL: Just show what commands are being executed
    - STANDARD: Balanced explanations with some context (default)
    - COMPREHENSIVE: Full explanations for beginners
    """

    MINIMAL = 0  # "I just want to see what the current implementation is"
    STANDARD = 1  # Balanced explanations with context
    COMPREHENSIVE = 2  # "I am new to all of this" - comprehensive explanations


class LearningModeEngine:
    """Manages educational output and explanations for tool operations.

    The engine generates context-appropriate educational content based on
    the tool category, command, and verbosity level. It provides explanations
    covering machine learning best practices, software engineering principles,
    operations, security, and other aspects of software product development.
    """

    def __init__(self, verbosity: VerbosityLevel = VerbosityLevel.STANDARD):
        """Initialize the learning mode engine.

        Args:
            verbosity: Default verbosity level for explanations
        """
        self.verbosity = verbosity
        self._educational_content = self._initialize_educational_content()

    def explain_command(
        self,
        command: str,
        context: str,
        category: str,
        executed_commands: Optional[List[str]] = None,
    ) -> LearningInfo:
        """Generate educational information for a command.

        Args:
            command: The specific command being explained
            context: Additional context about the command execution
            category: The tool category (quality, test, env, ci, agentic)
            executed_commands: List of actual commands that were executed

        Returns:
            LearningInfo with educational content appropriate to verbosity level
        """
        if executed_commands is None:
            executed_commands = []

        # Get base educational content for this command
        content_key = f"{category}.{command}"
        base_content = self._educational_content.get(content_key, {})

        # Generate explanations based on verbosity level
        explanations = self._generate_explanations(
            command, context, category, base_content
        )
        best_practices = self._generate_best_practices(command, category, base_content)
        related_concepts = self._generate_related_concepts(
            command, category, base_content
        )

        return LearningInfo(
            commands_executed=executed_commands,
            explanations=explanations,
            best_practices=best_practices,
            related_concepts=related_concepts,
        )

    def format_output(
        self,
        tool_result: object,
        learning_enabled: bool,
    ) -> str:
        """Format tool output with optional learning information.

        Args:
            tool_result: The ToolResult from tool execution
            learning_enabled: Whether to include learning information

        Returns:
            Formatted output string with optional educational content
        """
        output_lines: List[str] = []

        # Access attributes through getattr to avoid circular import and Any leaks
        success = bool(getattr(tool_result, "success", False))
        operation_id = str(getattr(tool_result, "operation_id", "unknown"))
        exit_code = getattr(tool_result, "exit_code", "unknown")
        stdout = str(getattr(tool_result, "stdout", ""))
        stderr = str(getattr(tool_result, "stderr", ""))
        learning_info = getattr(tool_result, "learning_info", None)

        # Always show the basic result
        if success:
            output_lines.append(f"✓ {operation_id} completed successfully")
        else:
            output_lines.append(f"✗ {operation_id} failed (exit code: {exit_code})")

        # Add stdout/stderr if present
        stripped_stdout = stdout.strip()
        if stripped_stdout:
            output_lines.append("\nOutput:")
            output_lines.append(stripped_stdout)

        stripped_stderr = stderr.strip()
        if stripped_stderr:
            output_lines.append("\nErrors:")
            output_lines.append(stripped_stderr)

        # Add learning information if enabled
        if learning_enabled and learning_info:
            output_lines.extend(
                self._format_learning_info(cast(LearningInfo, learning_info))
            )

        return "\n".join(output_lines)

    def _generate_explanations(
        self, command: str, context: str, category: str, base_content: Dict[str, object]
    ) -> List[str]:
        """Generate explanations based on verbosity level."""
        explanations: List[str] = []

        if self.verbosity == VerbosityLevel.MINIMAL:
            minimal = base_content.get("minimal_explanation")
            if minimal:
                explanations.append(str(minimal))
            return explanations

        if self.verbosity == VerbosityLevel.STANDARD:
            standard = base_content.get("standard_explanation")
            if standard:
                explanations.extend(cast(List[str], standard))
            if context:
                explanations.append(f"Context: {context}")
            return explanations

        comprehensive = base_content.get("comprehensive_explanation")
        if comprehensive:
            explanations.extend(cast(List[str], comprehensive))
        if context:
            explanations.append(f"Context: {context}")

        return explanations

    def _generate_best_practices(
        self, command: str, category: str, base_content: Dict[str, object]
    ) -> List[str]:
        """Generate best practices based on verbosity level."""
        if self.verbosity == VerbosityLevel.MINIMAL:
            return []

        best_practices: List[str] = []

        # Add command-specific best practices
        raw_best_practices = base_content.get("best_practices")
        if raw_best_practices:
            best_practices.extend(cast(List[str], raw_best_practices))

        # Add category-level best practices for comprehensive mode
        if self.verbosity == VerbosityLevel.COMPREHENSIVE:
            category_practices = self._get_category_best_practices(category)
            best_practices.extend(category_practices)

        return best_practices

    def _generate_related_concepts(
        self, command: str, category: str, base_content: Dict[str, object]
    ) -> List[str]:
        """Generate related concepts based on verbosity level."""
        if self.verbosity == VerbosityLevel.MINIMAL:
            return []

        related_concepts: List[str] = []

        # Add command-specific related concepts
        raw_related_concepts = base_content.get("related_concepts")
        if raw_related_concepts:
            related_concepts.extend(cast(List[str], raw_related_concepts))

        return related_concepts

    def _format_learning_info(self, learning_info: LearningInfo) -> List[str]:
        """Format learning information for display."""
        lines: List[str] = []

        if learning_info.commands_executed:
            lines.append("\n📋 Commands executed:")
            for cmd in learning_info.commands_executed:
                lines.append(f"  $ {cmd}")

        if learning_info.explanations:
            lines.append("\n💡 Explanation:")
            for explanation in learning_info.explanations:
                lines.append(f"  {explanation}")

        if learning_info.best_practices:
            lines.append("\n✨ Best practices:")
            for practice in learning_info.best_practices:
                lines.append(f"  • {practice}")

        if learning_info.related_concepts:
            lines.append("\n🔗 Related concepts:")
            for concept in learning_info.related_concepts:
                lines.append(f"  • {concept}")

        return lines

    def get_category_best_practices(self, category: str) -> List[str]:
        """Public accessor for category best practices."""
        return self._get_category_best_practices(category)

    def _get_category_best_practices(self, category: str) -> List[str]:
        """Get general best practices for a tool category."""
        category_practices = {
            "quality": [
                "Run quality checks early and often in your development workflow",
                "Fix linting issues before committing code to maintain consistency",
                "Use type checking to catch errors before runtime",
            ],
            "test": [
                "Write tests before implementing features (TDD approach)",
                "Aim for high test coverage but focus on meaningful tests",
                "Run tests frequently during development to catch regressions early",
            ],
            "env": [
                "Keep your development environment synchronized with team standards",
                "Regularly update dependencies to get security fixes and improvements",
                "Use virtual environments to isolate project dependencies",
            ],
            "ci": [
                "Automate quality gates to ensure consistent code quality",
                "Use mutation testing to validate test effectiveness",
                "Monitor coverage trends to maintain code quality over time",
            ],
            "agentic": [
                "Use AI tools to augment, not replace, human judgment",
                "Review AI-generated code carefully before integration",
                "Maintain clear guidelines for AI-assisted development workflows",
            ],
            "prepare": [
                "Validate data quality and distribution before training",
                "Document data preprocessing steps for reproducibility",
                "Use consistent preprocessing across training and inference",
                "Monitor data splits to prevent leakage between train/test sets",
            ],
            "train": [
                "Monitor training and validation metrics to detect overfitting",
                "Save model checkpoints regularly to prevent data loss",
                "Use appropriate learning rate schedules for stable convergence",
                "Validate model outputs during training to ensure quality",
            ],
            "sample": [
                "Experiment with sampling parameters to achieve desired output quality",
                "Use appropriate prompts that match training data distribution",
                "Generate multiple samples to assess model consistency",
                "Evaluate generated content for quality and appropriateness",
            ],
            "analyze": [
                "Use multiple evaluation metrics for comprehensive assessment",
                "Compare model performance against appropriate baselines",
                "Include both automatic metrics and human evaluation",
                "Document analysis results for future improvement",
            ],
        }

        return category_practices.get(category, [])

    def _initialize_educational_content(self) -> Dict[str, Dict[str, object]]:
        """Initialize the educational content database from a JSON resource."""
        import json
        import importlib.resources

        try:
            # Use files() API for better compatibility and fewer Pyright issues
            resource_files = importlib.resources.files(
                "ml_playground.tools.core.resources"
            )
            content = resource_files.joinpath("educational_content.json").read_text()
            return cast(Dict[str, Dict[str, object]], json.loads(content))
        except Exception:
            # Fallback to minimal content if resource loading fails
            return {}
