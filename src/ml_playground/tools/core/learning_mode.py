"""Learning mode engine for educational tool explanations.

This module provides the LearningModeEngine class that generates educational
content for tool operations, including command explanations, best practices,
and related concepts at different verbosity levels.
"""

from enum import Enum
from typing import Dict, List, Optional, Any
from .interfaces import LearningInfo


class VerbosityLevel(Enum):
    """Learning mode verbosity levels.
    
    Defines the amount of educational content to provide:
    - MINIMAL: Just show what commands are being executed
    - STANDARD: Balanced explanations with some context (default)
    - COMPREHENSIVE: Full explanations for beginners
    """
    
    MINIMAL = 0      # "I just want to see what the current implementation is"
    STANDARD = 1     # Balanced explanations with context
    COMPREHENSIVE = 2 # "I am new to all of this" - comprehensive explanations


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
        executed_commands: Optional[List[str]] = None
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
        explanations = self._generate_explanations(command, context, category, base_content)
        best_practices = self._generate_best_practices(command, category, base_content)
        related_concepts = self._generate_related_concepts(command, category, base_content)
        
        return LearningInfo(
            commands_executed=executed_commands,
            explanations=explanations,
            best_practices=best_practices,
            related_concepts=related_concepts
        )
    
    def format_output(
        self, 
        tool_result: Any,  # ToolResult type - avoiding circular import
        learning_enabled: bool
    ) -> str:
        """Format tool output with optional learning information.
        
        Args:
            tool_result: The ToolResult from tool execution
            learning_enabled: Whether to include learning information
            
        Returns:
            Formatted output string with optional educational content
        """
        output_lines = []
        
        # Always show the basic result
        if tool_result.success:
            output_lines.append(f"✓ {tool_result.operation_id} completed successfully")
        else:
            output_lines.append(f"✗ {tool_result.operation_id} failed (exit code: {tool_result.exit_code})")
        
        # Add stdout/stderr if present
        if tool_result.stdout.strip():
            output_lines.append("\nOutput:")
            output_lines.append(tool_result.stdout.strip())
        
        if tool_result.stderr.strip():
            output_lines.append("\nErrors:")
            output_lines.append(tool_result.stderr.strip())
        
        # Add learning information if enabled
        if learning_enabled and tool_result.learning_info:
            output_lines.extend(self._format_learning_info(tool_result.learning_info))
        
        return "\n".join(output_lines)
    
    def _generate_explanations(
        self, 
        command: str, 
        context: str, 
        category: str, 
        base_content: Dict[str, Any]
    ) -> List[str]:
        """Generate explanations based on verbosity level."""
        explanations = []
        
        if self.verbosity == VerbosityLevel.MINIMAL:
            # Just basic command info
            if base_content.get("minimal_explanation"):
                explanations.append(base_content["minimal_explanation"])
        
        elif self.verbosity == VerbosityLevel.STANDARD:
            # Balanced explanations
            if base_content.get("standard_explanation"):
                explanations.extend(base_content["standard_explanation"])
            
        elif self.verbosity == VerbosityLevel.COMPREHENSIVE:
            # Full explanations for beginners
            if base_content.get("comprehensive_explanation"):
                explanations.extend(base_content["comprehensive_explanation"])
        
        # Add context-specific explanation if provided
        if context and self.verbosity != VerbosityLevel.MINIMAL:
            explanations.append(f"Context: {context}")
        
        return explanations
    
    def _generate_best_practices(
        self, 
        command: str, 
        category: str, 
        base_content: Dict[str, Any]
    ) -> List[str]:
        """Generate best practices based on verbosity level."""
        if self.verbosity == VerbosityLevel.MINIMAL:
            return []
        
        best_practices = []
        
        # Add command-specific best practices
        if base_content.get("best_practices"):
            best_practices.extend(base_content["best_practices"])
        
        # Add category-level best practices for comprehensive mode
        if self.verbosity == VerbosityLevel.COMPREHENSIVE:
            category_practices = self._get_category_best_practices(category)
            best_practices.extend(category_practices)
        
        return best_practices
    
    def _generate_related_concepts(
        self, 
        command: str, 
        category: str, 
        base_content: Dict[str, Any]
    ) -> List[str]:
        """Generate related concepts based on verbosity level."""
        if self.verbosity == VerbosityLevel.MINIMAL:
            return []
        
        related_concepts = []
        
        # Add command-specific related concepts
        if base_content.get("related_concepts"):
            related_concepts.extend(base_content["related_concepts"])
        
        return related_concepts
    
    def _format_learning_info(self, learning_info: LearningInfo) -> List[str]:
        """Format learning information for display."""
        lines = []
        
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
    
    def _get_category_best_practices(self, category: str) -> List[str]:
        """Get general best practices for a tool category."""
        category_practices = {
            "quality": [
                "Run quality checks early and often in your development workflow",
                "Fix linting issues before committing code to maintain consistency",
                "Use type checking to catch errors before runtime"
            ],
            "test": [
                "Write tests before implementing features (TDD approach)",
                "Aim for high test coverage but focus on meaningful tests",
                "Run tests frequently during development to catch regressions early"
            ],
            "env": [
                "Keep your development environment synchronized with team standards",
                "Regularly update dependencies to get security fixes and improvements",
                "Use virtual environments to isolate project dependencies"
            ],
            "ci": [
                "Automate quality gates to ensure consistent code quality",
                "Use mutation testing to validate test effectiveness",
                "Monitor coverage trends to maintain code quality over time"
            ],
            "agentic": [
                "Use AI tools to augment, not replace, human judgment",
                "Review AI-generated code carefully before integration",
                "Maintain clear guidelines for AI-assisted development workflows"
            ]
        }
        
        return category_practices.get(category, [])
    
    def _initialize_educational_content(self) -> Dict[str, Dict[str, Any]]:
        """Initialize the educational content database.
        
        This will be populated in subtask 4.2 with specific content
        for testing and quality tools.
        """
        return {}