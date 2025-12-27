"""Core interfaces and data models for the ML Playground tools system.

This module defines the fundamental protocols and data structures used
throughout the tools system, including ToolInterface, ToolResult, and
supporting models with Pydantic validation.
"""

from typing import List, Literal, Set
from pydantic import BaseModel, Field, ValidationInfo, field_validator


class LearningInfo(BaseModel):
    """Educational information about tool execution.

    Contains explanatory content for learning mode, including commands
    executed, explanations, best practices, and related concepts.
    """

    commands_executed: List[str] = Field(
        default_factory=list, description="List of actual commands executed by the tool"
    )
    explanations: List[str] = Field(
        default_factory=list,
        description="Educational explanations about what the tool does",
    )
    best_practices: List[str] = Field(
        default_factory=list,
        description="Best practices related to the tool's functionality",
    )
    related_concepts: List[str] = Field(
        default_factory=list, description="Related concepts and cross-references"
    )


class OperationId(BaseModel):
    """Structured operation identifier with validation.

    Provides a standardized way to identify operations across the tools
    and ML workflow systems using the format: namespace.category.command
    """

    namespace: Literal["tools", "ml"] = Field(
        ..., description="Operation namespace (tools or ml)"
    )
    category: str = Field(
        ..., description="Operation category (e.g., 'ci', 'quality', 'prepare')"
    )
    command: str = Field(
        ...,
        description="Specific command (e.g., 'coverage-test', 'lint', 'shakespeare')",
    )

    @field_validator("category")
    @classmethod
    def validate_category(cls, v: str, info: ValidationInfo) -> str:
        """Validate category based on namespace."""
        valid_categories_by_namespace: dict[str, Set[str]] = {
            "tools": {
                "ci",
                "quality",
                "test",
                "env",
                "agentic",
                "dev",
                "learn",
                "analysis",
            },
            "ml": {"prepare", "train", "sample", "analyze"},
        }

        namespace = info.data["namespace"]
        valid_categories = valid_categories_by_namespace[namespace]  # type: ignore[index]

        if v not in valid_categories:
            raise ValueError(
                f"Invalid {namespace} category: {v}. Must be one of {valid_categories}"
            )
        return v

    @field_validator("command")
    @classmethod
    def validate_command_format(cls, v: str) -> str:
        """Ensure command follows kebab-case naming."""
        if not v.replace("-", "").replace("_", "").isalnum():
            raise ValueError(
                f"Command must be alphanumeric with hyphens/underscores: {v}"
            )
        return v

    def __str__(self) -> str:
        """Generate the dot-separated operation ID."""
        return f"{self.namespace}.{self.category}.{self.command}"


class ToolResult(BaseModel):
    """Result of tool execution with validated operation identification.

    This class can be reused by the existing ML workflow CLI (cli.py)
    for consistent result handling across all CLI operations.
    """

    success: bool = Field(..., description="Whether the tool execution was successful")
    exit_code: int = Field(..., description="Exit code from the tool execution")
    stdout: str = Field(default="", description="Standard output from the tool")
    stderr: str = Field(default="", description="Standard error from the tool")
    learning_info: LearningInfo = Field(
        default_factory=LearningInfo,
        description="Educational information for learning mode",
    )
    operation_id: OperationId = Field(
        ..., description="Structured identifier for the operation"
    )

    @classmethod
    def create(
        cls,
        success: bool,
        exit_code: int,
        namespace: Literal["tools", "ml"],
        category: str,
        command: str,
        stdout: str = "",
        stderr: str = "",
        learning_info: LearningInfo | None = None,
    ) -> "ToolResult":
        """Factory method to create ToolResult with validated operation_id."""
        if learning_info is None:
            learning_info = LearningInfo()

        operation_id = OperationId(
            namespace=namespace, category=category, command=command
        )
        return cls(
            success=success,
            exit_code=exit_code,
            stdout=stdout,
            stderr=stderr,
            learning_info=learning_info,
            operation_id=operation_id,
        )


class ToolInterface:
    """Runtime interface contract for integrated tools."""

    @property
    def category(self) -> str:
        """The tool category (e.g., 'ci', 'quality', 'test')."""
        raise NotImplementedError

    @property
    def command(self) -> str:
        """The specific command name (e.g., 'coverage-test', 'lint')."""
        raise NotImplementedError

    @property
    def description(self) -> str:
        """A brief description of what the tool does."""
        raise NotImplementedError

    def execute(
        self,
        args: List[str],
        *,
        learning_mode: bool = False,
        verbosity_level: int = 0,
        dry_run: bool = False,
    ) -> ToolResult:
        """Execute the tool with the given arguments."""
        raise NotImplementedError

    def get_help(self) -> str:
        """Return help text for the tool."""
        raise NotImplementedError

    def validate_args(self, args: List[str]) -> None:
        """Validate arguments before execution."""
        raise NotImplementedError
