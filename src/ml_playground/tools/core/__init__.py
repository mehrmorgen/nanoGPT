"""Core infrastructure for the ML Playground tools system.

This package contains the fundamental interfaces, error handling,
configuration management, and execution engine for the unified tooling system.
"""

from ml_playground.tools.core.interfaces import (
    ToolInterface,
    ToolResult,
    OperationId,
    LearningInfo,
)
from ml_playground.tools.core.errors import (
    ToolExecutionError,
    ToolConfigurationError,
    EnvironmentSetupError,
    DependencyError,
)

__all__ = [
    "ToolInterface",
    "ToolResult",
    "OperationId", 
    "LearningInfo",
    "ToolExecutionError",
    "ToolConfigurationError",
    "EnvironmentSetupError",
    "DependencyError",
]