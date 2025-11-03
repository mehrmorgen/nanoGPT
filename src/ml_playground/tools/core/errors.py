"""Tool-specific error handling for the ML Playground tools system.

This module defines error types specific to tool execution, configuration,
and environment management, all following the MLPlaygroundError pattern
with structured reason and rationale information.
"""

from ml_playground.core.error_handling import MLPlaygroundError


class ToolExecutionError(MLPlaygroundError):
    """Raised when tool execution fails.

    This is the base class for all tool execution failures, including
    subprocess failures, timeout errors, and unexpected tool behavior.
    """

    pass


class ToolConfigurationError(MLPlaygroundError):
    """Raised when tool configuration is invalid.

    This includes invalid configuration values, missing required settings,
    and configuration that conflicts with the current environment.
    """

    pass


class EnvironmentSetupError(MLPlaygroundError):
    """Raised when environment setup fails.

    This covers failures in setting up the development environment,
    including dependency installation, virtual environment creation,
    and environment validation.
    """

    pass


class DependencyError(MLPlaygroundError):
    """Raised when required dependencies are missing or incompatible.

    This includes missing external tools, incompatible versions,
    and dependency conflicts that prevent tool execution.
    """

    pass


class CommandNotFoundError(ToolExecutionError):
    """Raised when a requested command is not found.

    This occurs when the external tool binary is not available in PATH
    or when a tool category/command combination is not implemented.
    """

    pass


class InvalidArgumentError(ToolExecutionError):
    """Raised when invalid arguments are provided to a tool.

    This includes malformed arguments, unsupported options, and
    argument combinations that are not valid for the specific tool.
    """

    pass


class TimeoutError(ToolExecutionError):
    """Raised when tool execution times out.

    This follows the project's timeout philosophy: there is no such thing
    as an infinite timeout. All timeouts should be short and based on the
    specific operation and environment.
    """

    pass


def handle_subprocess_failure(
    command: str, exit_code: int, stderr: str, timeout_seconds: int | None = None
) -> None:
    """Handle subprocess execution failure with structured error information.

    Args:
        command: The command that failed
        exit_code: The exit code returned by the process
        stderr: Standard error output from the process
        timeout_seconds: The timeout value if applicable

    Raises:
        CommandNotFoundError: If the command was not found
        InvalidArgumentError: If the arguments were invalid
        TimeoutError: If the process timed out
        ToolExecutionError: For other execution failures
    """
    if "command not found" in stderr.lower() or "not found" in stderr.lower():
        raise CommandNotFoundError(
            f"Tool command '{command}' not found",
            reason="External tool binary is not available in PATH",
            rationale="All required tools must be installed and accessible for the development workflow to function",
        )

    if exit_code == 2:  # Common exit code for argument errors
        raise InvalidArgumentError(
            f"Invalid arguments provided to '{command}'",
            reason=f"Tool exited with code {exit_code} indicating argument problems",
            rationale="Tool arguments must be validated before execution to ensure predictable behavior",
        )

    stderr_normalized = stderr.lower()

    if (
        exit_code == 124
        or "timeout" in stderr_normalized
        or "timed out" in stderr_normalized
    ):
        timeout_msg = f" (timeout: {timeout_seconds}s)" if timeout_seconds else ""
        raise TimeoutError(
            f"Tool '{command}' timed out{timeout_msg}",
            reason="Process exceeded the configured timeout limit",
            rationale="Timeouts indicate environmental assumptions are wrong; choose timeouts based on expected operation duration",
        )

    # Generic execution error
    raise ToolExecutionError(
        f"Tool '{command}' failed with exit code {exit_code}",
        reason=f"External process returned non-zero exit code {exit_code}",
        rationale="Tool execution must succeed for the development workflow to proceed reliably",
    )


def handle_configuration_error(
    setting_name: str,
    setting_value: str | None = None,
    expected_type: str | None = None,
) -> None:
    """Handle configuration validation failure.

    Args:
        setting_name: The name of the configuration setting
        setting_value: The invalid value (if applicable)
        expected_type: The expected type or format (if applicable)

    Raises:
        ToolConfigurationError: With structured error information
    """
    if setting_value is None:
        raise ToolConfigurationError(
            f"Required configuration setting '{setting_name}' is missing",
            reason="Configuration entry is not present in pyproject.toml",
            rationale="Tool configuration must be complete to ensure predictable behavior across environments",
        )

    if expected_type:
        raise ToolConfigurationError(
            f"Configuration setting '{setting_name}' has invalid value '{setting_value}', expected {expected_type}",
            reason="Configuration value does not match expected format or type",
            rationale="Configuration validation prevents runtime failures by catching issues early",
        )

    raise ToolConfigurationError(
        f"Configuration setting '{setting_name}' is invalid: {setting_value}",
        reason="Configuration value failed validation checks",
        rationale="All configuration must be validated to maintain system reliability",
    )


def handle_dependency_error(
    dependency_name: str,
    required_version: str | None = None,
    found_version: str | None = None,
) -> None:
    """Handle dependency validation failure.

    Args:
        dependency_name: The name of the missing or incompatible dependency
        required_version: The required version (if applicable)
        found_version: The found version (if applicable)

    Raises:
        DependencyError: With structured error information
    """
    if found_version is None:
        raise DependencyError(
            f"Required dependency '{dependency_name}' is not installed",
            reason="External tool is not available in the current environment",
            rationale="All development tools must be installed to maintain workflow consistency",
        )

    if required_version and found_version != required_version:
        raise DependencyError(
            f"Dependency '{dependency_name}' version mismatch: required {required_version}, found {found_version}",
            reason="Installed version does not match requirements",
            rationale="Version consistency ensures reproducible behavior across development environments",
        )

    raise DependencyError(
        f"Dependency '{dependency_name}' is incompatible",
        reason="Dependency validation failed for unknown reasons",
        rationale="All dependencies must be validated to prevent runtime failures",
    )
