"""Tests for `ml_playground.tools.core.errors`."""

from __future__ import annotations

import pytest

import ml_playground.tools.core.errors as errors


class TestHandleSubprocessFailure:
    """Tests for `handle_subprocess_failure`."""

    def test_command_not_found(self) -> None:
        with pytest.raises(errors.CommandNotFoundError) as exc:
            errors.handle_subprocess_failure("uv", 127, "command not found: uv")
        assert "uv" in str(exc.value)

    def test_invalid_arguments(self) -> None:
        with pytest.raises(errors.InvalidArgumentError) as exc:
            errors.handle_subprocess_failure("pytest", 2, "usage: pytest")
        assert "pytest" in str(exc.value)

    def test_timeout_detected(self) -> None:
        with pytest.raises(errors.TimeoutError) as exc:
            errors.handle_subprocess_failure(
                "pytest", 1, "Command timed out", timeout_seconds=30
            )
        assert "30" in str(exc.value)

    def test_generic_failure(self) -> None:
        with pytest.raises(errors.ToolExecutionError) as exc:
            errors.handle_subprocess_failure("pytest", 3, "unexpected error")
        assert "exit code 3" in str(exc.value)


class TestHandleConfigurationError:
    """Tests for `handle_configuration_error`."""

    def test_missing_value(self) -> None:
        with pytest.raises(errors.ToolConfigurationError) as exc:
            errors.handle_configuration_error("testing.timeout")
        assert "missing" in str(exc.value)

    def test_invalid_type(self) -> None:
        with pytest.raises(errors.ToolConfigurationError) as exc:
            errors.handle_configuration_error("testing.timeout", "fast", "integer")
        assert "integer" in str(exc.value)

    def test_generic_invalid(self) -> None:
        with pytest.raises(errors.ToolConfigurationError) as exc:
            errors.handle_configuration_error("testing.timeout", "-1")
        assert "-1" in str(exc.value)


class TestHandleDependencyError:
    """Tests for `handle_dependency_error`."""

    def test_missing_dependency(self) -> None:
        with pytest.raises(errors.DependencyError) as exc:
            errors.handle_dependency_error("uv")
        assert "uv" in str(exc.value)

    def test_version_mismatch(self) -> None:
        with pytest.raises(errors.DependencyError) as exc:
            errors.handle_dependency_error(
                "python", required_version="3.12", found_version="3.11"
            )
        assert "3.12" in str(exc.value)

    def test_incompatible_dependency(self) -> None:
        with pytest.raises(errors.DependencyError) as exc:
            errors.handle_dependency_error("pytest", found_version="8.2")
        assert "pytest" in str(exc.value)
