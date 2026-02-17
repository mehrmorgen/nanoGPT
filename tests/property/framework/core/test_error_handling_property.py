"""Property-based tests for error handling utilities.

Tests validation functions, progress reporters, and error formatting
using Hypothesis to discover edge cases.
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any, Mapping

import pytest
from hypothesis import assume, given, settings, strategies as st

from ml_playground.framework.core.error_handling import (
    DataError,
    ProgressReporter,
    ValidationError,
    format_error_message,
    log_operation_complete,
    log_operation_error,
    log_operation_progress,
    log_operation_start,
    validate_config_value,
    validate_directory_exists,
    validate_file_exists,
)


class CapturingLogger:
    """Logger that captures all logged messages."""

    def __init__(self) -> None:
        self.messages: list[tuple[str, str]] = []

    def info(
        self,
        msg: object,
        *args: object,
        exc_info: Any = None,
        stack_info: bool = False,
        stacklevel: int = 1,
        extra: Mapping[str, object] | None = None,
    ) -> None:
        self.messages.append(("info", str(msg)))

    def error(
        self,
        msg: object,
        *args: object,
        exc_info: Any = None,
        stack_info: bool = False,
        stacklevel: int = 1,
        extra: Mapping[str, object] | None = None,
    ) -> None:
        self.messages.append(("error", str(msg)))

    def debug(
        self,
        msg: object,
        *args: object,
        exc_info: Any = None,
        stack_info: bool = False,
        stacklevel: int = 1,
        extra: Mapping[str, object] | None = None,
    ) -> None:
        pass

    def warning(
        self,
        msg: object,
        *args: object,
        exc_info: Any = None,
        stack_info: bool = False,
        stacklevel: int = 1,
        extra: Mapping[str, object] | None = None,
    ) -> None:
        pass


# =============================================================================
# validate_file_exists Property Tests
# =============================================================================


@settings(max_examples=20, deadline=None, derandomize=True)
@given(
    filename=st.text(
        alphabet=st.characters(whitelist_categories=("L", "N")),
        min_size=1,
        max_size=20,
    )
)
def test_validate_file_exists_raises_for_nonexistent(filename: str) -> None:
    """validate_file_exists raises DataError for files that don't exist."""
    path = Path("/nonexistent") / filename
    assume("/" not in filename and "\\" not in filename)
    with pytest.raises(DataError, match="not found"):
        validate_file_exists(path)


@settings(max_examples=10, deadline=None, derandomize=True)
@given(description=st.text(min_size=1, max_size=30))
def test_validate_file_exists_includes_description(description: str) -> None:
    """validate_file_exists includes description in error message."""
    path = Path("/tmp/nonexistent_file.txt")
    with pytest.raises(DataError) as exc:
        validate_file_exists(path, description=description)
    assert description in str(exc.value)


def test_validate_file_exists_rejects_directory(tmp_path: Path) -> None:
    """validate_file_exists raises when path is a directory not file."""
    dir_path = tmp_path / "testdir"
    dir_path.mkdir()
    with pytest.raises(DataError, match="not a file"):
        validate_file_exists(dir_path, description="Config")


def test_validate_file_exists_accepts_valid_file(tmp_path: Path) -> None:
    """validate_file_exists passes for actual files."""
    file_path = tmp_path / "testfile.txt"
    file_path.write_text("test")
    # Should not raise
    validate_file_exists(file_path)


# =============================================================================
# validate_directory_exists Property Tests
# =============================================================================


@settings(max_examples=20, deadline=None, derandomize=True)
@given(
    dirname=st.text(
        alphabet=st.characters(whitelist_categories=("L", "N")),
        min_size=1,
        max_size=20,
    )
)
def test_validate_directory_exists_raises_for_nonexistent(dirname: str) -> None:
    """validate_directory_exists raises DataError for directories that don't exist."""
    assume("/" not in dirname and "\\" not in dirname)
    path = Path("/nonexistent") / dirname
    with pytest.raises(DataError, match="not found"):
        validate_directory_exists(path)


@settings(max_examples=10, deadline=None, derandomize=True)
@given(description=st.text(min_size=1, max_size=30))
def test_validate_directory_exists_includes_description(description: str) -> None:
    """validate_directory_exists includes description in error message."""
    path = Path("/tmp/nonexistent_dir")
    with pytest.raises(DataError) as exc:
        validate_directory_exists(path, description=description)
    assert description in str(exc.value)


def test_validate_directory_exists_rejects_file(tmp_path: Path) -> None:
    """validate_directory_exists raises when path is a file not directory."""
    file_path = tmp_path / "testfile.txt"
    file_path.write_text("test")
    with pytest.raises(DataError, match="not a directory"):
        validate_directory_exists(file_path, description="Data")


def test_validate_directory_exists_accepts_valid_directory(tmp_path: Path) -> None:
    """validate_directory_exists passes for actual directories."""
    dir_path = tmp_path / "testdir"
    dir_path.mkdir()
    # Should not raise
    validate_directory_exists(dir_path)


# =============================================================================
# validate_config_value Property Tests
# =============================================================================


@settings(max_examples=30, deadline=None, derandomize=True)
@given(
    name=st.text(min_size=1, max_size=20).filter(
        lambda s: "'" not in s and "?" not in s
    ),
    expected_type=st.sampled_from([str, int, float, bool, list]),
)
def test_validate_config_value_rejects_none_when_required(
    name: str, expected_type: type
) -> None:
    """validate_config_value raises when required value is None."""
    with pytest.raises(ValidationError, match=re.escape(name)):
        validate_config_value(None, name, expected_type, required=True)


@settings(max_examples=30, deadline=None, derandomize=True)
@given(
    name=st.text(min_size=1, max_size=20),
    expected_type=st.sampled_from([str, int, float, bool]),
)
def test_validate_config_value_accepts_none_when_optional(
    name: str, expected_type: type
) -> None:
    """validate_config_value passes when None and not required."""
    # Should not raise
    validate_config_value(None, name, expected_type, required=False)


@settings(max_examples=30, deadline=None, derandomize=True)
@given(value=st.text(min_size=1, max_size=64), name=st.text(min_size=1, max_size=20))
def test_validate_config_value_type_mismatch(value: str, name: str) -> None:
    """validate_config_value raises on type mismatch with clear message."""
    with pytest.raises(ValidationError) as exc:
        validate_config_value(value, name, int, required=True)
    assert name in str(exc.value)
    assert "int" in str(exc.value)


@settings(max_examples=20, deadline=None, derandomize=True)
@given(value=st.integers())
def test_validate_config_value_accepts_correct_type(value: int) -> None:
    """validate_config_value passes when type matches."""
    # Should not raise
    validate_config_value(value, "test_key", int, required=True)


# =============================================================================
# format_error_message Property Tests
# =============================================================================


@settings(max_examples=30, deadline=None, derandomize=True)
@given(
    msg=st.text(max_size=128),
    context=st.one_of(st.none(), st.text(min_size=1, max_size=50)),
)
def test_format_error_message_properties(msg: str, context: str | None) -> None:
    """format_error_message includes context when provided."""
    error = ValueError(msg)
    result = format_error_message(error, context=context or "")
    assert msg in result
    if context:
        assert context in result


# =============================================================================
# ProgressReporter Property Tests
# =============================================================================


@settings(max_examples=20, deadline=None, derandomize=True)
@given(
    total_steps=st.integers(min_value=1, max_value=1000),
    update_steps=st.integers(min_value=1, max_value=100),
)
def test_progress_reporter_percent_calculation(
    total_steps: int, update_steps: int
) -> None:
    """ProgressReporter calculates percentages correctly."""
    assume(update_steps <= total_steps)
    logger = CapturingLogger()
    reporter = ProgressReporter(logger, total_steps=total_steps)
    reporter.start("Test operation")
    reporter.update(update_steps)
    # Check that progress was logged
    progress_msgs = [m for level, m in logger.messages if level == "info" and "%" in m]
    if update_steps >= total_steps * 0.1:  # Only logs every 10%
        assert len(progress_msgs) >= 1


@settings(max_examples=20, deadline=None, derandomize=True)
@given(total_steps=st.integers(min_value=1, max_value=100))
def test_progress_reporter_finish_reports_100(total_steps: int) -> None:
    """ProgressReporter.finish ensures 100% is reported."""
    logger = CapturingLogger()
    reporter = ProgressReporter(logger, total_steps=total_steps)
    reporter.start()
    reporter.current_step = total_steps - 1
    reporter.finish("Done")
    # Check final message
    finish_msgs = [m for level, m in logger.messages if "100%" in m or "Done" in m]
    assert len(finish_msgs) >= 1


@settings(max_examples=15, deadline=None, derandomize=True)
@given(steps=st.integers(min_value=0, max_value=50))
def test_progress_reporter_no_total_steps_message(steps: int) -> None:
    """ProgressReporter logs step count when no total_steps."""
    logger = CapturingLogger()
    reporter = ProgressReporter(logger, total_steps=None)
    reporter.start()
    reporter.update(steps, message="Step complete")
    step_msgs = [m for level, m in logger.messages if "Step" in m]
    if steps > 0:
        assert len(step_msgs) >= 1


@settings(max_examples=20, deadline=None, derandomize=True)
@given(
    current=st.integers(min_value=0, max_value=200),
    total=st.integers(min_value=1, max_value=100),
)
def test_progress_reporter_clamps_percentage(current: int, total: int) -> None:
    """ProgressReporter clamps percentage to 0-100 range."""
    logger = CapturingLogger()
    reporter = ProgressReporter(logger, total_steps=total)
    reporter.start()
    reporter.current_step = current
    # Manually trigger an update
    reporter.update(0)
    # Check percentage is clamped
    progress_msgs = [m for level, m in logger.messages if "%" in m]
    for msg in progress_msgs:
        # Extract percentage from message like "Progress: 50%"
        if "Progress:" in msg:
            percent_str = msg.split("%")[0].split()[-1]
            try:
                percent = int(percent_str)
                assert 0 <= percent <= 100
            except ValueError:
                pass  # Skip if parsing fails


# =============================================================================
# Log Operation Functions Property Tests
# =============================================================================


@settings(max_examples=20, deadline=None, derandomize=True)
@given(
    operation=st.text(min_size=1, max_size=30),
    details=st.one_of(st.none(), st.text(max_size=50)),
)
def test_log_operation_start_formats_correctly(
    operation: str, details: str | None
) -> None:
    """log_operation_start formats message with optional details."""
    logger = CapturingLogger()
    log_operation_start(logger, operation, details=details or "")
    assert len(logger.messages) == 1
    assert logger.messages[0][0] == "info"
    assert operation in logger.messages[0][1]
    if details:
        assert details in logger.messages[0][1]


@settings(max_examples=20, deadline=None, derandomize=True)
@given(
    operation=st.text(min_size=1, max_size=30),
    progress=st.text(min_size=1, max_size=30),
)
def test_log_operation_progress_formats_correctly(
    operation: str, progress: str
) -> None:
    """log_operation_progress formats message correctly."""
    logger = CapturingLogger()
    log_operation_progress(logger, operation, progress)
    assert len(logger.messages) == 1
    assert logger.messages[0][0] == "info"
    assert operation in logger.messages[0][1]
    assert progress in logger.messages[0][1]


@settings(max_examples=20, deadline=None, derandomize=True)
@given(
    operation=st.text(min_size=1, max_size=30),
    result=st.one_of(st.none(), st.text(max_size=50)),
)
def test_log_operation_complete_formats_correctly(
    operation: str, result: str | None
) -> None:
    """log_operation_complete formats message with optional result."""
    logger = CapturingLogger()
    log_operation_complete(logger, operation, result=result or "")
    assert len(logger.messages) == 1
    assert logger.messages[0][0] == "info"
    assert operation in logger.messages[0][1]
    if result:
        assert result in logger.messages[0][1]


@settings(max_examples=20, deadline=None, derandomize=True)
@given(operation=st.text(min_size=1, max_size=30))
def test_log_operation_error_formats_correctly(operation: str) -> None:
    """log_operation_error formats error message correctly."""
    logger = CapturingLogger()
    error = ValueError("Test error message")
    log_operation_error(logger, operation, error)
    assert len(logger.messages) == 1
    assert logger.messages[0][0] == "error"
    assert operation in logger.messages[0][1]
    assert "Test error message" in logger.messages[0][1]
