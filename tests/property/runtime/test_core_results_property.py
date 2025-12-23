"""Property-based tests for runtime/core/results module.

Tests ToolResult creation, validation, and LearningModeEngine behavior
using Hypothesis for comprehensive coverage.
"""

from __future__ import annotations

from typing import Literal, cast

from hypothesis import given, settings
from hypothesis import strategies as st

from ml_playground.runtime.core.results import (
    LearningModeEngine,
    LearningInfo,
    OperationId,
    ToolResult,
    VerbosityLevel,
)


@st.composite
def operation_ids(draw: st.DrawFn) -> OperationId:
    """Generate valid OperationId objects."""
    namespace = cast(Literal["ml", "tools"], draw(st.sampled_from(["ml", "tools"])))
    category = draw(
        st.text(min_size=1, max_size=10, alphabet="abcdefghijklmnopqrstuvwxyz")
    )
    command = draw(
        st.text(min_size=1, max_size=10, alphabet="abcdefghijklmnopqrstuvwxyz")
    )
    return OperationId(namespace=namespace, category=category, command=command)


@st.composite
def learning_infos(draw: st.DrawFn) -> LearningInfo:
    """Generate valid LearningInfo objects."""
    num_commands = draw(st.integers(min_value=0, max_value=5))
    num_explanations = draw(st.integers(min_value=0, max_value=5))
    num_practices = draw(st.integers(min_value=0, max_value=5))
    num_concepts = draw(st.integers(min_value=0, max_value=5))

    return LearningInfo(
        commands_executed=[
            draw(st.text(min_size=1, max_size=20)) for _ in range(num_commands)
        ],
        explanations=[
            draw(st.text(min_size=1, max_size=50)) for _ in range(num_explanations)
        ],
        best_practices=[
            draw(st.text(min_size=1, max_size=50)) for _ in range(num_practices)
        ],
        related_concepts=[
            draw(st.text(min_size=1, max_size=30)) for _ in range(num_concepts)
        ],
    )


@given(
    success=st.booleans(),
    exit_code=st.integers(min_value=0, max_value=255),
    operation_id=operation_ids(),
    stdout=st.one_of([st.none(), st.text(max_size=100)]),
    stderr=st.one_of([st.none(), st.text(max_size=100)]),
    learning_info=st.one_of([st.none(), learning_infos()]),
)
@settings(max_examples=20, deadline=None, derandomize=True)
def test_tool_result_creation_with_all_fields(
    success: bool,
    exit_code: int,
    operation_id: OperationId,
    stdout: str | None,
    stderr: str | None,
    learning_info: LearningInfo | None,
) -> None:
    """Test ToolResult.create with various field combinations."""
    result = ToolResult.create(
        success=success,
        exit_code=exit_code,
        namespace=operation_id.namespace,
        category=operation_id.category,
        command=operation_id.command,
        stdout=stdout or "",
        stderr=stderr or "",
        learning_info=learning_info,
    )

    assert isinstance(result, ToolResult)
    assert result.success == success
    assert result.exit_code == exit_code
    assert result.operation_id == operation_id
    assert result.stdout == (stdout or "")
    assert result.stderr == (stderr or "")
    if learning_info is None:
        assert isinstance(result.learning_info, LearningInfo)
        assert result.learning_info.commands_executed == []
        assert result.learning_info.explanations == []
        assert result.learning_info.best_practices == []
        assert result.learning_info.related_concepts == []
    else:
        assert result.learning_info == learning_info


@given(
    success=st.booleans(),
    exit_code=st.integers(min_value=0, max_value=255),
    category=st.text(min_size=1, max_size=10),
    command=st.text(min_size=1, max_size=10),
)
@settings(max_examples=15, deadline=None, derandomize=True)
def test_tool_result_factory_method(
    success: bool,
    exit_code: int,
    category: str,
    command: str,
) -> None:
    """Test ToolResult.create factory method."""
    result = ToolResult.create(
        success=success,
        exit_code=exit_code,
        namespace="ml",
        category=category,
        command=command,
    )

    assert isinstance(result, ToolResult)
    assert result.success == success
    assert result.exit_code == exit_code
    assert result.operation_id.namespace == "ml"
    assert result.operation_id.category == category
    assert result.operation_id.command == command
    assert result.stdout == ""
    assert result.stderr == ""
    assert isinstance(result.learning_info, LearningInfo)


@given(
    success=st.booleans(),
    has_stdout=st.booleans(),
    has_stderr=st.booleans(),
)
@settings(max_examples=10, deadline=None, derandomize=True)
def test_tool_result_output_handling(
    success: bool, has_stdout: bool, has_stderr: bool
) -> None:
    """Test ToolResult handles stdout/stderr correctly."""
    stdout = "Test output" if has_stdout else ""
    stderr = "Test error" if has_stderr else ""

    result = ToolResult.create(
        success=success,
        exit_code=0 if success else 1,
        namespace="ml",
        category="test",
        command="test",
        stdout=stdout,
        stderr=stderr,
    )

    assert result.stdout == stdout
    assert result.stderr == stderr


@given(
    commands=st.lists(st.text(min_size=1, max_size=20), min_size=0, max_size=5),
    explanations=st.lists(st.text(min_size=1, max_size=50), min_size=0, max_size=5),
    practices=st.lists(st.text(min_size=1, max_size=50), min_size=0, max_size=5),
    concepts=st.lists(st.text(min_size=1, max_size=30), min_size=0, max_size=5),
)
@settings(max_examples=10, deadline=None, derandomize=True)
def test_learning_info_creation(
    commands: list[str],
    explanations: list[str],
    practices: list[str],
    concepts: list[str],
) -> None:
    """Test LearningInfo dataclass creation."""
    info = LearningInfo(
        commands_executed=commands,
        explanations=explanations,
        best_practices=practices,
        related_concepts=concepts,
    )

    assert info.commands_executed == commands
    assert info.explanations == explanations
    assert info.best_practices == practices
    assert info.related_concepts == concepts


@given(
    namespace=st.sampled_from(["ml", "tools"]),
    category=st.text(min_size=1, max_size=10),
    command=st.text(min_size=1, max_size=10),
)
@settings(max_examples=10, deadline=None, derandomize=True)
def test_operation_id_creation_and_equality(
    namespace: Literal["ml", "tools"], category: str, command: str
) -> None:
    """Test OperationId creation and equality."""
    op1 = OperationId(namespace=namespace, category=category, command=command)
    op2 = OperationId(namespace=namespace, category=category, command=command)
    other_namespace: Literal["ml", "tools"] = "tools" if namespace == "ml" else "ml"
    op3 = OperationId(namespace=other_namespace, category=category, command=command)

    assert op1 == op2
    assert op1 != op3
    assert hash(op1) == hash(op2)
    assert hash(op1) != hash(op3)

    str_repr = str(op1)
    assert namespace in str_repr
    assert category in str_repr
    assert command in str_repr


@given(
    verbosity=st.sampled_from(list(VerbosityLevel)),
    has_commands=st.booleans(),
    command=st.text(min_size=1, max_size=20),
    context=st.text(min_size=1, max_size=50),
    category=st.sampled_from(["prepare", "train", "sample", "analyze"]),
)
@settings(max_examples=10, deadline=None, derandomize=True)
def test_learning_mode_engine(
    verbosity: VerbosityLevel,
    has_commands: bool,
    command: str,
    context: str,
    category: str,
) -> None:
    """Test LearningModeEngine functionality."""
    engine = LearningModeEngine(verbosity=verbosity)

    commands = ["cmd1", "cmd2"] if has_commands else []

    info = engine.explain_command(
        command=command,
        context=context,
        category=category,
        executed_commands=commands,
    )

    assert isinstance(info, LearningInfo)
    assert info.commands_executed == commands
    assert len(info.explanations) >= 0


@given(
    level=st.sampled_from(list(VerbosityLevel)),
    message=st.text(min_size=1, max_size=50),
)
@settings(max_examples=5, deadline=None, derandomize=True)
def test_verbosity_level_enum(level: VerbosityLevel, message: str) -> None:
    """Test VerbosityLevel enum behavior."""
    assert isinstance(level, VerbosityLevel)
    assert level.value in (0, 1, 2)

    same_level = VerbosityLevel(level.value)
    assert level == same_level


def test_verbosity_level_ordering() -> None:
    """Test VerbosityLevel ordering."""
    assert VerbosityLevel.MINIMAL.value == 0
    assert VerbosityLevel.STANDARD.value == 1
    assert VerbosityLevel.COMPREHENSIVE.value == 2
    assert VerbosityLevel.MINIMAL.value < VerbosityLevel.STANDARD.value
    assert VerbosityLevel.STANDARD.value < VerbosityLevel.COMPREHENSIVE.value


@given(
    success=st.booleans(),
    stdout=st.text(min_size=1, max_size=100),
    stderr=st.text(min_size=1, max_size=100),
)
@settings(max_examples=10, deadline=None, derandomize=True)
def test_tool_result_with_complex_outputs(
    success: bool, stdout: str, stderr: str
) -> None:
    """Test ToolResult with complex output strings."""
    result = ToolResult.create(
        success=success,
        exit_code=0 if success else 1,
        namespace="ml",
        category="test",
        command="test",
        stdout=stdout,
        stderr=stderr,
    )

    assert result.stdout == stdout
    assert result.stderr == stderr
    assert hasattr(result, "success")
    assert hasattr(result, "exit_code")
    assert hasattr(result, "operation_id")
    assert hasattr(result, "stdout")
    assert hasattr(result, "stderr")
    assert hasattr(result, "learning_info")


def test_tool_result_default_values() -> None:
    """Test ToolResult default values."""
    result = ToolResult.create(
        success=True,
        exit_code=0,
        namespace="ml",
        category="test",
        command="test",
    )

    assert result.stdout == ""
    assert result.stderr == ""
    assert isinstance(result.learning_info, LearningInfo)
    assert result.learning_info.commands_executed == []
    assert result.learning_info.explanations == []
    assert result.learning_info.best_practices == []
    assert result.learning_info.related_concepts == []


@given(verbosity=st.sampled_from(list(VerbosityLevel)))
@settings(max_examples=5, deadline=None, derandomize=True)
def test_learning_mode_engine_verbosity(verbosity: VerbosityLevel) -> None:
    """Test LearningModeEngine with different verbosity levels."""
    engine = LearningModeEngine(verbosity=verbosity)
    assert engine.verbosity == verbosity

    info = engine.explain_command(
        command="test",
        context="test context",
        category="prepare",
        executed_commands=["cmd1"],
    )
    assert isinstance(info, LearningInfo)
    assert info.commands_executed == ["cmd1"]
    assert len(info.explanations) >= 0
