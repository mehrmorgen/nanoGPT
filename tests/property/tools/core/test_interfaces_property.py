"""Property-based tests for tools.core interfaces."""

from __future__ import annotations

from typing import Literal, cast

from hypothesis import given, settings
from hypothesis import strategies as st

from ml_playground.tools.core.interfaces import LearningInfo, OperationId, ToolResult

TOOLS_CATEGORIES = [
    "ci",
    "quality",
    "test",
    "env",
    "agentic",
    "dev",
    "learn",
    "analysis",
]
ML_CATEGORIES = ["prepare", "train", "sample", "analyze"]


@given(
    namespace=st.sampled_from(["tools", "ml"]),
    category_tools=st.sampled_from(TOOLS_CATEGORIES),
    category_ml=st.sampled_from(ML_CATEGORIES),
    command=st.text(
        min_size=1,
        max_size=20,
        alphabet=st.characters(whitelist_categories=["Ll", "Lu"]),
    ),
)
@settings(max_examples=30, deadline=None, derandomize=True)
def test_operation_id_accepts_only_valid_categories(
    namespace: str, category_tools: str, category_ml: str, command: str
) -> None:
    category = category_tools if namespace == "tools" else category_ml
    ns_literal = cast(Literal["tools", "ml"], namespace)
    op = OperationId(namespace=ns_literal, category=category, command=command)
    assert op.namespace == namespace
    assert op.category == category
    assert op.command == command


@given(
    namespace=st.sampled_from(["tools", "ml"]),
    command=st.text(
        min_size=1,
        max_size=20,
        alphabet=st.characters(whitelist_categories=["Ll", "Lu"]),
    ),
    bad_category=st.text(min_size=1, max_size=6, alphabet="xyzuvw"),
)
@settings(max_examples=25, deadline=None, derandomize=True)
def test_operation_id_rejects_invalid_categories(
    namespace: str, command: str, bad_category: str
) -> None:
    """Ensure category validator raises for categories outside the allowlist."""
    if namespace == "tools" and bad_category in TOOLS_CATEGORIES:
        return
    if namespace == "ml" and bad_category in ML_CATEGORIES:
        return
    ns_literal = cast(Literal["tools", "ml"], namespace)
    try:
        OperationId(namespace=ns_literal, category=bad_category, command=command)
    except ValueError as exc:
        assert "Invalid" in str(exc)
    else:
        raise AssertionError("Expected ValueError for invalid category")


@given(
    success=st.booleans(),
    exit_code=st.integers(min_value=0, max_value=255),
    category=st.sampled_from(TOOLS_CATEGORIES),
    command=st.text(
        min_size=1,
        max_size=10,
        alphabet=st.characters(whitelist_categories=["Ll", "Lu"]),
    ),
    stdout=st.text(min_size=0, max_size=50),
    stderr=st.text(min_size=0, max_size=50),
    has_learning=st.booleans(),
)
@settings(max_examples=20, deadline=None, derandomize=True)
def test_tool_result_create_sets_defaults_and_accepts_custom_learning(
    success: bool,
    exit_code: int,
    category: str,
    command: str,
    stdout: str,
    stderr: str,
    has_learning: bool,
) -> None:
    learning = (
        LearningInfo(
            commands_executed=["cmd"],
            explanations=["exp"],
            best_practices=["bp"],
            related_concepts=["rc"],
        )
        if has_learning
        else None
    )

    result = ToolResult.create(
        success=success,
        exit_code=exit_code,
        namespace="tools",
        category=category,
        command=command,
        stdout=stdout,
        stderr=stderr,
        learning_info=learning,
    )

    assert result.operation_id.namespace == "tools"
    assert result.operation_id.category == category
    assert result.operation_id.command == command
    assert result.stdout == stdout
    assert result.stderr == stderr
    if has_learning:
        assert result.learning_info == learning
    else:
        assert isinstance(result.learning_info, LearningInfo)
        assert result.learning_info.commands_executed == []
        assert result.learning_info.explanations == []
        assert result.learning_info.best_practices == []
        assert result.learning_info.related_concepts == []
