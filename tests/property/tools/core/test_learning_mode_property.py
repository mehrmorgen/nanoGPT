"""Property-based tests for tools.core.learning_mode."""

from __future__ import annotations

from typing import List

from hypothesis import given, settings
from hypothesis import strategies as st

from ml_playground.tools.core.interfaces import LearningInfo, ToolResult
from ml_playground.tools.core.learning_mode import LearningModeEngine, VerbosityLevel

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


def _engine() -> LearningModeEngine:
    return LearningModeEngine()


@given(
    verbosity=st.sampled_from(list(VerbosityLevel)),
    category=st.sampled_from(TOOLS_CATEGORIES),
    command=st.text(
        min_size=1,
        max_size=20,
        alphabet=st.characters(whitelist_categories=["Ll", "Lu"]),
    ),
    context=st.text(min_size=0, max_size=60),
    executed_commands=st.lists(
        st.text(min_size=1, max_size=40), min_size=0, max_size=4
    ),
)
@settings(max_examples=20, deadline=None, derandomize=True)
def test_explain_command_preserves_inputs_and_respects_verbosity(
    verbosity: VerbosityLevel,
    category: str,
    command: str,
    context: str,
    executed_commands: List[str],
) -> None:
    engine = LearningModeEngine(verbosity=verbosity)

    info = engine.explain_command(
        command=command,
        context=context,
        category=category,
        executed_commands=executed_commands,
    )

    assert info.commands_executed == executed_commands
    if verbosity == VerbosityLevel.MINIMAL:
        assert info.best_practices == []
        assert info.related_concepts == []
        assert all("Context:" not in exp for exp in info.explanations)
    else:
        if context:
            assert f"Context: {context}" in info.explanations


@given(
    success=st.booleans(),
    exit_code=st.integers(min_value=0, max_value=255),
    category=st.sampled_from(TOOLS_CATEGORIES),
    command=st.text(
        min_size=1,
        max_size=15,
        alphabet=st.characters(whitelist_categories=["Ll", "Lu"]),
    ),
    stdout=st.text(min_size=0, max_size=80),
    stderr=st.text(min_size=0, max_size=80),
    learning_enabled=st.booleans(),
    verbosity=st.sampled_from(list(VerbosityLevel)),
)
@settings(max_examples=15, deadline=None, derandomize=True)
def test_format_output_includes_operation_and_optional_sections(
    success: bool,
    exit_code: int,
    category: str,
    command: str,
    stdout: str,
    stderr: str,
    learning_enabled: bool,
    verbosity: VerbosityLevel,
) -> None:
    engine = _engine()

    learning_info: LearningInfo | None = None
    if learning_enabled:
        learning_info = LearningModeEngine(verbosity=verbosity).explain_command(
            command=command,
            context="context for formatting",
            category=category,
            executed_commands=["cmd"],
        )

    result = ToolResult.create(
        success=success,
        exit_code=exit_code,
        namespace="tools",
        category=category,
        command=command,
        stdout=stdout,
        stderr=stderr,
        learning_info=learning_info,
    )

    output = engine.format_output(result, learning_enabled=learning_enabled)

    assert str(result.operation_id) in output
    if success:
        assert "completed successfully" in output
    else:
        assert f"exit code: {exit_code}" in output

    if stdout.strip():
        assert "Output:" in output
        assert stdout.strip() in output
    if stderr.strip():
        assert "Errors:" in output
        assert stderr.strip() in output

    if learning_enabled and learning_info:
        if learning_info.commands_executed:
            assert "Commands executed" in output
        if learning_info.explanations:
            assert "Explanation:" in output
