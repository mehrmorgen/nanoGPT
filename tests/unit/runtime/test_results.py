from __future__ import annotations

import pytest

from ml_playground.runtime.core.results import (
    LearningInfo,
    LearningModeEngine,
    OperationId,
    ToolResult,
    VerbosityLevel,
)


def test_operation_id_requires_category() -> None:
    with pytest.raises(ValueError):
        OperationId(namespace="ml", category="", command="prepare")


def test_operation_id_requires_command() -> None:
    with pytest.raises(ValueError):
        OperationId(namespace="tools", category="train", command="")


def test_operation_id_string_representation() -> None:
    op_id = OperationId(namespace="ml", category="train", command="demo")
    assert str(op_id) == "ml.train.demo"


def test_tool_result_create_populates_defaults() -> None:
    result = ToolResult.create(
        success=True,
        exit_code=0,
        namespace="ml",
        category="prepare",
        command="demo",
        stdout="ok",
    )
    assert result.success is True
    assert result.exit_code == 0
    assert result.stdout == "ok"
    assert isinstance(result.learning_info, LearningInfo)
    assert result.operation_id == OperationId(
        namespace="ml", category="prepare", command="demo"
    )


def test_learning_mode_engine_minimal_verbosity() -> None:
    engine = LearningModeEngine(VerbosityLevel.MINIMAL)
    info = engine.explain_command(
        command="demo",
        context="minimal context",
        category="prepare",
        executed_commands=["prepare demo"],
    )
    assert info.commands_executed == ["prepare demo"]
    # Minimal verbosity only includes the minimal template.
    assert info.explanations == ["Prepares data before training."]
    assert info.best_practices == []
    assert info.related_concepts == []


def test_learning_mode_engine_standard_includes_context_and_practices() -> None:
    engine = LearningModeEngine(VerbosityLevel.STANDARD)
    info = engine.explain_command(
        command="demo",
        context="standard context",
        category="train",
        executed_commands=["train demo"],
    )
    assert "Context: standard context" in info.explanations
    assert any(
        "training loop" in explanation.lower() for explanation in info.explanations
    )
    # Standard verbosity adds best practices and related concepts.
    assert "Track loss curves to detect divergence early." in info.best_practices
    assert "Gradient descent" in info.related_concepts


def test_learning_mode_engine_comprehensive_extends_templates() -> None:
    engine = LearningModeEngine(VerbosityLevel.COMPREHENSIVE)
    info = engine.explain_command(
        command="demo",
        context="comprehensive context",
        category="sample",
        executed_commands=["sample demo"],
    )
    assert any("Sampling evaluates inference" in exp for exp in info.explanations)
    assert (
        "Compare samples across checkpoints to track quality drift."
        in info.best_practices
    )
    assert "Decoding strategies" in info.related_concepts


def test_learning_mode_engine_command_override_applies() -> None:
    engine = LearningModeEngine(VerbosityLevel.STANDARD)
    info = engine.explain_command(
        command="bundestag_char",
        context="analysis",
        category="prepare",
        executed_commands=["prepare bundestag_char"],
    )
    assert any(
        "character-level tokens" in explanation for explanation in info.explanations
    )


def test_learning_mode_engine_unknown_category_still_records_context() -> None:
    engine = LearningModeEngine(VerbosityLevel.STANDARD)
    info = engine.explain_command(
        command="demo",
        context="unknown context",
        category="unknown",
        executed_commands=["custom"],
    )
    # No templates exist, but context should still be recorded.
    assert info.explanations == ["Context: unknown context"]
    assert info.best_practices == []
    assert info.related_concepts == []
