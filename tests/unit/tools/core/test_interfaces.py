"""Unit tests for `ml_playground.tools.core.interfaces`."""

from __future__ import annotations

from types import SimpleNamespace

import pytest
from pydantic import ValidationError

from ml_playground.tools.core.interfaces import (
    LearningInfo,
    OperationId,
    ToolInterface,
    ToolResult,
)


class TestOperationId:
    """Tests covering `OperationId` validation helpers."""

    def test_operation_id_valid_tools_category(self) -> None:
        op = OperationId(namespace="tools", category="ci", command="quality-gate")

        assert op.namespace == "tools"
        assert op.category == "ci"
        assert str(op) == "tools.ci.quality-gate"

    def test_operation_id_invalid_tools_category_raises(self) -> None:
        with pytest.raises(ValidationError) as excinfo:
            OperationId(namespace="tools", category="invalid", command="lint")

        assert "Invalid tools category" in str(excinfo.value)

    def test_operation_id_invalid_command_format_raises(self) -> None:
        with pytest.raises(ValidationError) as excinfo:
            OperationId(namespace="tools", category="ci", command="bad command!")

        assert "Command must be alphanumeric" in str(excinfo.value)

    def test_operation_id_ml_namespace_validates_categories(self) -> None:
        op = OperationId(namespace="ml", category="prepare", command="dataset")

        assert str(op) == "ml.prepare.dataset"

        with pytest.raises(ValidationError) as excinfo:
            OperationId(namespace="ml", category="invalid", command="step")

        assert "Invalid ml category" in str(excinfo.value)

    def test_operation_id_validate_category_ml_branch_is_executed(self) -> None:
        info = SimpleNamespace(data={"namespace": "ml"})
        assert OperationId.validate_category("prepare", info) == "prepare"


class TestToolResult:
    """Tests covering `ToolResult` factory behavior."""

    def test_tool_result_create_populates_operation_id(self) -> None:
        result = ToolResult.create(
            success=True,
            exit_code=0,
            namespace="tools",
            category="quality",
            command="lint",
            stdout="ok",
        )

        assert result.success is True
        assert result.exit_code == 0
        assert str(result.operation_id) == "tools.quality.lint"
        assert result.learning_info.commands_executed == []

    def test_tool_result_create_uses_supplied_learning_info(self) -> None:
        learning = LearningInfo(commands_executed=["uv run tools quality lint"])

        result = ToolResult.create(
            success=False,
            exit_code=1,
            namespace="tools",
            category="test",
            command="coverage-threshold",
            stderr="failed",
            learning_info=learning,
        )

        assert result.learning_info is learning
        assert result.stderr == "failed"
        assert result.success is False


class TestLearningInfo:
    """Tests for `LearningInfo` default behavior."""

    def test_learning_info_defaults_are_empty_lists(self) -> None:
        info = LearningInfo()

        assert info.commands_executed == []
        assert info.explanations == []
        assert info.best_practices == []
        assert info.related_concepts == []

    def test_learning_info_populates_fields(self) -> None:
        info = LearningInfo(
            commands_executed=["uv run tools test unit"],
            explanations=["Runs unit tests"],
            best_practices=["Write fast tests"],
            related_concepts=["TDD"],
        )

        assert info.commands_executed == ["uv run tools test unit"]
        assert info.explanations == ["Runs unit tests"]
        assert info.best_practices == ["Write fast tests"]
        assert info.related_concepts == ["TDD"]


class TestToolInterfaceProtocol:
    def test_protocol_placeholders_are_callable(self) -> None:
        obj = object()

        assert ToolInterface.category.fget(obj) is None  # type: ignore[union-attr]
        assert ToolInterface.command.fget(obj) is None  # type: ignore[union-attr]
        assert ToolInterface.description.fget(obj) is None  # type: ignore[union-attr]
        assert (
            ToolInterface.execute(
                obj, [], learning_mode=False, verbosity_level=0, dry_run=False
            )
            is None
        )
        assert ToolInterface.get_help(obj) is None
        assert ToolInterface.validate_args(obj, []) is None
