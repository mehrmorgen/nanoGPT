"""Unit tests for `ml_playground.tools.core.interfaces`."""
# pyright: reportAttributeAccessIssue=false

from __future__ import annotations

from typing import Literal

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
        with pytest.raises(ValidationError, match="Invalid tools category"):
            OperationId(namespace="tools", category="invalid", command="lint")

    def test_operation_id_invalid_command_format_raises(self) -> None:
        with pytest.raises(ValidationError, match="Command must be alphanumeric"):
            OperationId(namespace="tools", category="ci", command="bad command!")

    def test_operation_id_ml_namespace_validates_categories(self) -> None:
        op = OperationId(namespace="ml", category="prepare", command="dataset")

        assert str(op) == "ml.prepare.dataset"

        with pytest.raises(ValidationError, match="Invalid ml category"):
            OperationId(namespace="ml", category="invalid", command="step")

    @pytest.mark.parametrize(  # type: ignore[attr-defined]
        ("namespace", "category"),
        [
            ("tools", "invalid"),
            ("ml", "not-valid"),
            ("ml", "invalid"),
        ],
    )
    def test_operation_id_invalid_category_branch(
        self, namespace: Literal["tools", "ml"], category: str
    ) -> None:
        with pytest.raises(ValidationError):
            OperationId(namespace=namespace, category=category, command="cmd")

    @pytest.mark.parametrize(  # type: ignore[attr-defined]
        "category",
        ["prepare", "train", "sample", "analyze"],
    )
    def test_operation_id_ml_namespace_valid_categories(self, category: str) -> None:
        op = OperationId(namespace="ml", category=category, command="step")
        assert op.category == category
        assert str(op) == f"ml.{category}.step"

    def test_tool_interface_concrete_and_base_contract(self) -> None:
        class DemoTool(ToolInterface):
            @property
            def category(self) -> str:
                return "ci"

            @property
            def command(self) -> str:
                return "run"

            @property
            def description(self) -> str:
                return "demo"

            def execute(
                self,
                args: list[str],
                *,
                learning_mode: bool = False,
                verbosity_level: int = 0,
                dry_run: bool = False,
            ) -> ToolResult:
                return ToolResult.create(
                    success=True,
                    exit_code=0,
                    namespace="tools",
                    category=self.category,
                    command=self.command,
                    stdout="ok",
                )

            def get_help(self) -> str:
                return "help"

            def validate_args(self, args: list[str]) -> None:
                return None

        tool = DemoTool()
        tool.validate_args([])
        result = tool.execute([])
        assert isinstance(result, ToolResult)
        assert result.operation_id.category == "ci"

        base = ToolInterface()
        with pytest.raises(NotImplementedError):
            _ = base.category
        with pytest.raises(NotImplementedError):
            _ = base.command
        with pytest.raises(NotImplementedError):
            _ = base.description
        with pytest.raises(NotImplementedError):
            base.execute([])
        with pytest.raises(NotImplementedError):
            base.get_help()
        with pytest.raises(NotImplementedError):
            base.validate_args([])


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
