from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator

from ml_playground.tools.core.config import ToolsConfig
from ml_playground.tools.core.interfaces import ToolResult
from ml_playground.tools.dev import status as dev_status


@contextmanager
def override_attr(obj: object, name: str, value: Any) -> Iterator[None]:
    original = getattr(obj, name)
    object.__setattr__(obj, name, value)
    try:
        yield
    finally:
        object.__setattr__(obj, name, original)


def test_run_dev_batch_review_delegates_to_run_batch_review(tmp_path: Path) -> None:
    captured: list[dict[str, object]] = []

    def fake_run_batch_review(
        config: ToolsConfig,
        project_root_path: Path,
        output_format: str = "json",
        subprocess_runner: object | None = None,
    ) -> ToolResult:
        captured.append(
            {
                "config": config,
                "project_root_path": project_root_path,
                "output_format": output_format,
                "subprocess_runner": subprocess_runner,
            }
        )
        return ToolResult.create(
            success=True,
            exit_code=0,
            namespace="tools",
            category="dev",
            command="batch-review",
            stdout="ok",
        )

    cfg = ToolsConfig()
    with override_attr(dev_status, "run_batch_review", fake_run_batch_review):
        result = dev_status.run_dev_batch_review(cfg, tmp_path, output_format="yaml")

    assert result.success is True
    assert captured == [
        {
            "config": cfg,
            "project_root_path": tmp_path,
            "output_format": "yaml",
            "subprocess_runner": None,
        }
    ]


def test_run_dev_workflow_status_delegates_to_run_workflow_status(
    tmp_path: Path,
) -> None:
    captured: list[dict[str, object]] = []

    def fake_run_workflow_status(
        config: ToolsConfig,
        project_root_path: Path,
        output_format: str = "json",
        subprocess_runner: object | None = None,
    ) -> ToolResult:
        captured.append(
            {
                "config": config,
                "project_root_path": project_root_path,
                "output_format": output_format,
                "subprocess_runner": subprocess_runner,
            }
        )
        return ToolResult.create(
            success=True,
            exit_code=0,
            namespace="tools",
            category="dev",
            command="workflow-status",
            stdout="ok",
        )

    cfg = ToolsConfig()
    with override_attr(dev_status, "run_workflow_status", fake_run_workflow_status):
        result = dev_status.run_dev_workflow_status(cfg, tmp_path, output_format="json")

    assert result.success is True
    assert captured == [
        {
            "config": cfg,
            "project_root_path": tmp_path,
            "output_format": "json",
            "subprocess_runner": None,
        }
    ]
