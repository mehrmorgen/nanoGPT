from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator

import ml_playground.tools.cli.commands.analysis as analysis_commands
import ml_playground.tools.analysis.sample_quality as sample_quality_module
from ml_playground.tools.core.interfaces import ToolResult


@contextmanager
def override_attr(obj: object, name: str, value: Any) -> Iterator[None]:
    original = getattr(obj, name)
    setattr(obj, name, value)
    try:
        yield
    finally:
        setattr(obj, name, original)


def test_sample_quality_oserror_branch_is_stable(tmp_path: Path) -> None:
    results: list[ToolResult] = []

    def fake_run_tool_command(command_func: Any, *args: Any, **kwargs: Any) -> None:
        result = command_func(*args, **kwargs)
        results.append(result)

    def fake_analyze_sample_file(_path: Path) -> object:
        raise OSError("boom")

    def fake_format_analysis(_analysis: object) -> str:
        return "unused"

    sample_path = tmp_path / "sample.txt"
    sample_path.write_text("x", encoding="utf-8")

    with (
        override_attr(analysis_commands, "run_tool_command", fake_run_tool_command),
        override_attr(
            sample_quality_module,
            "analyze_sample_file",
            fake_analyze_sample_file,
        ),
        override_attr(sample_quality_module, "format_analysis", fake_format_analysis),
    ):
        analysis_commands.sample_quality(sample_path)

    assert results, "Expected ToolResult to be captured"
    result = results[-1]
    assert result.success is False
    assert result.exit_code == 1
    assert result.operation_id.category == "analysis"
    assert result.operation_id.command == "sample-quality"
    assert "Failed to read sample file" in (result.stderr or "")
    assert "(OSError)" in (result.stderr or "")
