"""Property-based tests for CI tools."""

from __future__ import annotations

import json
from pathlib import Path
from typing import cast

import pytest
from hypothesis import HealthCheck, given, settings, strategies as st

from ml_playground.tools.ci import CITools
from ml_playground.tools.core.config import ToolsConfig
from ml_playground.tools.core.interfaces import OperationId
from tests.unit.tools.fakes import FakeSubprocessRunner, create_success_result


@pytest.fixture
def ci_tools(tmp_path: Path) -> CITools:
    """Create CITools instance with fake subprocess runner."""
    config = ToolsConfig()
    fake_runner = FakeSubprocessRunner()
    return CITools(config, tmp_path, subprocess_runner=fake_runner)


@settings(suppress_health_check=[HealthCheck.function_scoped_fixture])
@given(
    ruff_stdout=st.text(),
    ruff_stderr=st.text(),
    format_stdout=st.text(),
    format_stderr=st.text(),
    md_stdout=st.text(),
    md_stderr=st.text(),
)
def test_quality_fast_output_aggregation_property(
    ci_tools: CITools,
    ruff_stdout: str,
    ruff_stderr: str,
    format_stdout: str,
    format_stderr: str,
    md_stdout: str,
    md_stderr: str,
):
    """Property: quality_fast correctly aggregates stdout/stderr from all hooks."""
    fake_runner = cast(FakeSubprocessRunner, ci_tools._subprocess_runner)
    # Clear calls from previous hypothesis iterations
    fake_runner.calls.clear()

    operation_id = OperationId(namespace="tools", category="ci", command="quality-fast")

    ruff_res = create_success_result(operation_id, ruff_stdout)
    ruff_res.stderr = ruff_stderr

    format_res = create_success_result(operation_id, format_stdout)
    format_res.stderr = format_stderr

    md_res = create_success_result(operation_id, md_stdout)
    md_res.stderr = md_stderr

    fake_runner.set_results([ruff_res, format_res, md_res])

    result = ci_tools.quality_fast([])

    assert result.success is True

    # Check stdout aggregation
    if ruff_stdout:
        assert f"ruff:\n{ruff_stdout}" in result.stdout
    if format_stdout:
        assert f"ruff-format:\n{format_stdout}" in result.stdout
    if md_stdout:
        assert f"mdformat:\n{md_stdout}" in result.stdout

    # Check stderr aggregation
    if ruff_stderr:
        assert f"ruff warnings:\n{ruff_stderr}" in result.stderr
    if format_stderr:
        assert f"ruff-format warnings:\n{format_stderr}" in result.stderr
    if md_stderr:
        assert f"mdformat warnings:\n{md_stderr}" in result.stderr


@settings(suppress_health_check=[HealthCheck.function_scoped_fixture])
@given(
    percent=st.floats(
        min_value=0, max_value=100, allow_nan=False, allow_infinity=False
    ),
)
def test_coverage_badge_logic_property(
    ci_tools: CITools, tmp_path: Path, percent: float
):
    """Property: coverage_badge correctly determines color and includes percent in SVG."""
    coverage_dir = ci_tools.cache_dir / "coverage"
    coverage_dir.mkdir(parents=True, exist_ok=True)
    json_path = coverage_dir / "coverage.json"

    json_path.write_text(json.dumps({"totals": {"percent_covered": percent}}))

    result = ci_tools.coverage_badge([])
    assert result.success is True

    badge_file = (
        ci_tools.root_path / ci_tools.config.ci.badge_output_dir / "coverage.svg"
    )
    svg_content = badge_file.read_text()

    # Check color
    if percent >= 90:
        assert 'fill="brightgreen"' in svg_content
    elif percent >= 80:
        assert 'fill="green"' in svg_content
    elif percent >= 70:
        assert 'fill="yellowgreen"' in svg_content
    elif percent >= 60:
        assert 'fill="yellow"' in svg_content
    else:
        assert 'fill="red"' in svg_content

    # Check percent text
    assert f"{percent:.0f}%" in svg_content
