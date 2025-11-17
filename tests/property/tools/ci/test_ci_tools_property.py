from __future__ import annotations

import tempfile
from pathlib import Path

import hypothesis.strategies as st
from hypothesis import given, settings

from ml_playground.tools.ci.ci import CITools
from ml_playground.tools.core.config import ToolsConfig
from tests.property.tools._helpers import DeterministicRunner


@settings(max_examples=25, deadline=None, derandomize=True)
@given(extra_args=st.lists(st.text(min_size=1, max_size=8), max_size=3))
def test_quality_gate_executes_precommit(extra_args: list[str]) -> None:
    """quality_gate should invoke pre-commit via uv run."""
    with tempfile.TemporaryDirectory() as temp_dir:
        tmp_path = Path(temp_dir)
        runner = DeterministicRunner()
        tools = CITools(ToolsConfig(), tmp_path, subprocess_runner=runner)

        result = tools.quality_gate(extra_args)

        assert result.success is True
        assert any(
            call.kind == "uv" and "pre-commit" in call.args for call in runner.calls
        )


@settings(max_examples=10, deadline=None, derandomize=True)
@given(st.just(()))
def test_coverage_badge_writes_svg(_: tuple[()]) -> None:
    """coverage_badge should emit an SVG file in the configured directory."""
    with tempfile.TemporaryDirectory() as temp_dir:
        root = Path(temp_dir)
        cache_dir = root / ".cache" / "coverage"
        cache_dir.mkdir(parents=True, exist_ok=True)
        (cache_dir / "coverage.json").write_text(
            '{"totals": {"percent_covered": 94.2}}',
            encoding="utf-8",
        )

        config = ToolsConfig()
        config.ci.badge_output_dir = Path("badges")
        runner = DeterministicRunner()
        tools = CITools(config, root, subprocess_runner=runner)

        result = tools.coverage_badge([])

        badge_file = root / config.ci.badge_output_dir / "coverage.svg"
        assert result.success is True
        assert badge_file.exists()
