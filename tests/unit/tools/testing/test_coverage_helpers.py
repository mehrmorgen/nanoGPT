"""Coverage helper unit tests for TestingTools.

Covers thresholds parsing, manifest reading, metrics collection,
undercovered file computation, and tree formatting.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from ml_playground.tools import testing as testing_module
from ml_playground.tools.core import config as config_module
from ml_playground.tools.core.config import ToolsConfig
from ml_playground.tools.core.errors import ToolExecutionError
from ml_playground.tools.core.interfaces import OperationId
from tests.unit.tools.fakes import FakeSubprocessRunner, RecordingRunner


@pytest.fixture
def config() -> ToolsConfig:
    return ToolsConfig(
        testing=config_module.TestToolsConfig(
            timeout=300,
            coverage_threshold=80.0,
            parallel_workers=2,
        )
    )


@pytest.fixture
def subprocess_runner() -> FakeSubprocessRunner:
    return FakeSubprocessRunner()


def test_read_coverage_thresholds_from_config(
    config: ToolsConfig, tmp_path: Path, subprocess_runner: FakeSubprocessRunner
) -> None:
    pyproject = tmp_path / "pyproject.toml"
    pyproject.write_text(
        (
            """
[tool.ml_playground.coverage.thresholds]
line_threshold = 91.5
branch_threshold = 73.0
"""
        ).strip(),
        encoding="utf-8",
    )

    tools = testing_module.TestingTools(config, tmp_path, subprocess_runner)

    thresholds = tools._read_coverage_thresholds_from_config()

    assert thresholds["line_threshold"] == 91.5
    assert thresholds["branch_threshold"] == 73.0


def test_read_coverage_thresholds_missing_returns_empty(
    config: ToolsConfig, tmp_path: Path, subprocess_runner: FakeSubprocessRunner
) -> None:
    tools = testing_module.TestingTools(config, tmp_path, subprocess_runner)

    assert tools._read_coverage_thresholds_from_config() == {}


def test_read_coverage_manifest_handles_invalid_json(
    config: ToolsConfig, tmp_path: Path, subprocess_runner: FakeSubprocessRunner
) -> None:
    tools = testing_module.TestingTools(config, tmp_path, subprocess_runner)

    manifest_path = tools._coverage_manifest_path()
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text("not-json", encoding="utf-8")

    assert tools._read_coverage_manifest() is None


def test_collect_coverage_metrics_missing_json_raises(
    config: ToolsConfig, tmp_path: Path
) -> None:
    runner = FakeSubprocessRunner()
    tools = testing_module.TestingTools(config, tmp_path, runner)

    coverage_dir = tools._coverage_file().parent
    coverage_dir.mkdir(parents=True, exist_ok=True)

    with pytest.raises(ToolExecutionError) as excinfo:
        tools._collect_coverage_metrics(
            env={},
            operation_id=OperationId(
                namespace="tools", category="test", command="metrics"
            ),
            executed_commands=[],
        )

    assert "Coverage JSON file missing" in str(excinfo.value)
    assert any("coverage" in call["command"] for call in runner.calls)


def test_collect_coverage_metrics_invalid_json_raises(
    config: ToolsConfig, tmp_path: Path
) -> None:
    runner = RecordingRunner()
    tools = testing_module.TestingTools(config, tmp_path, runner)

    coverage_json = tools._coverage_file().parent / "coverage.json"
    coverage_json.parent.mkdir(parents=True, exist_ok=True)
    coverage_json.write_text("not-json", encoding="utf-8")

    with pytest.raises(ToolExecutionError) as excinfo:
        tools._collect_coverage_metrics(
            env={},
            operation_id=OperationId(
                namespace="tools", category="test", command="metrics"
            ),
            executed_commands=[],
        )

    assert "Failed to parse coverage JSON" in str(excinfo.value)


def test_collect_coverage_metrics_success_reports_totals(
    config: ToolsConfig, tmp_path: Path
) -> None:
    from tests.unit.tools.categories.test_testing import RecordingRunner

    runner = RecordingRunner()
    tools = testing_module.TestingTools(config, tmp_path, runner)

    coverage_dir = tools._coverage_file().parent
    coverage_dir.mkdir(parents=True, exist_ok=True)
    coverage_json = coverage_dir / "coverage.json"
    coverage_json.write_text(
        json.dumps(
            {
                "totals": {
                    "num_statements": 20,
                    "covered_lines": 18,
                    "num_branches": 4,
                    "covered_branches": 3,
                }
            }
        ),
        encoding="utf-8",
    )

    result, lines = tools._collect_coverage_metrics(
        env={},
        operation_id=OperationId(namespace="tools", category="test", command="metrics"),
        executed_commands=[],
    )

    assert result is None
    assert lines[0] == "Coverage totals: lines=90.00% (18/20)"
    assert lines[1] == "Branch totals: branches=75.00% (3/4)"


def test_collect_coverage_metrics_missing_totals_raises(
    config: ToolsConfig, tmp_path: Path
) -> None:
    from tests.unit.tools.categories.test_testing import RecordingRunner

    runner = RecordingRunner()
    tools = testing_module.TestingTools(config, tmp_path, runner)

    coverage_dir = tools._coverage_file().parent
    coverage_dir.mkdir(parents=True, exist_ok=True)
    coverage_json = coverage_dir / "coverage.json"
    coverage_json.write_text(json.dumps({"files": {}}), encoding="utf-8")

    with pytest.raises(ToolExecutionError) as excinfo:
        tools._collect_coverage_metrics(
            env={},
            operation_id=OperationId(
                namespace="tools", category="test", command="metrics"
            ),
            executed_commands=[],
        )

    assert "Failed to parse coverage JSON" in str(excinfo.value)


def test_collect_undercovered_files_uses_percent_display(
    config: ToolsConfig, tmp_path: Path
) -> None:
    tools = testing_module.TestingTools(config, tmp_path, FakeSubprocessRunner())

    coverage_data = {
        "files": {
            "pkg/module.py": {
                "summary": {
                    "percent_covered": None,
                    "percent_covered_display": "87.5",
                    "num_branches": 4,
                    "covered_branches": 3,
                }
            },
            "pkg/fully_covered.py": {
                "summary": {
                    "percent_covered": 100,
                    "num_branches": 0,
                    "covered_branches": 0,
                }
            },
        }
    }

    entries = tools._collect_undercovered_files(coverage_data)

    assert entries == [("pkg/module.py", 87.5, 75.0)]


def test_format_undercovered_tree_outputs_hierarchy(
    config: ToolsConfig, tmp_path: Path
) -> None:
    tools = testing_module.TestingTools(config, tmp_path, FakeSubprocessRunner())

    entries = [
        ("pkg/module.py", 87.5, 75.0),
        ("pkg/sub/inner.py", 50.0, None),
    ]
    tree = tools._format_undercovered_tree(entries)

    assert tree == [
        "└── pkg/",
        "    ├── sub/",
        "    │   └── inner.py: line = 50.00%",
        "    └── module.py: line = 87.50% branch = 75.00%",
    ]


def test_compute_fingerprint_skips_unstatable_files(
    config: ToolsConfig, tmp_path: Path, subprocess_runner: FakeSubprocessRunner
) -> None:
    tools = testing_module.TestingTools(config, tmp_path, subprocess_runner)

    source_dir = tmp_path / "src" / "ml_playground" / "tools"
    source_dir.mkdir(parents=True, exist_ok=True)

    stable_file = source_dir / "stable.py"
    stable_file.write_text("value = 1", encoding="utf-8")

    broken_link = source_dir / "broken.py"
    broken_target = source_dir / "missing.py"
    broken_link.symlink_to(broken_target)

    fingerprint_with_broken = tools._compute_coverage_fingerprint()

    broken_link.unlink()

    fingerprint_without_broken = tools._compute_coverage_fingerprint()

    assert fingerprint_with_broken == fingerprint_without_broken
