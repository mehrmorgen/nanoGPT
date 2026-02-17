"""Unit tests targeting the public coverage helper utilities."""

from __future__ import annotations

from pathlib import Path

from ml_playground.tools.testing import coverage_helpers as helpers


def test_read_coverage_manifest_handles_missing_and_invalid(tmp_path: Path) -> None:
    manifest = tmp_path / "manifest.json"
    assert helpers.read_coverage_manifest(manifest) is None

    manifest.write_text("not-json", encoding="utf-8")
    assert helpers.read_coverage_manifest(manifest) is None


def test_write_coverage_manifest_round_trip(tmp_path: Path) -> None:
    manifest = tmp_path / "manifest.json"
    helpers.write_coverage_manifest(manifest, fingerprint="abc123")

    loaded = helpers.read_coverage_manifest(manifest)
    assert loaded == {"fingerprint": "abc123"}


def test_clean_pytest_output_filters_progress_lines() -> None:
    raw = """
    test session starts
    collecting ...
    ..
    PASSED
    summary line
    """.strip()

    cleaned = helpers.clean_pytest_output(raw)
    # Function filters standalone progress lines (..) but keeps other content
    assert "test session" in cleaned
    assert "collecting ..." in cleaned  # ".." in text remains
    assert "    ..\n" not in cleaned  # standalone progress line removed
    assert "PASSED" in cleaned
    assert "summary line" in cleaned


def test_clean_pytest_output_filters_blank_and_xdist_lines() -> None:
    raw = """

    bringing up nodes...
    test session starts
    .
    """.strip("\n")

    cleaned = helpers.clean_pytest_output(raw)

    assert "bringing up nodes" not in cleaned
    assert "test session starts" in cleaned


def test_collect_undercovered_files_uses_display_values() -> None:
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

    entries = helpers.collect_undercovered_files(coverage_data)
    assert entries == [("pkg/module.py", 87.5, 75.0, 0)]


def test_collect_undercovered_files_skips_invalid_or_missing_percent() -> None:
    coverage_data = {
        "files": {
            "pkg/bad_display.py": {
                "summary": {
                    "percent_covered": None,
                    "percent_covered_display": "not-a-float",
                }
            },
            "pkg/missing_percent.py": {"summary": {}},
        }
    }

    assert helpers.collect_undercovered_files(coverage_data) == []


def test_collect_undercovered_files_branch_percent_handles_errors() -> None:
    coverage_data = {
        "files": {
            "pkg/type_error.py": {
                "summary": {
                    "percent_covered": 50.0,
                    "num_branches": 2,
                    "covered_branches": None,
                }
            },
            "pkg/zero_division.py": {
                "summary": {
                    "percent_covered": 50.0,
                    "num_branches": 1,
                    "covered_branches": 0,
                }
            },
        }
    }

    entries = helpers.collect_undercovered_files(coverage_data)
    assert entries == [
        ("pkg/type_error.py", 50.0, None, 0),
        ("pkg/zero_division.py", 50.0, 0.0, 0),
    ]


def test_format_undercovered_tree_outputs_hierarchy() -> None:
    entries: list[tuple[str, float, float | None]] = [
        ("pkg/module.py", 87.5, 75.0),
        ("pkg/sub/inner.py", 50.0, None),
    ]
    tree = helpers.format_undercovered_tree(entries)

    assert tree == [
        "└── pkg/",
        "    ├── sub/",
        "    │   └── inner.py: line = 50.00% loc = 0",
        "    └── module.py: line = 87.50% branch = 75.00% loc = 0",
    ]


def test_format_command_quotes_arguments() -> None:
    command = ["pytest", "tests/unit", "-k", "spaces inside"]
    formatted = helpers.format_command(command)
    assert "pytest" in formatted
    assert "'spaces inside'" in formatted


def test_format_tool_invocation_respects_prefix() -> None:
    formatted = helpers.format_tool_invocation("unit", ["-k", "demo"], prefix="")
    assert formatted == "Executed: tools test unit -k demo"


def test_format_coverage_status_variants() -> None:
    ok = helpers.format_coverage_status(
        metric="Line", percentage=91.23, threshold=90.0, passed=True
    )
    fail = helpers.format_coverage_status(
        metric="Branch", percentage=65.0, threshold=70.0, passed=False
    )

    assert "SUCCESS" in ok and ">=" in ok
    assert "FAILURE" in fail and "<" in fail


def test_compute_coverage_fingerprint_skips_missing_files(tmp_path: Path) -> None:
    source_dir = tmp_path / "src" / "ml_playground" / "tools"
    source_dir.mkdir(parents=True, exist_ok=True)

    stable_file = source_dir / "stable.py"
    stable_file.write_text("value = 1", encoding="utf-8")

    broken_link = source_dir / "broken.py"
    broken_target = source_dir / "missing.py"
    broken_link.symlink_to(broken_target)

    first = helpers.compute_coverage_fingerprint(tmp_path)
    broken_link.unlink()
    second = helpers.compute_coverage_fingerprint(tmp_path)

    assert first == second


def test_format_undercovered_tree_can_render_multiple_roots() -> None:
    entries: list[tuple[str, float, float | None]] = [
        ("pkg/file_a.py", 80.0, None),
        ("other/file_b.py", 75.0, 50.0),
    ]
    tree = helpers.format_undercovered_tree(entries)

    assert any("pkg/" in line for line in tree)
    assert any("other/" in line for line in tree)


def test_expected_suite_for_path_matches_framework() -> None:
    hint = helpers.expected_suite_for_path(
        "src/ml_playground/framework/core/runtime_context.py"
    )
    assert "tests/unit" in hint and "tests/property" in hint


def test_normalize_coverage_path_handles_repo_relative(tmp_path: Path) -> None:
    target = tmp_path / "src" / "ml_playground" / "framework" / "core" / "sample.py"
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text("print('ok')", encoding="utf-8")

    normalized = helpers.normalize_coverage_path(str(target), tmp_path)
    assert normalized == "src/ml_playground/framework/core/sample.py"


def test_format_coverage_map_includes_suite_hint(tmp_path: Path) -> None:
    path = tmp_path / "src" / "ml_playground" / "tools" / "foo.py"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("print('ok')", encoding="utf-8")

    lines = helpers.format_coverage_map(
        [(str(path), 75.0, None)],
        tmp_path,
    )
    assert "expected=tests/unit/tools" in lines[0]
