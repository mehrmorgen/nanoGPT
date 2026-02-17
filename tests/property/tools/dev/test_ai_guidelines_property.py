"""Property-based tests for ai_guidelines module using Hypothesis."""

from __future__ import annotations

import tempfile
from pathlib import Path
from hypothesis import given, settings, strategies as st, HealthCheck

from ml_playground.tools.dev.ai_guidelines import (
    gitignore_match,
    is_listed_in_aiignore,
    relative_tool_path,
    project_path,
)


@settings(max_examples=50, deadline=500)
@given(  # type: ignore[reportAny]
    tool=st.text(
        min_size=1,
        alphabet=st.characters(
            whitelist_categories=("L", "Nd"), min_codepoint=48, max_codepoint=57
        ),
    ),
    path=st.text(
        min_size=1,
        alphabet=st.characters(
            whitelist_categories=("L", "Nd"), min_codepoint=48, max_codepoint=57
        ),
    ),
)
def test_relative_tool_path_basic(tool: str, path: str) -> None:
    """Test basic relative_tool_path functionality."""
    tool_path = Path(f".tool{tool}") / path
    relative = relative_tool_path(tool_path, Path(f"tool{tool}"))
    assert isinstance(relative, str)
    assert len(relative) > 0


@settings(max_examples=30, deadline=500)
@given(  # type: ignore[reportAny]
    path=st.text(
        min_size=1,
        alphabet=st.characters(
            whitelist_categories=("L", "Nd"), min_codepoint=48, max_codepoint=57
        ),
    ),
)
def test_is_listed_in_aiignore_basic(path: str) -> None:
    """Test basic is_listed_in_aiignore functionality."""
    with tempfile.TemporaryDirectory() as tmp_dir:
        project_dir = Path(tmp_dir)
        tool_dir = Path(path)
        is_listed = is_listed_in_aiignore(project_dir, tool_dir)
        assert not is_listed


@settings(
    max_examples=40,
    deadline=1000,
    suppress_health_check=[HealthCheck.function_scoped_fixture],
)
@given(  # type: ignore[reportAny]
    # Specifically include empty string and root to hit line 111 branch reliably
    relative_path=st.sampled_from(["", "/", "foo", "foo/bar"])
    | st.text(min_size=0, max_size=5),
    directory=st.just(True),
    # Include malformed pattern "[" to reliably trigger line 129 ValueError branch
    patterns=st.lists(
        st.text(min_size=1, max_size=12, alphabet="abcdefghijklmnopqrstuvwxyz*?[]/")
        | st.just("["),
        min_size=1,
        max_size=5,
    ),
)
def test_gitignore_match_comprehensive(
    tmp_path: Path, relative_path: str, directory: bool, patterns: list[str]
) -> None:
    """Cover gitignore_match branches including empty base and invalid patterns."""
    gitignore = tmp_path / ".gitignore"

    # Ensure malformed pattern is present to hit ValueError branch
    processed_patterns = list(patterns)
    if "[" not in processed_patterns:
        processed_patterns.append("[")

    content = "\n".join(processed_patterns)
    gitignore.write_text(content, encoding="utf-8")

    ignored, matched_pattern = gitignore_match(
        tmp_path, relative_path, directory=directory
    )

    assert isinstance(ignored, bool)
    if ignored:
        assert matched_pattern is not None


@settings(
    max_examples=30,
    deadline=500,
    suppress_health_check=[HealthCheck.function_scoped_fixture],
)
@given(  # type: ignore[reportAny]
    relative_path=st.text(
        alphabet=st.characters(
            whitelist_categories=("L", "Nd"), whitelist_characters="/._-"
        ),
        max_size=32,
    ),
)
def test_project_path_validation(tmp_path: Path, relative_path: str) -> None:
    """Cover project_path validation branches."""
    try:
        res = project_path(tmp_path, relative_path)
        assert res.is_absolute()
        assert tmp_path in res.parents or res == tmp_path
    except ValueError as e:
        msg = str(e)
        assert "must be project-relative" in msg or "parent directory references" in msg
