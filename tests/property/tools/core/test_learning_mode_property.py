from __future__ import annotations

import hypothesis.strategies as st
from hypothesis import given, settings

from typing import Literal

from ml_playground.tools.core.interfaces import LearningInfo, ToolResult
from ml_playground.tools.core.learning_mode import LearningModeEngine, VerbosityLevel


def _build_tool_result(
    ns: Literal["tools", "ml"],
    cat: str,
    cmd: str,
    ok: bool,
    code: int,
    out: str,
    err: str,
    li: LearningInfo,
    vb: VerbosityLevel,
) -> tuple[ToolResult, VerbosityLevel]:
    return (
        ToolResult.create(
            success=ok,
            exit_code=code,
            namespace=ns,
            category=cat,
            command=cmd,
            stdout=out,
            stderr=err,
            learning_info=li,
        ),
        vb,
    )


def _tool_result_strategy() -> st.SearchStrategy[tuple[ToolResult, VerbosityLevel]]:
    namespace: st.SearchStrategy[Literal["tools", "ml"]] = st.sampled_from(
        ["tools", "ml"]
    )

    tools_categories: st.SearchStrategy[str] = st.sampled_from(
        ["ci", "quality", "test", "env", "agentic", "dev", "utils"]
    )
    ml_categories: st.SearchStrategy[str] = st.sampled_from(
        ["prepare", "train", "sample", "analyze"]
    )

    # OperationId validation allows alnum plus hyphen/underscore.
    command = st.text(
        alphabet=st.characters(
            whitelist_categories=("Ll", "Lu", "Nd"), whitelist_characters="-_"
        ),
        min_size=1,
        max_size=20,
    ).filter(lambda value: value.replace("-", "").replace("_", "").isalnum())

    stdout = st.text(min_size=0, max_size=200)
    stderr = st.text(min_size=0, max_size=200)

    learning_info = st.builds(
        LearningInfo,
        commands_executed=st.lists(st.text(min_size=1, max_size=40), max_size=5),
        explanations=st.lists(st.text(min_size=1, max_size=80), max_size=5),
        best_practices=st.lists(st.text(min_size=1, max_size=80), max_size=5),
        related_concepts=st.lists(st.text(min_size=1, max_size=80), max_size=5),
    )

    verbosity = st.sampled_from(
        [VerbosityLevel.MINIMAL, VerbosityLevel.STANDARD, VerbosityLevel.COMPREHENSIVE]
    )

    def _for_namespace(
        ns: Literal["tools", "ml"],
    ) -> st.SearchStrategy[tuple[ToolResult, VerbosityLevel]]:
        cat_strategy: st.SearchStrategy[str] = (
            tools_categories if ns == "tools" else ml_categories
        )
        return st.builds(
            _build_tool_result,
            ns=st.just(ns),
            cat=cat_strategy,
            cmd=command,
            ok=st.booleans(),
            code=st.integers(min_value=0, max_value=255),
            out=stdout,
            err=stderr,
            li=learning_info,
            vb=verbosity,
        )

    return namespace.flatmap(_for_namespace)


@settings(max_examples=80, deadline=None, derandomize=True)
@given(data=_tool_result_strategy(), learning_enabled=st.booleans())
def test_format_output_always_returns_non_empty_header(
    data: tuple[ToolResult, VerbosityLevel], learning_enabled: bool
) -> None:
    result, verbosity = data
    engine = LearningModeEngine(verbosity)

    text = engine.format_output(result, learning_enabled=learning_enabled)

    assert isinstance(text, str)
    assert str(result.operation_id) in text
    assert ("completed successfully" in text) or ("failed" in text)


@settings(max_examples=80, deadline=None, derandomize=True)
@given(data=_tool_result_strategy(), learning_enabled=st.booleans())
def test_format_output_includes_stdout_stderr_only_when_present(
    data: tuple[ToolResult, VerbosityLevel], learning_enabled: bool
) -> None:
    result, verbosity = data
    engine = LearningModeEngine(verbosity)

    text = engine.format_output(result, learning_enabled=learning_enabled)

    if result.stdout.strip():
        assert "Output:" in text
        assert result.stdout.strip() in text
    else:
        assert "\nOutput:\n" not in text

    if result.stderr.strip():
        assert "Errors:" in text
        assert result.stderr.strip() in text
    else:
        assert "\nErrors:\n" not in text


@settings(max_examples=80, deadline=None, derandomize=True)
@given(data=_tool_result_strategy(), learning_enabled=st.booleans())
def test_format_output_learning_sections_gated(
    data: tuple[ToolResult, VerbosityLevel], learning_enabled: bool
) -> None:
    result, verbosity = data
    engine = LearningModeEngine(verbosity)

    text = engine.format_output(result, learning_enabled=learning_enabled)

    has_any_learning = bool(
        result.learning_info.commands_executed
        or result.learning_info.explanations
        or result.learning_info.best_practices
        or result.learning_info.related_concepts
    )

    if learning_enabled and has_any_learning:
        # At least one learning section header should be present.
        assert (
            "Commands executed:" in text
            or "Explanation:" in text
            or "Best practices:" in text
            or "Related concepts:" in text
        )
    else:
        assert "Commands executed:" not in text
        assert "Explanation:" not in text
        assert "Best practices:" not in text
        assert "Related concepts:" not in text
