from __future__ import annotations

import json
from contextlib import contextmanager
from typing import Any, Iterator

import hypothesis.strategies as st
from hypothesis import HealthCheck, given, settings

import ml_playground.tools.dev.batch_review as batch_review_module
from ml_playground.tools.core.config import ToolsConfig


@contextmanager
def override_attr(obj: object, name: str, value: Any) -> Iterator[None]:
    original = getattr(obj, name)
    setattr(obj, name, value)
    try:
        yield
    finally:
        setattr(obj, name, original)


FORMAT_STRATEGY = st.sampled_from(["json", "text"])


def _build_summary(success: bool) -> dict[str, Any]:
    status = "passed" if success else "failed"
    return {
        "overall": {"status": status, "success": success},
        "summary": {"status": status},
    }


@settings(
    max_examples=40,
    deadline=None,
    derandomize=True,
    suppress_health_check=[HealthCheck.function_scoped_fixture],
)
@given(
    quality_success=st.booleans(),
    test_success=st.booleans(),
    output_format=FORMAT_STRATEGY,
)
def test_run_batch_review_overall_success_matches_components(
    quality_success: bool,
    test_success: bool,
    output_format: str,
    tmp_path,
) -> None:
    quality_summary = _build_summary(quality_success)
    test_summary = _build_summary(test_success)

    with override_attr(
        batch_review_module, "_run_quality_batch", lambda *_args, **_kwargs: quality_summary
    ), override_attr(
        batch_review_module, "_run_test_batch", lambda *_args, **_kwargs: test_summary
    ), override_attr(batch_review_module, "_get_timestamp", lambda: "ts"):
        result = batch_review_module.run_batch_review(
            ToolsConfig(), tmp_path, output_format=output_format
        )

    expected_success = quality_success and test_success
    assert result.success is expected_success
    assert result.exit_code == (0 if expected_success else 1)

    if output_format == "json":
        payload = json.loads(result.stdout)
        assert payload["quality_checks"] == quality_summary
        assert payload["test_summary"] == test_summary
        assert payload["overall_status"]["success"] is expected_success
    else:
        text_output = result.stdout
        assert "Batch Review Results" in text_output
        marker = "✓ PASSED" if expected_success else "✗ FAILED"
        assert marker in text_output
