from __future__ import annotations

from typing import cast
from pathlib import Path

import pytest
from hypothesis import given, settings, strategies as st

from ml_playground.framework.configuration.models import RuntimeConfig, DTypeKind


@settings(max_examples=10, deadline=None, derandomize=True)
@given(  # type: ignore[reportAny]
    compile_flag=st.booleans()
)
def test_runtime_config_when_mps_compile_then_raises(compile_flag: bool) -> None:
    """Runtime config when mps compile then raises."""
    if not compile_flag:
        return
    with pytest.raises(ValueError):
        RuntimeConfig(out_dir=Path("/tmp"), device="mps", compile=compile_flag)


def test_runtime_config_when_mps_float16_then_raises() -> None:
    """Runtime config when mps float16 then raises."""
    with pytest.raises(ValueError):
        RuntimeConfig(out_dir=Path("/tmp"), device="mps", dtype="float16")


@settings(max_examples=10, deadline=None, derandomize=True)
@given(  # type: ignore[reportAny]
    dtype=st.sampled_from(["float32", "bfloat16", "float16"])
)
def test_runtime_config_when_cuda_dtype_then_allows(dtype: str) -> None:
    """Runtime config when cuda dtype then allows."""
    cfg = RuntimeConfig(
        out_dir=Path("/tmp"), device="cuda", dtype=cast(DTypeKind, dtype)
    )
    assert cfg.device == "cuda"
