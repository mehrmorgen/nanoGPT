from __future__ import annotations

from pathlib import Path
from tempfile import TemporaryDirectory

import pytest
from hypothesis import HealthCheck, given, settings, strategies as st
from pydantic import ValidationError

from ml_playground.framework.configuration.models import RuntimeConfig


@settings(
    max_examples=20,
    deadline=None,
    derandomize=True,
    suppress_health_check=[HealthCheck.function_scoped_fixture],
)
@given(  # type: ignore[reportAny]
    label=st.text(min_size=1, max_size=8)
)
def test_runtime_config_rejects_invalid_domain_label(label: str) -> None:
    """Invalid domain labels are rejected for checkpoint naming."""
    if all(ch in "abcdefghijklmnopqrstuvwxyz0123456789_" for ch in label):
        return
    with TemporaryDirectory() as tmp_dir:
        with pytest.raises(ValidationError):
            RuntimeConfig(
                out_dir=Path(tmp_dir),
                ckpt_naming_policy="domain",
                ckpt_domain_label=label,
            )


def test_runtime_config_requires_domain_label() -> None:
    """Missing domain labels are rejected for domain naming."""
    with TemporaryDirectory() as tmp_dir:
        with pytest.raises(ValidationError):
            RuntimeConfig(
                out_dir=Path(tmp_dir),
                ckpt_naming_policy="domain",
                ckpt_domain_label=None,
            )


def test_runtime_config_accepts_valid_domain_label() -> None:
    """Valid domain labels are accepted."""
    with TemporaryDirectory() as tmp_dir:
        cfg = RuntimeConfig(
            out_dir=Path(tmp_dir),
            ckpt_naming_policy="domain",
            ckpt_domain_label="games",
        )
        assert cfg.ckpt_domain_label == "games"
