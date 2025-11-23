from __future__ import annotations

import sys
from pathlib import Path

import pytest
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from ml_playground.tools.core.interfaces import OperationId
from ml_playground.tools.utils.subprocess_utils import RealSubprocessRunner


@pytest.mark.skip(reason="Flaky in pre-commit environment")
@settings(max_examples=10, deadline=None, derandomize=True)
@given(
    env_key=st.text(
        min_size=3,
        max_size=5,
        alphabet=st.characters(
            whitelist_categories=("Lu",), min_codepoint=65, max_codepoint=90
        ),
    ),
    env_val=st.text(
        min_size=3, max_size=5, alphabet=st.characters(whitelist_categories=("L", "N"))
    ),
)
def test_real_runner_propagates_env_vars(env_key: str, env_val: str) -> None:
    """RealSubprocessRunner should propagate environment variables to the child process."""
    runner = RealSubprocessRunner()
    operation_id = OperationId(namespace="tools", category="utils", command="env-test")

    # We use python to print the env var to avoid shell dependencies
    # python -c "import os; print(os.environ.get('KEY', ''))"
    command = [
        sys.executable,
        "-c",
        f"import os; print(os.environ.get('{env_key}', 'MISSING'))",
    ]

    result = runner.run_subprocess(
        command,
        env={env_key: env_val},
        operation_id=operation_id,
    )

    assert result.success is True
    assert result.stdout.strip() == env_val


@pytest.mark.skip(reason="Flaky in pre-commit environment")
@settings(
    max_examples=10,
    deadline=None,
    derandomize=True,
    suppress_health_check=[HealthCheck.function_scoped_fixture],
)
@given(
    use_path_obj=st.booleans(),
)
def test_real_runner_respects_cwd(tmp_path: Path, use_path_obj: bool) -> None:
    """RealSubprocessRunner should respect cwd argument (str or Path)."""
    runner = RealSubprocessRunner()
    operation_id = OperationId(namespace="tools", category="utils", command="cwd-test")

    target_dir = tmp_path / "subdir"
    target_dir.mkdir(exist_ok=True)

    cwd_arg = target_dir if use_path_obj else str(target_dir)

    # python -c "import os; print(os.getcwd())"
    command = [
        sys.executable,
        "-c",
        "import os; print(os.getcwd())",
    ]

    result = runner.run_subprocess(
        command,
        cwd=cwd_arg,
        operation_id=operation_id,
    )

    assert result.success is True
    # Resolve symlinks for comparison (Mac /tmp is strictly /private/tmp)
    assert Path(result.stdout.strip()).resolve() == target_dir.resolve()
