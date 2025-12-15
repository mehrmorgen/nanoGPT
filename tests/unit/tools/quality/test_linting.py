from __future__ import annotations

from pathlib import Path

import ml_playground.tools.core.config as config_module
import ml_playground.tools.quality.linting as linting
from ml_playground.tools.core.config import ToolsConfig
from ml_playground.tools.core.interfaces import OperationId
from tests.unit.tools.fakes import FakeSubprocessRunner, create_success_result


def test_run_lint_check_delegates_to_run_lint(tmp_path: Path) -> None:
    cfg = ToolsConfig(
        quality=config_module.QualityToolsConfig(
            timeout=120,
            ruff_config_path=Path("pyproject.toml"),
        )
    )
    runner = FakeSubprocessRunner()
    op_id = OperationId(namespace="tools", category="quality", command="lint")
    runner.set_results([create_success_result(op_id, "ok")])

    result = linting.run_lint_check(
        config=cfg,
        root_path=tmp_path,
        args=[],
        subprocess_runner=runner,
        learning_mode=False,
        verbosity_level=1,
    )

    assert result.success is True
    assert runner.calls
    assert runner.calls[0]["operation_id"].command == "lint"
