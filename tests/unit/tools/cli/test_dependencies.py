from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

from ml_playground.tools.cli.dependencies import (
    ToolsDependencies,
    default_tools_dependencies,
    get_tools_dependencies,
    override_tools_dependencies,
)
from ml_playground.tools.core.config import ToolsConfig
from ml_playground.tools.utils.subprocess_utils import RealSubprocessRunner


def test_default_dependencies_return_noop_factories() -> None:
    deps = default_tools_dependencies()

    assert callable(deps.load_config)
    config = deps.load_config(None)
    assert isinstance(config, ToolsConfig)

    tool = deps.dev_factory()
    assert isinstance(tool, SimpleNamespace)
    assert isinstance(tool.subprocess_runner, RealSubprocessRunner)


def test_override_dependencies_restores_previous_stack() -> None:
    baseline = get_tools_dependencies()

    calls: list[str] = []

    def _loader(_: Path | None) -> ToolsConfig:
        calls.append("load")
        return ToolsConfig()

    custom = ToolsDependencies(
        load_config=_loader,
        quality_factory=lambda: SimpleNamespace(name="quality"),
        testing_factory=lambda: SimpleNamespace(name="testing"),
        environment_factory=lambda: SimpleNamespace(name="environment"),
        ci_factory=lambda: SimpleNamespace(name="ci"),
        dev_factory=lambda: SimpleNamespace(name="dev"),
        result_handler=lambda result: calls.append(
            f"result:{getattr(result, 'command', '<unknown>')}"
        ),
    )

    with override_tools_dependencies(custom):
        active = get_tools_dependencies()
        assert active is custom
        cfg = active.load_config(None)
        assert isinstance(cfg, ToolsConfig)
        assert calls == ["load"]

    assert get_tools_dependencies() == baseline
