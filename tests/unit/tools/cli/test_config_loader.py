"""Unit tests for ml_playground.tools.cli.config_loader."""

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from typing import Any, Iterator

import pytest
import typer

from ml_playground.tools.cli import config_loader
from ml_playground.tools.cli.config_loader import (
    ensure_config_loaded,
    load_config_with_error_handling,
)
from ml_playground.tools.cli.state import state
from ml_playground.tools.core.config import ToolsConfig
from ml_playground.tools.core.errors import ToolConfigurationError


@pytest.fixture
def _reset_cli_state() -> Iterator[None]:  # pyright: ignore[reportUnusedFunction]
    original = state.__dict__.copy()
    try:
        state.reset()
        yield
    finally:
        # Restore all attributes to their original values to avoid bleed-over.
        state.__dict__.update(original)


class _FakeQualityConfig(SimpleNamespace):
    """Fake QualityToolsConfig that satisfies the protocol."""

    enabled: bool = True
    timeout: int = 300
    environment_vars: dict[str, str] = {}


class _FakeTestConfig(SimpleNamespace):
    """Fake TestToolsConfig that satisfies the protocol."""

    enabled: bool = True
    timeout: int = 300
    environment_vars: dict[str, str] = {}


class _FakeEnvironmentConfig(SimpleNamespace):
    """Fake EnvironmentToolsConfig that satisfies the protocol."""

    enabled: bool = True
    timeout: int = 300
    environment_vars: dict[str, str] = {}


class _FakeCIConfig(SimpleNamespace):
    """Fake CIToolsConfig that satisfies the protocol."""

    enabled: bool = True
    timeout: int = 300
    environment_vars: dict[str, str] = {}


class _FakeConfig(SimpleNamespace):
    """Fake config that implements ToolsConfigLike protocol."""

    learning_mode_default: bool
    default_verbosity: int

    # Add required protocol attributes with proper types
    quality: _FakeQualityConfig
    testing: _FakeTestConfig
    environment: _FakeEnvironmentConfig
    ci: _FakeCIConfig

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        # Ensure required protocol attributes exist with proper types
        self.quality = getattr(self, "quality", _FakeQualityConfig())
        self.testing = getattr(self, "testing", _FakeTestConfig())
        self.environment = getattr(self, "environment", _FakeEnvironmentConfig())
        self.ci = getattr(self, "ci", _FakeCIConfig())


def test_load_config_with_error_handling_tool_config_error(
    tmp_path: Path, _reset_cli_state: None
) -> None:
    """ToolConfigurationError should be echoed and cause typer.Exit(1).

    This exercises the ToolConfigurationError-specific branch.
    """

    class FailingDeps:
        """Fake dependencies that implements ToolsDependencies protocol."""

        def load_config(self, root: Path | None) -> ToolsConfig:
            raise ToolConfigurationError(
                "test config error",
                reason="invalid config",
                rationale="test failure",
            )

    with pytest.raises(typer.Exit):
        load_config_with_error_handling(project_root=tmp_path, deps=FailingDeps())  # type: ignore[arg-type]


def test_load_config_with_error_handling_updates_state_when_loading(
    tmp_path: Path, _reset_cli_state: None
) -> None:
    cfg = _FakeConfig(
        learning_mode_default=True,
        default_verbosity=2,
    )

    class TrackingDeps:
        """Fake dependencies that implements ToolsDependencies protocol."""

        def __init__(self) -> None:
            self.calls: list[Path | None] = []

        def load_config(self, root: Path | None) -> ToolsConfig:
            self.calls.append(root)
            return cfg  # type: ignore[return-value]

        # Add other required methods for ToolsDependencies protocol
        def quality_factory(self, config: ToolsConfig, project_root: Path) -> Any:
            return None

        def testing_factory(self, config: ToolsConfig, project_root: Path) -> Any:
            return None

        def environment_factory(self, config: ToolsConfig, project_root: Path) -> Any:
            return None

        def ci_factory(self, config: ToolsConfig, project_root: Path) -> Any:
            return None

        def dev_factory(
            self, config: ToolsConfig, project_root: Path | None = None
        ) -> Any:
            return None

        def result_handler(self, result: Any) -> None:
            pass

    deps = TrackingDeps()
    load_config_with_error_handling(project_root=tmp_path, deps=deps)  # type: ignore[arg-type]

    assert deps.calls == [tmp_path]
    assert state.config is cfg  # pyright: ignore[reportAttributeAccessIssue]
    assert state.project_root == tmp_path
    assert state.learning_mode is True
    assert state.learning_mode_set is True
    assert state.verbosity == cfg.default_verbosity


def test_load_config_with_error_handling_does_not_override_explicit_learning_mode(
    tmp_path: Path, _reset_cli_state: None
) -> None:
    cfg = _FakeConfig(
        learning_mode_default=True,
        default_verbosity=2,
    )

    state.learning_mode = False
    state.mark_learning_mode_explicit(True)

    class TrackingDeps:
        def load_config(self, root: Path | None) -> ToolsConfig:
            assert root == tmp_path
            return cfg  # type: ignore[return-value]

        def quality_factory(self, config: ToolsConfig, project_root: Path) -> Any:
            return None

        def testing_factory(self, config: ToolsConfig, project_root: Path) -> Any:
            return None

        def environment_factory(self, config: ToolsConfig, project_root: Path) -> Any:
            return None

        def ci_factory(self, config: ToolsConfig, project_root: Path) -> Any:
            return None

        def dev_factory(
            self, config: ToolsConfig, project_root: Path | None = None
        ) -> Any:
            return None

        def result_handler(self, result: Any) -> None:
            pass

    load_config_with_error_handling(project_root=tmp_path, deps=TrackingDeps())  # type: ignore[arg-type]

    assert state.config is cfg  # pyright: ignore[reportAttributeAccessIssue]
    assert state.project_root == tmp_path
    assert state.learning_mode is False
    assert state.learning_mode_set is True
    assert state.verbosity == cfg.default_verbosity


def test_load_config_with_error_handling_reuses_cached_config(
    _reset_cli_state: None,
) -> None:
    cfg = _FakeConfig(learning_mode_default=False, default_verbosity=1)
    state.config = cfg  # pyright: ignore[reportAttributeAccessIssue]
    state.project_root = Path("/tmp/example")

    class FailingDeps:
        def load_config(
            self, root: Path | None
        ) -> ToolsConfig:  # pragma: no cover - should not run
            raise AssertionError("load_config should not be invoked when config cached")

    load_config_with_error_handling(deps=FailingDeps())  # type: ignore[arg-type]

    assert state.config is cfg
    assert state.project_root == Path("/tmp/example")


def test_ensure_config_loaded_invokes_loader_when_missing(
    tmp_path: Path, _reset_cli_state: None
) -> None:
    state.project_root = tmp_path

    calls: list[Path | None] = []

    def fake_loader(project_root: Path | None) -> None:
        calls.append(project_root)
        state.config = _FakeConfig(learning_mode_default=False, default_verbosity=1)  # type: ignore[assignment]

    try:
        original_loader = config_loader.load_config_with_error_handling
        config_loader.load_config_with_error_handling = fake_loader  # type: ignore[assignment]
        ensure_config_loaded()
    finally:
        config_loader.load_config_with_error_handling = original_loader  # type: ignore[assignment]

    assert calls == [tmp_path]
    assert state.config is not None


def test_load_config_with_error_handling_unexpected_error(
    tmp_path: Path, _reset_cli_state: None
) -> None:
    """Unexpected exceptions should be echoed and cause typer.Exit(1).

    This exercises the generic Exception branch.
    """

    class FailingDeps:
        """Fake dependencies that implements ToolsDependencies protocol."""

        def load_config(self, root: Path | None) -> ToolsConfig:
            raise AttributeError("unexpected boom")

        # Add other required methods for ToolsDependencies protocol
        def quality_factory(self, config: ToolsConfig, project_root: Path) -> Any:
            return None

        def testing_factory(self, config: ToolsConfig, project_root: Path) -> Any:
            return None

        def environment_factory(self, config: ToolsConfig, project_root: Path) -> Any:
            return None

        def ci_factory(self, config: ToolsConfig, project_root: Path) -> Any:
            return None

        def dev_factory(
            self, config: ToolsConfig, project_root: Path | None = None
        ) -> Any:
            return None

        def result_handler(self, result: Any) -> None:
            pass

    with pytest.raises(typer.Exit):
        load_config_with_error_handling(project_root=tmp_path, deps=FailingDeps())  # type: ignore[arg-type]
