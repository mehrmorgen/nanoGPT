"""Additional coverage tests for mutation testing tools."""

import builtins
import sys
from contextlib import contextmanager
from pathlib import Path
from types import ModuleType
from typing import Any, Dict, Iterator, List, Union

import pytest

from ml_playground.tools.core.config import ToolsConfig
from ml_playground.tools.testing import mutation


_MISSING = object()


@contextmanager
def override_attr(obj: Any, name: str, value: Any) -> Iterator[None]:
    original = getattr(obj, name, _MISSING)
    setattr(obj, name, value)
    try:
        yield
    finally:
        if original is _MISSING:
            delattr(obj, name)
        else:
            setattr(obj, name, original)


@contextmanager
def install_modules(modules: dict[str, Any]) -> Iterator[None]:
    originals = {name: sys.modules.get(name) for name in modules}
    for name, module in modules.items():
        sys.modules[name] = module
    try:
        yield
    finally:
        for name, original in originals.items():
            if original is None:
                sys.modules.pop(name, None)
            else:
                sys.modules[name] = original


class FakeModule(ModuleType):
    def __init__(self, name: str, **kwargs: Any):
        super().__init__(name)
        for k, v in kwargs.items():
            setattr(self, k, v)


@pytest.fixture
def config() -> ToolsConfig:
    return ToolsConfig()


def test_mutation_summary_as_dict_branches(config: ToolsConfig, tmp_path: Path) -> None:
    """Cover _as_dict branches in mutation_summary (lines 77-81)."""
    raw_config: Dict[Union[int, str], Union[str, List[Any]]] = {
        123: "ignore",
        "session": "not-a-dict",
        "modules": [],
    }

    cr_config = FakeModule("cosmic_ray.config", load_config=lambda path: raw_config)
    cr_modules = FakeModule("cosmic_ray.modules", find_modules=lambda cfg: [])

    with install_modules(
        {
            "cosmic_ray.config": cr_config,
            "cosmic_ray.modules": cr_modules,
        }
    ):
        result = mutation.mutation_summary(config, tmp_path)

    assert result.success is True
    assert "session: .cache/cosmic-ray/session.sqlite" in result.stdout


def test_mutation_report_as_dict_branches(config: ToolsConfig, tmp_path: Path) -> None:
    """Cover _as_dict branches in mutation_report (lines 216-220)."""
    raw_config = {
        123: "ignore",
        "session": "not-a-dict",
    }

    cr_config = FakeModule("cosmic_ray.config", load_config=lambda path: raw_config)

    # Fake sqlite3 that raises FileNotFoundError on connect
    def failing_connect(*args: Any, **kwargs: Any) -> Any:
        raise FileNotFoundError("boom")

    class SqliteError(Exception):
        pass

    sqlite_mod = FakeModule("sqlite3", connect=failing_connect, Error=SqliteError)

    with install_modules(
        {
            "cosmic_ray.config": cr_config,
            "sqlite3": sqlite_mod,
        }
    ):
        result = mutation.mutation_report(config, tmp_path)

    assert result.success is True


def test_mutation_report_sqlite_connect_errors(
    config: ToolsConfig, tmp_path: Path
) -> None:
    """Cover specific sqlite connect errors (lines 239-247)."""
    cr_config = FakeModule("cosmic_ray.config", load_config=lambda path: {})

    class SqliteError(Exception):
        pass

    def failing_connect(*args: Any, **kwargs: Any) -> Any:
        raise TypeError("bad arg")

    sqlite_mod = FakeModule("sqlite3", connect=failing_connect, Error=SqliteError)

    with install_modules(
        {
            "cosmic_ray.config": cr_config,
            "sqlite3": sqlite_mod,
        }
    ):
        result = mutation.mutation_report(config, tmp_path)

    assert result.success is True
    assert "sqlite3.connect failed: bad arg" in result.stderr


def test_mutation_report_row_factory(config: ToolsConfig, tmp_path: Path) -> None:
    """Cover row_factory inner function (line 250)."""
    cr_config = FakeModule("cosmic_ray.config", load_config=lambda path: {})

    class FakeConn:
        row_factory: Any = None

        def __enter__(self) -> "FakeConn":
            return self

        def __exit__(self, *args: Any) -> None:
            pass

        def execute(self, *args: Any) -> Any:
            return iter([])

    conn_instance = FakeConn()

    def connect(*args: Any, **kwargs: Any) -> Any:
        return conn_instance

    class SqliteError(Exception):
        pass

    sqlite_mod = FakeModule("sqlite3", connect=connect, Error=SqliteError)

    with install_modules(
        {
            "cosmic_ray.config": cr_config,
            "sqlite3": sqlite_mod,
        }
    ):
        mutation.mutation_report(config, tmp_path)

    # Extract the assigned row_factory
    row_factory = conn_instance.row_factory
    assert callable(row_factory)
    assert row_factory(None, ["test-value"]) == "test-value"


def test_mutation_report_iterator_scalar_result(
    config: ToolsConfig, tmp_path: Path
) -> None:
    """Cover scalar result from iterator cursor (lines 277-281)."""
    cr_config = FakeModule("cosmic_ray.config", load_config=lambda path: {})

    class FakeCursor:
        def __iter__(self) -> Iterator[Any]:
            return iter([42])

    class FakeConn:
        def __enter__(self) -> "FakeConn":
            return self

        def __exit__(self, *args: Any) -> None:
            pass

        def execute(self, sql: str) -> Any:
            return FakeCursor()

    class SqliteError(Exception):
        pass

    sqlite_mod = FakeModule(
        "sqlite3", connect=lambda path: FakeConn(), Error=SqliteError
    )

    with install_modules(
        {
            "cosmic_ray.config": cr_config,
            "sqlite3": sqlite_mod,
        }
    ):
        result = mutation.mutation_report(config, tmp_path)

    assert result.success is True
    assert "mutants processed: 42" in result.stdout


def test_mutation_report_import_error(config: ToolsConfig, tmp_path: Path) -> None:
    """Cover ImportError in mutation_report (line 310)."""

    original_import = builtins.__import__

    def raising_import(name: str, *args: Any, **kwargs: Any) -> Any:
        if name == "cosmic_ray.config":
            raise ImportError("no cosmic ray")
        return original_import(name, *args, **kwargs)

    with override_attr(builtins, "__import__", raising_import):
        result = mutation.mutation_report(config, tmp_path)

    assert result.success is False
    assert "cosmic_ray must be installed" in result.stderr
