from typing import Generator

import pytest

from ml_playground.framework.runtime.core.bootstrap import CLIDependencies
from ml_playground.runtime_cli.runners import (
    configure_cli_dependencies,
    get_cli_dependencies,
    reset_cli_dependencies,
)


@pytest.fixture(autouse=True)
def _reset_deps() -> Generator[None, None, None]:
    reset_cli_dependencies()
    yield
    reset_cli_dependencies()


def test_default_dependencies_are_configured() -> None:
    deps = get_cli_dependencies()
    assert deps is not None
    assert callable(deps.load_experiment)
    assert callable(deps.run_train)


def test_configure_cli_dependencies() -> None:
    def fake_deps() -> CLIDependencies:
        # We just need a valid CLIDependencies object
        from ml_playground.runtime_cli.runners import create_default_cli_dependencies

        return create_default_cli_dependencies()

    configure_cli_dependencies(fake_deps)
    deps = get_cli_dependencies()
    assert deps is not None
