"""Shared test configuration and fixtures for ml_playground tests.

This module provides session-level fixtures and configuration that applies
to all tests in the ml_playground test suite.
"""

from __future__ import annotations

import os
from contextlib import contextmanager
from pathlib import Path
from textwrap import dedent
from typing import Callable, ContextManager, Iterator
import random
import numpy as np
import pytest
from hypothesis import settings
from hypothesis.database import DirectoryBasedExampleDatabase

from ml_playground.framework.configuration.models import MetadataConfig
from tests.support.config_builders import create_metadata_config

# Set Hypothesis storage directory before any Hypothesis imports or usage
os.environ["HYPOTHESIS_STORAGE_DIRECTORY"] = ".cache/hypothesis"

# Enable pytest-xdist parallel coverage natively
if "COVERAGE_PROCESS_START" in os.environ:
    try:
        import coverage

        coverage.process_startup()
    except ImportError:
        pass


# Pyright struggles with pytest's decorator typing when using keyword arguments.
@pytest.fixture(autouse=True, scope="session")  # type: ignore[arg-type]
def _seed_randomness() -> None:  # pyright: ignore[reportUnusedFunction]
    """Seed random number generators for deterministic test runs.

    This fixture automatically runs once per test session to ensure
    reproducible results across all tests that use randomness.
    """
    random.seed(1337)
    np.random.seed(1337)


@pytest.fixture(autouse=True)
def _reset_global_cli_dependency_overrides() -> None:  # pyright: ignore[reportUnusedFunction]
    """Reset global dependency overrides between tests.

    The tools CLIs maintain overrideable dependency singletons.
    Resetting them here keeps tests isolated even if earlier tests reconfigure
    dependencies without restoring.
    """
    from ml_playground.tools.cli.dependencies import reset_tools_dependencies

    reset_tools_dependencies()
    return None


# ----------------------------------------------------------------------------
# Hypothesis global database location
# ----------------------------------------------------------------------------

# Ensure Hypothesis stores its example database under the centralized cache.
# This applies regardless of how pytest is invoked (Makefile, pre-commit, IDE, etc.).
settings.register_profile(
    "repo-default",
    database=DirectoryBasedExampleDatabase(Path(".cache/hypothesis")),
)
settings.load_profile("repo-default")


# ----------------------------------------------------------------------------
# Global path fixture(s)
# ----------------------------------------------------------------------------


@pytest.fixture
def out_dir(tmp_path: Path) -> Path:
    """Provide a conventionally named output directory under tmp_path.

    Many tests construct out_dir = tmp_path / "out"; centralize this for reuse.
    """
    p = tmp_path / "out"
    p.mkdir(parents=True, exist_ok=True)
    return p


# ----------------------------------------------------------------------------
# Shared helpers for CLI/config property and unit tests
# ----------------------------------------------------------------------------


@pytest.fixture
def metadata_config_factory() -> Callable[[Path], MetadataConfig]:
    """Return a factory that builds a `MetadataConfig` rooted at the provided path."""
    return create_metadata_config


@pytest.fixture
def override_attr() -> Callable[[object, str, object], ContextManager[None]]:
    """Provide a context manager for temporarily overriding attributes on objects."""

    @contextmanager
    def _override(target: object, attr: str, value: object) -> Iterator[None]:
        original: object = getattr(target, attr)
        setattr(target, attr, value)
        try:
            yield
        finally:
            setattr(target, attr, original)

    return _override


# ----------------------------------------------------------------------------
# Central TOML builders for common experiment shapes
# ----------------------------------------------------------------------------


def _fmt_path(p: Path) -> str:
    # Ensure forward slashes in TOML strings for portability
    return str(p.as_posix())


def minimal_full_experiment_toml(
    dataset_dir: Path,
    out_dir: Path,
    *,
    extra_optim: str = "",
    extra_train: str = "",
    extra_sample: str = "",
    extra_sample_sample: str = "",
    include_train_data: bool = True,
    include_train_runtime: bool = True,
    include_sample: bool = True,
) -> str:
    """Return a minimal, strict ExperimentConfig TOML with overridable sections.

    Parameters allow injecting extra lines per section via string snippets
    (already properly indented TOML lines).
    """
    base = """
    [prepare]

    [training.model]
    """
    if include_train_data:
        base += """
        [training.data]
        """
    base += f"""
    [training.optim]
    {extra_optim}

    [training.schedule]
    """
    if include_train_runtime:
        base += f"""
        [training.runtime]
        out_dir = "{_fmt_path(out_dir)}"
        {extra_train}
        """
    if include_sample:
        base += f"""
        [sampling]
        [sampling.runtime]
        out_dir = "{_fmt_path(out_dir)}"
        {extra_sample}
        [sampling.sample]
        {extra_sample_sample}
        """
    # Add metadata section: tied to provided dataset_dir/out_dir; generic experiment metadata
    base += f"""
    [metadata]
    experiment = "exp"
    config_path = "{_fmt_path(out_dir.parent / "cfg.toml")}"
    project_home = "{_fmt_path(out_dir.parent)}"
    dataset_dir = "{_fmt_path(dataset_dir)}"
    train_out_dir = "{_fmt_path(out_dir)}"
    sample_out_dir = "{_fmt_path(out_dir)}"
    """
    return dedent(base)
