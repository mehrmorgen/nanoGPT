from __future__ import annotations

from pathlib import Path


from ml_playground.tools.core.config import ToolsConfig
from ml_playground.tools.environment.clean import run_clean
from tests.unit.tools.fakes import FakeSubprocessRunner


def test_run_clean_empty(tmp_path: Path) -> None:
    """Test clean on empty project."""
    config = ToolsConfig()
    runner = FakeSubprocessRunner()
    cache_dir = tmp_path / ".cache"

    result = run_clean(config, tmp_path, cache_dir, [], runner)

    assert result.success is True
    assert "Cache directory was empty or missing" in result.stdout
    assert "Cache directory is now empty or removed" in result.stdout


def test_run_clean_removes_targets(tmp_path: Path) -> None:
    """Test removal of specific targets."""
    config = ToolsConfig()
    runner = FakeSubprocessRunner()
    cache_dir = tmp_path / ".cache"
    cache_dir.mkdir()

    # Create some targets
    (cache_dir / "pytest").mkdir()
    (cache_dir / "file_target").write_text("content")
    (tmp_path / "build").mkdir()
    (tmp_path / "dist").mkdir()

    # Fake target modification in run_clean
    # We need to patch run_clean's cache_targets list or similar?
    # No, run_clean defines targets internally.
    # It cleans: pytest, coverage, hypothesis, pre-commit, ruff, uv, mypy in .cache
    # And build, dist, htmlcov, *.egg-info in root

    result = run_clean(config, tmp_path, cache_dir, [], runner)

    assert result.success is True
    assert not (cache_dir / "pytest").exists()
    assert not (tmp_path / "build").exists()
    assert not (tmp_path / "dist").exists()
    assert "Cleaned" in result.stdout
    assert "pytest" in result.stdout


def test_run_clean_egg_info_glob(tmp_path: Path) -> None:
    """Test removal of egg-info directories via glob."""
    config = ToolsConfig()
    runner = FakeSubprocessRunner()
    cache_dir = tmp_path / ".cache"

    egg_info = tmp_path / "my_pkg.egg-info"
    egg_info.mkdir()
    (egg_info / "PKG-INFO").write_text("info")

    result = run_clean(config, tmp_path, cache_dir, [], runner)

    assert result.success is True
    assert not egg_info.exists()
    assert "my_pkg.egg-info" in result.stdout


def test_run_clean_egg_info_glob_skips_nonexistent(tmp_path: Path) -> None:
    config = ToolsConfig()
    runner = FakeSubprocessRunner()
    cache_dir = tmp_path / ".cache"

    broken = tmp_path / "broken.egg-info"
    try:
        broken.symlink_to(tmp_path / "does-not-exist")
    except OSError:
        return

    result = run_clean(config, tmp_path, cache_dir, [], runner)

    assert result.success is True
    assert broken.exists() is False


def test_run_clean_pycache_recursive(tmp_path: Path) -> None:
    """Test recursive removal of __pycache__."""
    config = ToolsConfig()
    runner = FakeSubprocessRunner()
    cache_dir = tmp_path / ".cache"

    src = tmp_path / "src" / "pkg"
    src.mkdir(parents=True)
    pycache1 = src / "__pycache__"
    pycache1.mkdir()
    (pycache1 / "mod.cpython-39.pyc").write_text("data")

    test_dir = tmp_path / "tests"
    test_dir.mkdir()
    pycache2 = test_dir / "__pycache__"
    pycache2.mkdir()

    result = run_clean(config, tmp_path, cache_dir, [], runner)

    assert result.success is True
    assert not pycache1.exists()
    assert not pycache2.exists()
    assert "Removed 2 __pycache__ directories" in result.stdout


def test_run_clean_pycache_skips_nonexistent(tmp_path: Path) -> None:
    config = ToolsConfig()
    runner = FakeSubprocessRunner()
    cache_dir = tmp_path / ".cache"

    src = tmp_path / "src"
    src.mkdir()
    broken = src / "__pycache__"
    try:
        broken.symlink_to(tmp_path / "does-not-exist")
    except OSError:
        return

    result = run_clean(config, tmp_path, cache_dir, [], runner)

    assert result.success is True
    assert broken.exists() is False


def test_run_clean_file_targets(tmp_path: Path) -> None:
    """Test removal of file targets if they exist where directories are expected."""
    config = ToolsConfig()
    runner = FakeSubprocessRunner()
    cache_dir = tmp_path / ".cache"
    cache_dir.mkdir()

    # Create a file where a dir is usually expected
    (cache_dir / "mypy").write_text("not a dir")

    result = run_clean(config, tmp_path, cache_dir, [], runner)

    assert result.success is True
    assert not (cache_dir / "mypy").exists()
    assert "mypy" in result.stdout
