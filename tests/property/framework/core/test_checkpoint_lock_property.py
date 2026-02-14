"""Property-based tests for checkpoint lock functionality.

Tests lock acquisition, stale lock detection, and metadata handling
using Hypothesis to discover edge cases.
"""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any

import pytest
from hypothesis import assume, given, settings, strategies as st

from ml_playground.framework.core.checkpoint_lock import (
    CheckpointLockError,
    checkpoint_lock,
    checkpoint_lock_path,
    read_lock_metadata,
)


# =============================================================================
# checkpoint_lock_path Property Tests
# =============================================================================


@settings(max_examples=20, deadline=None, derandomize=True)
@given(
    filename=st.text(
        alphabet=st.characters(whitelist_categories=("L", "N", "P")),
        min_size=1,
        max_size=30,
    )
)
def test_checkpoint_lock_path_properties(filename: str) -> None:
    """checkpoint_lock_path produces valid lock file paths."""
    assume(filename not in {".", "..", "/", "//"})
    assume(not filename.startswith("/"))
    assume("/" not in filename or filename.count("/") == 0)

    out_dir = Path("/tmp/checkpoints")
    lock_path = checkpoint_lock_path(out_dir, filename)

    # Lock path should be under out_dir
    assert lock_path.parent == out_dir
    # Lock path should end with .lock
    assert lock_path.suffix == ".lock"
    # Lock path should contain the filename
    assert filename.replace(".pt", "").replace(".pth", "") in lock_path.stem


@settings(max_examples=10, deadline=None, derandomize=True)
@given(bad_filename=st.sampled_from([".", ".."]))
def test_checkpoint_lock_path_rejects_parent_refs(bad_filename: str) -> None:
    """checkpoint_lock_path rejects parent directory references."""
    out_dir = Path("/tmp/checkpoints")
    with pytest.raises(ValueError, match="must not"):
        checkpoint_lock_path(out_dir, bad_filename)


@settings(max_examples=10, deadline=None, derandomize=True)
@given(absolute=st.just("/absolute/path.pt"))
def test_checkpoint_lock_path_rejects_absolute(absolute: str) -> None:
    """checkpoint_lock_path rejects absolute paths."""
    out_dir = Path("/tmp/checkpoints")
    with pytest.raises(ValueError, match="must not be absolute"):
        checkpoint_lock_path(out_dir, absolute)


# =============================================================================
# read_lock_metadata Property Tests
# =============================================================================


@settings(max_examples=15, deadline=None, derandomize=True)
@given(
    owner=st.text(min_size=1, max_size=20),
    pid=st.integers(min_value=1, max_value=65535),
)
def test_read_lock_metadata_valid_hypothesis(owner: str, pid: int) -> None:
    """read_lock_metadata parses valid lock files with various inputs."""
    import tempfile

    with tempfile.TemporaryDirectory() as tmp:
        lock_file = Path(tmp) / "test.lock"
        data = {"owner": owner, "pid": pid, "timestamp": time.time()}
        lock_file.write_text(json.dumps(data))

        result = read_lock_metadata(lock_file)
        assert result is not None
        assert result.get("owner") == owner
        assert result.get("pid") == pid


def test_read_lock_metadata_valid_file(tmp_path: Path) -> None:
    """read_lock_metadata parses valid lock file content."""
    lock_file = tmp_path / "test.lock"
    data = {"owner": "test:run", "pid": 1234, "timestamp": time.time()}
    lock_file.write_text(json.dumps(data))

    result = read_lock_metadata(lock_file)
    assert result is not None
    assert result.get("owner") == "test:run"
    assert result.get("pid") == 1234


@settings(max_examples=10, deadline=None, derandomize=True)
@given(
    bad_content=st.one_of(
        st.just("not json"),
        st.just(""),
        st.just("{invalid"),
    )
)
def test_read_lock_metadata_invalid_returns_none(bad_content: str) -> None:
    """read_lock_metadata returns None for invalid content."""
    # Test via actual file since we need file system
    import tempfile

    with tempfile.TemporaryDirectory() as tmp:
        lock_file = Path(tmp) / "test.lock"
        lock_file.write_text(bad_content)
        result = read_lock_metadata(lock_file)
        assert result is None


def test_read_lock_metadata_handles_unicode_error(tmp_path: Path) -> None:
    """read_lock_metadata handles files with invalid Unicode."""
    lock_file = tmp_path / "test.lock"
    # Write invalid UTF-8 bytes
    lock_file.write_bytes(b"\xff\xfe\x00\x00")
    result = read_lock_metadata(lock_file)
    assert result is None


# =============================================================================
# checkpoint_lock Property Tests
# =============================================================================


@settings(max_examples=10, deadline=None, derandomize=True)
@given(owner=st.text(min_size=1, max_size=30))
def test_checkpoint_lock_basic_acquisition(owner: str) -> None:
    """checkpoint_lock creates and releases lock files with various owners."""
    import tempfile

    with tempfile.TemporaryDirectory() as tmp:
        lock_path = Path(tmp) / "test.pt.lock"
        with checkpoint_lock(lock_path, owner=owner):
            assert lock_path.exists()
            content = json.loads(lock_path.read_text())
            assert content["owner"] == owner
        assert not lock_path.exists()


def test_checkpoint_lock_creates_and_releases(tmp_path: Path) -> None:
    """Lock is created during context and removed after."""
    lock_path = tmp_path / "test.pt.lock"
    with checkpoint_lock(lock_path, owner="test:unit"):
        assert lock_path.exists()
    assert not lock_path.exists()


def test_checkpoint_lock_contains_metadata(tmp_path: Path) -> None:
    """Lock file contains owner and timestamp metadata."""
    lock_path = tmp_path / "test.pt.lock"
    with checkpoint_lock(lock_path, owner="test:metadata"):
        content = lock_path.read_text()
        data = json.loads(content)
        assert data["owner"] == "test:metadata"
        assert "pid" in data
        assert "timestamp" in data


@settings(max_examples=5, deadline=None, derandomize=True)
@given(
    ts=st.one_of(
        st.floats(min_value=0, max_value=1e10),
        st.floats(min_value=float("-inf"), max_value=float("inf")),
    )
)
def test_checkpoint_lock_handles_various_timestamps(ts: float) -> None:
    """Lock acquisition handles various timestamp formats in existing locks."""
    import tempfile

    with tempfile.TemporaryDirectory() as tmp:
        lock_path = Path(tmp) / "test.pt.lock"
        # Pre-populate with specific timestamp
        data = {"owner": "existing", "pid": 9999, "timestamp": ts}
        lock_path.parent.mkdir(parents=True, exist_ok=True)
        lock_path.write_text(json.dumps(data))

        # Should handle gracefully - either succeed or raise CheckpointLockError
        try:
            with checkpoint_lock(lock_path, owner="test:new", max_retries=0):
                pass
        except CheckpointLockError:
            pass  # Expected if timestamp is valid and lock appears fresh
        except (ValueError, OverflowError, OSError):
            pass  # Expected for invalid timestamps


def test_checkpoint_lock_prevents_double_acquisition(tmp_path: Path) -> None:
    """Second lock acquisition fails when lock is held."""
    lock_path = tmp_path / "test.pt.lock"
    with checkpoint_lock(lock_path, owner="first:holder"):
        with pytest.raises(CheckpointLockError):
            with checkpoint_lock(lock_path, owner="second:holder", max_retries=0):
                pass


# =============================================================================
# Stale Lock Detection Property Tests
# =============================================================================


def test_checkpoint_lock_detects_stale_lock_by_timestamp(tmp_path: Path) -> None:
    """Stale locks with old timestamps are removed and lock acquired."""
    lock_path = tmp_path / "test.pt.lock"
    old_ts = time.time() - 1000  # 1000 seconds ago
    data = {"owner": "stale:owner", "pid": 9999, "timestamp": old_ts}
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    lock_path.write_text(json.dumps(data))

    # Should be able to acquire despite existing lock
    with checkpoint_lock(lock_path, owner="test:new", stale_lock_timeout=600):
        assert lock_path.exists()
        new_content = json.loads(lock_path.read_text())
        assert new_content["owner"] == "test:new"


def test_checkpoint_lock_respects_fresh_lock(tmp_path: Path) -> None:
    """Fresh locks prevent acquisition."""
    lock_path = tmp_path / "test.pt.lock"
    fresh_ts = time.time()  # Current time
    data = {"owner": "fresh:owner", "pid": 9999, "timestamp": fresh_ts}
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    lock_path.write_text(json.dumps(data))

    with pytest.raises(CheckpointLockError):
        with checkpoint_lock(
            lock_path, owner="test:new", stale_lock_timeout=600, max_retries=0
        ):
            pass


@settings(max_examples=10, deadline=None, derandomize=True)
@given(
    bad_ts=st.one_of(
        st.just(float("nan")),
        st.just(float("inf")),
        st.just(float("-inf")),
        st.text(),
    )
)
def test_checkpoint_lock_handles_bad_timestamp_types(bad_ts: Any) -> None:
    """Various invalid timestamp types are handled gracefully."""
    import tempfile

    with tempfile.TemporaryDirectory() as tmp:
        lock_path = Path(tmp) / "test.pt.lock"
        data: dict[str, Any] = {"owner": "bad:ts", "pid": 9999, "timestamp": bad_ts}
        lock_path.parent.mkdir(parents=True, exist_ok=True)
        lock_path.write_text(json.dumps(data, default=str))

        # Should handle gracefully - either succeed or raise CheckpointLockError
        try:
            with checkpoint_lock(lock_path, owner="test:new", max_retries=0):
                pass
        except CheckpointLockError:
            pass  # Expected


# =============================================================================
# Lock Retry Logic Property Tests
# =============================================================================


def test_checkpoint_lock_retries_on_stale_lock(tmp_path: Path) -> None:
    """Lock acquisition retries when stale lock is detected."""
    lock_path = tmp_path / "test.pt.lock"
    # Create a lock that will be detected as stale
    old_ts = time.time() - 1000
    data = {"owner": "retry:test", "pid": 8888, "timestamp": old_ts}
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    lock_path.write_text(json.dumps(data))

    # Should succeed after detecting stale lock and retrying
    with checkpoint_lock(
        lock_path, owner="test:retry", stale_lock_timeout=600, max_retries=3
    ):
        new_data = json.loads(lock_path.read_text())
        assert new_data["owner"] == "test:retry"


@settings(max_examples=5, deadline=None, derandomize=True)
@given(max_retries=st.integers(min_value=0, max_value=5))
def test_checkpoint_lock_respects_max_retries(max_retries: int) -> None:
    """Lock acquisition respects max_retries parameter."""
    import tempfile

    with tempfile.TemporaryDirectory() as tmp:
        lock_path = Path(tmp) / "test.pt.lock"
        # Create a valid fresh lock that won't be considered stale
        fresh_ts = time.time()
        data = {"owner": "fresh:blocker", "pid": 7777, "timestamp": fresh_ts}
        lock_path.parent.mkdir(parents=True, exist_ok=True)
        lock_path.write_text(json.dumps(data))

        # Should fail immediately or after retries
        with pytest.raises(CheckpointLockError):
            with checkpoint_lock(
                lock_path,
                owner="test:retry",
                max_retries=max_retries,
                stale_lock_timeout=600,
            ):
                pass
