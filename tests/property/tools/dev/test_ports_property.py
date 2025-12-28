"""Property-based tests for DevTools.kill_port."""

from __future__ import annotations

from hypothesis import given, settings, strategies as st

from ml_playground.tools import dev


@settings(max_examples=50, deadline=500)
@given(
    st.lists(st.integers(min_value=1, max_value=50000), max_size=10),
    st.booleans(),
)
def test_kill_port_deduplicates_and_handles_failures(
    pids: list[int], kill_success: bool
) -> None:
    """kill_port should deduplicate PIDs and handle kill failures deterministically."""

    seen: list[int] = []

    def fake_kill(pid: int) -> bool:
        seen.append(pid)
        return kill_success

    tools = dev.DevTools(pids_by_port=lambda _: pids, kill_pid=fake_kill)
    result = tools.kill_port(4242)

    expected_unique = list(dict.fromkeys(pid for pid in pids))

    if not pids:
        assert result.success is True
        assert "No processes found" in (result.stdout or "")
    elif kill_success:
        assert result.success is True
        assert f"Killed {len(expected_unique)} processes" in (result.stdout or "")
        assert seen == expected_unique
    else:
        assert result.success is False
        assert "Failed to kill PID" in (result.stderr or "")
        # only the first unique pid should have been attempted
        assert seen == expected_unique[:1]
