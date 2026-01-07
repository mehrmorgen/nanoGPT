from __future__ import annotations

from pathlib import Path
import tempfile

from hypothesis import given, settings, strategies as st

from ml_playground.data_pipeline.transforms.ingestion import stream_text_lines


@settings(max_examples=25, deadline=50, derandomize=True)
@given(
    lines=st.lists(
        st.text(alphabet=st.characters(blacklist_characters="\n"), max_size=10),
        min_size=1,
        max_size=5,
    )
)
def test_stream_text_lines_round_trips(lines: list[str]) -> None:
    """Streaming text lines returns the original line sequence."""
    with tempfile.TemporaryDirectory() as tmp_dir:
        path = Path(tmp_dir) / "input.txt"
        path.write_text("\n".join(lines) + "\n", encoding="utf-8")

        assert list(stream_text_lines(path)) == lines
