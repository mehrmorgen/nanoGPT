from __future__ import annotations

from hypothesis import given, strategies as st

from ml_playground.tools.analysis.sample_quality import (
    analyze_sample_text,
    extract_header,
    find_anomalies,
    format_analysis,
    line_stats,
    ngram_stats,
)

# Strategies
text_strategy = st.text(min_size=0, max_size=1000)
lines_strategy = st.lists(st.text(min_size=0, max_size=200), min_size=0, max_size=50)


@given(lines_strategy)
def test_extract_header_property(lines: list[str]) -> None:
    """extract_header should never crash and return a Header object."""
    header = extract_header(lines)
    assert hasattr(header, "speaker")
    assert hasattr(header, "topic")
    assert hasattr(header, "year")
    assert isinstance(header.year_count, int)


@given(lines_strategy)
def test_line_stats_property(lines: list[str]) -> None:
    """line_stats should never crash and return valid stats."""
    stats = line_stats(lines)
    assert stats.total_lines == len(lines)
    assert 0 <= stats.non_empty_lines <= len(lines)
    assert 0 <= stats.unique_lines <= stats.non_empty_lines
    if stats.non_empty_lines > 0:
        assert 0.0 <= stats.unique_ratio <= 1.0
    else:
        assert stats.unique_ratio == 0.0
    assert stats.longest_identical_run >= 0


@given(lines_strategy, st.integers(min_value=1, max_value=10))
def test_ngram_stats_property(lines: list[str], n: int) -> None:
    """ngram_stats should never crash and return valid ngram counts."""
    stats = ngram_stats(lines, n)
    assert stats.n == n
    assert stats.unique_ngrams >= 0
    for gram, count in stats.top_repeated_ngrams:
        assert count > 1
        assert isinstance(gram, str)


@given(lines_strategy)
def test_find_anomalies_property(lines: list[str]) -> None:
    """find_anomalies should never crash."""
    anomalies = find_anomalies(lines)
    assert isinstance(anomalies.trailing_incomplete_line, bool)
    assert isinstance(anomalies.stray_year_tokens, list)


@given(text_strategy, st.integers(min_value=1, max_value=5))
def test_analyze_sample_text_property(text: str, n: int) -> None:
    """analyze_sample_text should handle arbitrary text strings."""
    analysis = analyze_sample_text(text, ngram_n=n)
    assert analysis.header is not None
    assert analysis.lines is not None
    assert analysis.ngrams is not None
    assert analysis.anomalies is not None

    # format_analysis should also work on the result
    report = format_analysis(analysis)
    assert isinstance(report, str)
    assert "== Header ==" in report
    assert "== Lines ==" in report
    assert "== N-grams ==" in report
    assert "== Anomalies ==" in report
