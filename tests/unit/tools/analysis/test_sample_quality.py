from __future__ import annotations
from pathlib import Path


from ml_playground.tools.analysis.sample_quality import (
    extract_header,
    line_stats,
    ngram_stats,
    find_anomalies,
    analyze_sample_text,
    analyze_sample_file,
    format_analysis,
)


def test_extract_header_variants() -> None:
    """Test extract header variants."""
    lines: list[str] = [
        "Sprecher: Dr. Example",
        "Thema: Haushalt",
        "Jahr: 2021",
        "Jahr: 2022",
        "Some body line",
    ]
    h = extract_header(lines)
    assert h.speaker == "Dr. Example"
    assert h.topic == "Haushalt"
    assert h.year == "2021"
    assert h.year_count == 2

    lines2: list[str] = ["no header here"]
    h2 = extract_header(lines2)
    assert (
        h2.speaker is None
        and h2.topic is None
        and h2.year is None
        and h2.year_count == 0
    )


def test_line_stats_repeats_and_runs() -> None:
    """Test line stats repeats and runs."""
    lines: list[str] = [
        "a",
        "a",
        "b",
        "",
        "b",
        "b",
        "c",
        "c",
        "c",
    ]
    ls = line_stats(lines)
    assert ls.total_lines == len(lines)
    assert ls.non_empty_lines == 8
    assert ls.longest_identical_run == 3
    assert any(s == "b" and c == 3 for s, c in ls.top_repeated_lines)


def test_ngram_stats_and_fallback() -> None:
    """Test ngram stats and fallback."""
    lines: list[str] = ["Hello, world! Hello."]
    ns = ngram_stats(lines, 1)
    assert ns.n == 1
    assert ns.unique_ngrams > 0

    ns2 = ngram_stats(lines, 0)  # fallback to 3
    assert ns2.n == 3


def test_find_anomalies_trailing_and_years() -> None:
    """Test find anomalies trailing and years."""
    lines: list[str] = [
        "Some intro.",
        "Jahr: 2020",
        "Body mentions 2018 and 1999 too",
        "unfinished line with year 2025",
    ]
    an = find_anomalies(lines)
    assert an.trailing_incomplete_line is True
    assert (
        "2018" in an.stray_year_tokens
        and "1999" in an.stray_year_tokens
        and "2025" in an.stray_year_tokens
    )


def test_analyze_and_format_text_and_file(tmp_path: Path) -> None:
    """Test analyze and format text and file."""
    text = "\n".join(
        [
            "Sprecher: A",
            "Thema: B",
            "Jahr: 2022",
            "Hello world.",
            "Hello world.",
            "Bye.",
        ]
    )
    a = analyze_sample_text(text, ngram_n=2)
    s = format_analysis(a)
    assert (
        "== Header ==" in s
        and "== Lines ==" in s
        and "== N-grams ==" in s
        and "== Anomalies ==" in s
    )

    p = tmp_path / "sample.txt"
    p.write_text(text, encoding="utf-8")
    a2 = analyze_sample_file(p, ngram_n=2)
    assert a2.header.speaker == "A"


def test_find_anomalies_empty_lines() -> None:
    """find_anomalies should handle empty lines list."""
    lines: list[str] = []
    an = find_anomalies(lines)
    assert an.trailing_incomplete_line is False
    assert an.stray_year_tokens == []


def test_find_anomalies_complete_line() -> None:
    """find_anomalies should detect complete lines (ending with punctuation)."""
    lines: list[str] = ["This is a complete sentence."]
    an = find_anomalies(lines)
    assert an.trailing_incomplete_line is False


def test_format_analysis_no_repeated_lines():
    """format_analysis should handle case with no repeated lines."""
    from ml_playground.tools.analysis.sample_quality import (
        SampleAnalysis,
        Header,
        LineStats,
        NgramStats,
        Anomalies,
    )

    analysis = SampleAnalysis(
        header=Header(speaker="A", topic="B", year="2022", year_count=1),
        lines=LineStats(
            total_lines=3,
            non_empty_lines=3,
            unique_lines=3,
            unique_ratio=1.0,
            longest_identical_run=1,
            top_repeated_lines=[],  # No repeated lines
        ),
        ngrams=NgramStats(n=3, unique_ngrams=5, top_repeated_ngrams=[]),
        anomalies=Anomalies(trailing_incomplete_line=False, stray_year_tokens=[]),
    )
    s = format_analysis(analysis)
    assert "top_repeated_lines: -" in s
    assert "top_repeated_ngrams: -" in s
    assert "stray_year_tokens: -" in s


def test_format_analysis_long_line_preview():
    """format_analysis should truncate long lines in preview."""
    from ml_playground.tools.analysis.sample_quality import (
        SampleAnalysis,
        Header,
        LineStats,
        NgramStats,
        Anomalies,
    )

    long_line = "x" * 100
    long_ngram = "word " * 30  # Very long ngram
    analysis = SampleAnalysis(
        header=Header(speaker="A", topic="B", year="2022", year_count=1),
        lines=LineStats(
            total_lines=1,
            non_empty_lines=1,
            unique_lines=1,
            unique_ratio=1.0,
            longest_identical_run=1,
            top_repeated_lines=[(long_line, 2)],
        ),
        ngrams=NgramStats(n=3, unique_ngrams=1, top_repeated_ngrams=[(long_ngram, 2)]),
        anomalies=Anomalies(trailing_incomplete_line=False, stray_year_tokens=[]),
    )
    s = format_analysis(analysis)
    assert "..." in s  # Should contain truncation indicator for both lines and ngrams


def test_format_analysis_with_stray_years():
    """format_analysis should display stray year tokens when present."""
    from ml_playground.tools.analysis.sample_quality import (
        SampleAnalysis,
        Header,
        LineStats,
        NgramStats,
        Anomalies,
    )

    analysis = SampleAnalysis(
        header=Header(speaker="A", topic="B", year="2022", year_count=1),
        lines=LineStats(
            total_lines=1,
            non_empty_lines=1,
            unique_lines=1,
            unique_ratio=1.0,
            longest_identical_run=1,
            top_repeated_lines=[],
        ),
        ngrams=NgramStats(n=3, unique_ngrams=1, top_repeated_ngrams=[]),
        anomalies=Anomalies(
            trailing_incomplete_line=False, stray_year_tokens=["2018", "1999"]
        ),
    )
    s = format_analysis(analysis)
    assert "stray_year_tokens: 2018, 1999" in s
