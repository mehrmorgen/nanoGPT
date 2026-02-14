"""Property-based tests for DevTools using Hypothesis."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Dict, cast

from hypothesis import HealthCheck, given, settings, strategies as st

from ml_playground.tools.dev.dev import (
    Comment,
    FetchResult,
    Thread,
    apply_filters,
    comment_lookup,
)

# Strategies for generating mock data
st_comment = st.fixed_dictionaries(
    {
        "author": st.text(
            min_size=1,
            alphabet=st.characters(blacklist_categories=("Cs",), min_codepoint=32),
        ),
        "viewer_did_author": st.booleans(),
        "body": st.text(
            alphabet=st.characters(blacklist_categories=("Cs",), min_codepoint=32)
        ),
        "url": st.one_of(
            st.none(),
            st.text(
                min_size=1,
                alphabet=st.characters(blacklist_categories=("Cs",), min_codepoint=32),
            ),
        ),
        "id": st.one_of(
            st.none(),
            st.text(
                min_size=1,
                alphabet=st.characters(blacklist_categories=("Cs",), min_codepoint=32),
            ),
        ),
        "databaseId": st.one_of(st.none(), st.integers(min_value=1)),
        "createdAt": st.one_of(
            st.none(),
            st.text(
                alphabet=st.characters(blacklist_categories=("Cs",), min_codepoint=32)
            ),
        ),
    }
)


@st.composite
def st_thread(draw: st.DrawFn) -> Thread:
    url = draw(
        st.text(
            min_size=1,
            alphabet=st.characters(blacklist_categories=("Cs",), min_codepoint=32),
        )
    )
    is_resolved = draw(st.booleans())
    raw_comments = draw(st.lists(st_comment, min_size=1, max_size=5))
    comments = [
        Comment(
            author=cast(str, c["author"]),
            viewer_did_author=cast(bool, c["viewer_did_author"]),
            body=cast(str, c["body"]),
            url=cast("str | None", c["url"]),
            id=cast("str | None", c["id"]),
            database_id=cast("int | None", c["databaseId"]),
            created_at=cast("str | None", c["createdAt"]),
        )
        for c in raw_comments
    ]
    return Thread(url=url, is_resolved=is_resolved, comments=comments)


@st.composite
def st_fetch_result(draw: st.DrawFn) -> FetchResult:
    threads = draw(st.lists(st_thread(), min_size=0, max_size=10))
    viewer = draw(st.one_of(st.none(), st.text(min_size=1)))
    return FetchResult(threads=threads, viewer=viewer)


@settings(max_examples=50, deadline=500)
@given(st_fetch_result())
def test_comment_lookup_invariants(fetch: FetchResult) -> None:
    """Test that _comment_lookup correctly maps available identifiers to IDs."""
    # Arrange / Act
    lookup = comment_lookup(fetch)

    # Assert
    # If multiple comments share the same identifier, setdefault means the first one wins.
    # We need to track which ID was set first for each identifier to validate invariants.
    first_id_for_ident: dict[str, str] = {}
    all_comment_ids: set[str] = set()

    for t in fetch.threads:
        for c in t.comments:
            if not c.id:
                continue
            all_comment_ids.add(c.id)

            # Implementation order: id -> url -> anchor -> database_id
            idents: list[str] = []
            idents.append(c.id)
            if c.url:
                idents.append(c.url)
                if "#" in c.url:
                    anchor = c.url.split("#")[-1]
                    if anchor:
                        idents.append(anchor)
            if c.database_id is not None:
                idents.append(str(c.database_id))

            for ident in idents:
                if ident not in first_id_for_ident:
                    first_id_for_ident[ident] = c.id

    # Invariant: Every ID in lookup must be one of the actual comment IDs
    for cid in lookup.values():
        assert cid in all_comment_ids

    # Invariant: Truthy identifiers must map to the ID that was set first
    for ident, expected_id in first_id_for_ident.items():
        assert lookup.get(ident) == expected_id


@settings(max_examples=50, deadline=500)
@given(
    st.lists(st_thread(), min_size=0, max_size=20),
    st.booleans(),
    st.booleans(),
    st.one_of(st.none(), st.text(min_size=1)),
)
def test_apply_filters_invariants(
    threads: list[Thread], unreplied: bool, unresolved: bool, viewer: str | None
) -> None:
    """Test that _apply_filters correctly preserves or removes threads based on criteria."""
    # Arrange / Act
    filtered = apply_filters(
        threads, unreplied=unreplied, unresolved=unresolved, viewer=viewer
    )

    # Assert
    assert len(filtered) <= len(threads)

    filtered_set = set(filtered)
    for t in threads:
        # If it's in the result, it must have satisfied the filters
        if t in filtered_set:
            if unresolved:
                assert not t.is_resolved
            if unreplied:
                assert not any(c.viewer_did_author for c in t.comments)
        else:
            # If it's NOT in the result, it must have failed at least one filter
            failed_unresolved = unresolved and t.is_resolved
            failed_unreplied = unreplied and any(
                c.viewer_did_author for c in t.comments
            )
            assert failed_unresolved or failed_unreplied


@settings(
    max_examples=50,
    deadline=500,
    suppress_health_check=[HealthCheck.function_scoped_fixture],
)
@given(st.dictionaries(st.text(), st.text()))
def test_load_replies_valid_json(tmp_path: Path, replies_dict: Dict[str, str]) -> None:
    """Test that _load_replies correctly loads arbitrary valid JSON dictionaries."""
    from ml_playground.tools.dev.dev import load_replies

    # Arrange
    replies_file = tmp_path / "replies.json"
    replies_file.write_text(json.dumps(replies_dict), encoding="utf-8")

    # Act
    loaded = load_replies(replies_file)

    # Assert
    # Every generated key/value is already str from st.text()
    assert loaded == replies_dict
