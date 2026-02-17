from __future__ import annotations

from pathlib import Path

import pytest
from hypothesis import given, strategies as st

from ml_playground.framework.core import checkpoint_lock, tokenizer

# --- Checkpoint Lock Tests ---


@given(
    filename=st.one_of(
        st.just("."),
        st.just(".."),
        st.just("/absolute/path"),
        st.just("/root"),
        st.text(min_size=1).map(lambda s: "/" + s),  # Absolute path simulation
    )
)
def test_checkpoint_lock_path_validation(filename: str) -> None:
    # We fake the check for absolute/root in a way that matches os.path behavior if possible
    # But since the code uses Path.is_absolute(), we rely on that.
    # Note: On non-posix, "/" might not be absolute.
    # For robust testing, we just check if it raises ValueError for known invalid inputs.

    if filename in {".", ".."}:
        with pytest.raises(ValueError, match="must not be"):
            checkpoint_lock.checkpoint_lock_path(Path("out"), filename)
    elif filename.startswith("/"):
        with pytest.raises(ValueError, match="must not be"):
            checkpoint_lock.checkpoint_lock_path(Path("out"), filename)


def test_checkpoint_lock_args_validation() -> None:
    p = Path("lock")
    with pytest.raises(ValueError, match="non-negative"):
        with checkpoint_lock.checkpoint_lock(p, owner="me", max_retries=-1):
            pass
    with pytest.raises(ValueError, match="non-negative"):
        with checkpoint_lock.checkpoint_lock(p, owner="me", stale_lock_timeout=-1.0):
            pass


def test_read_lock_metadata_corruption(tmp_path: Path) -> None:
    lock_path = tmp_path / "lock.json"
    lock_path.write_text("not-json", encoding="utf-8")
    assert checkpoint_lock.read_lock_metadata(lock_path) is None

    assert checkpoint_lock.read_lock_metadata(tmp_path) is None


# --- Tokenizer Tests ---

TOK_TYPES = st.sampled_from(["char", "word", "tiktoken"])


@given(tok_type=TOK_TYPES, extra_kw=st.text(min_size=1))
def test_create_tokenizer_invalid_kwargs(tok_type: str, extra_kw: str) -> None:
    # Ensure extra_kw is not a valid arg
    valid_args = {"vocab", "encoding_name", "loader", "tokenizer_type"}
    if extra_kw in valid_args:
        return

    kwargs = {extra_kw: "val"}

    # Pragma no cover lines in source might handle TypeError (invalid type of valid arg)
    # But here we test ValueError (unknown arg)

    if tok_type == "char":
        with pytest.raises(ValueError, match="Unsupported keyword arguments"):
            tokenizer.create_tokenizer(tok_type, **kwargs)
    elif tok_type == "word":
        with pytest.raises(ValueError, match="Unsupported keyword arguments"):
            tokenizer.create_tokenizer(tok_type, **kwargs)
    elif tok_type == "tiktoken":
        # Pass required loader/encoding to trigger the kwargs check
        with pytest.raises(ValueError, match="Unsupported keyword arguments"):
            tokenizer.create_tokenizer(tok_type, loader=lambda: None, **kwargs)


@given(_vals=st.lists(st.text()))
def test_create_tokenizer_invalid_vocab_types(_vals: list[str]) -> None:
    vocab = {"k": "string_value"}
    with pytest.raises(TypeError, match="numeric or boolean"):
        tokenizer.create_tokenizer("char", vocab=vocab)


def test_char_tokenizer_empty_decode() -> None:
    t = tokenizer.CharTokenizer()
    assert t.decode([]) == ""
    assert t.decode([999]) == ""  # Invalid ID


def test_word_tokenizer_empty_decode() -> None:
    t = tokenizer.WordTokenizer()
    assert t.decode([]) == ""
    assert t.decode([999]) == ""


def test_create_tokenizer_vocab_coercion() -> None:
    vocab = {
        "a": True,
        "b": 1,
        "c": 2.0,
    }
    tok = tokenizer.create_tokenizer("char", vocab=vocab)
    assert tok.vocab["a"] == 1
    assert tok.vocab["b"] == 1
    assert tok.vocab["c"] == 2


def test_create_tokenizer_tiktoken_defaults() -> None:
    # Hit the loader is None branch
    try:
        tokenizer.create_tokenizer("tiktoken")
    except ImportError:  # if tiktoken missing
        pass
