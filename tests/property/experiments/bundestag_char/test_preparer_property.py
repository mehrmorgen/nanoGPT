from __future__ import annotations

import io
import logging
from pathlib import Path
import pickle
import tarfile
import tempfile
from typing import cast

from hypothesis import given, settings, strategies as st
import pytest

from ml_playground.framework.configuration.models import PreparerConfig
from ml_playground.framework.core.error_handling import DataError
from ml_playground.experiments.bundestag_char.preparer import BundestagCharPreparer

_DEFAULT_REPO = "PolMine/GermaParlTEI"
_DEFAULT_REF = "main"

_TEXT = st.text(
    alphabet=st.characters(
        min_codepoint=32,
        max_codepoint=126,
        blacklist_characters="<>&",
    ),
    min_size=1,
    max_size=120,
)
_SPLIT = st.floats(min_value=0.0, max_value=1.0, allow_nan=False, allow_infinity=False)


def _tei_xml(paragraph: str) -> str:
    return f"""<?xml version=\"1.0\" encoding=\"UTF-8\"?>
<TEI>
  <text><body>
    <div type=\"agenda_item\" n=\"U1\">
      <sp who=\"Alice\" party=\"SPD\">
        <speaker>Alice</speaker>
        <p>{paragraph}</p>
      </sp>
    </div>
  </body></text>
</TEI>
"""


def _tarball_with_xml(files: dict[str, str]) -> bytes:
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w:gz") as archive:
        for rel_path, content in files.items():
            full_path = f"GermaParlTEI-main/{rel_path}"
            data = content.encode("utf-8")
            info = tarfile.TarInfo(name=full_path)
            info.size = len(data)
            archive.addfile(info, io.BytesIO(data))
    return buf.getvalue()


def _cache_tarball(exp_dir: Path, *, head_sha: str, text: str) -> None:
    cache_dir = exp_dir / "raw" / "germaparl_cache"
    cache_dir.mkdir(parents=True, exist_ok=True)
    path = (
        cache_dir
        / f"{_DEFAULT_REPO.replace('/', '_')}-{_DEFAULT_REF}-{head_sha}.tar.gz"
    )
    path.write_bytes(_tarball_with_xml({"01/BT_01_001.xml": _tei_xml(text)}))


def _cfg(
    exp_dir: Path,
    *,
    remote_head_sha: str,
    split: float = 0.9,
    overwrite_confirm: object | None = None,
) -> PreparerConfig:
    extras: dict[str, object] = {
        "dataset_dir_override": str(exp_dir),
        "germaparl_repo": _DEFAULT_REPO,
        "germaparl_ref": _DEFAULT_REF,
        "germaparl_cache_dir": str(exp_dir / "raw" / "germaparl_cache"),
        "split": split,
        "__remote_head_resolver": lambda _repo, _ref: remote_head_sha,
    }
    if overwrite_confirm is not None:
        extras["overwrite_confirm"] = overwrite_confirm
    return PreparerConfig(
        tokenizer_type="char",
        logger=logging.getLogger(__name__),
        extras=extras,
    )


def _load_meta(meta_path: Path) -> dict[str, object]:
    with meta_path.open("rb") as handle:
        return pickle.load(handle)


@given(text=_TEXT)
@settings(max_examples=20, deadline=None)
def test_prepare_idempotent_for_stable_remote_sha(text: str) -> None:
    with tempfile.TemporaryDirectory() as tmp:
        exp_dir = Path(tmp) / "bundestag_char"
        exp_dir.mkdir(parents=True)
        _cache_tarball(exp_dir, head_sha="sha1", text=text)

        preparer = BundestagCharPreparer()
        preparer.prepare(_cfg(exp_dir, remote_head_sha="sha1"))
        second = preparer.prepare(_cfg(exp_dir, remote_head_sha="sha1"))

        assert len(second.skipped_files) == 4
        assert any("skipping" in message for message in second.messages)


@given(text=_TEXT, split=_SPLIT)
@settings(max_examples=20, deadline=None)
def test_prepare_token_conservation_for_valid_splits(text: str, split: float) -> None:
    with tempfile.TemporaryDirectory() as tmp:
        exp_dir = Path(tmp) / "bundestag_char"
        exp_dir.mkdir(parents=True)
        _cache_tarball(exp_dir, head_sha="sha1", text=text)

        preparer = BundestagCharPreparer()
        preparer.prepare(_cfg(exp_dir, remote_head_sha="sha1", split=split))

        ds_dir = exp_dir / "datasets"
        input_text = (ds_dir / "input.txt").read_text(encoding="utf-8")
        meta = _load_meta(ds_dir / "meta.pkl")
        train_tokens = cast(int, meta["train_tokens"])
        val_tokens = cast(int, meta["val_tokens"])

        assert train_tokens + val_tokens == len(input_text)


@given(old_text=_TEXT, new_text=_TEXT, accept=st.booleans())
@settings(max_examples=20, deadline=None)
def test_prepare_remote_sha_change_switches_to_overwrite_branch(
    old_text: str, new_text: str, accept: bool
) -> None:
    with tempfile.TemporaryDirectory() as tmp:
        exp_dir = Path(tmp) / "bundestag_char"
        exp_dir.mkdir(parents=True)
        _cache_tarball(exp_dir, head_sha="sha1", text=old_text)
        _cache_tarball(exp_dir, head_sha="sha2", text=new_text)

        preparer = BundestagCharPreparer()
        preparer.prepare(_cfg(exp_dir, remote_head_sha="sha1"))

        prompts: list[str] = []

        def _confirm(prompt: str) -> bool:
            prompts.append(prompt)
            return accept

        if accept:
            preparer.prepare(
                _cfg(exp_dir, remote_head_sha="sha2", overwrite_confirm=_confirm)
            )
            meta = _load_meta(exp_dir / "datasets" / "meta.pkl")
            assert meta["source_head_sha"] == "sha2"
        else:
            with pytest.raises(DataError, match="cancelled"):
                preparer.prepare(
                    _cfg(exp_dir, remote_head_sha="sha2", overwrite_confirm=_confirm)
                )

        assert prompts
