from __future__ import annotations

import io
import logging
from pathlib import Path
import pickle
import tarfile
from typing import Callable, cast

import pytest

from ml_playground.framework.configuration.models import PreparerConfig
from ml_playground.framework.core.error_handling import DataError
from ml_playground.experiments.bundestag_char.preparer import (
    BundestagCharPreparer,
    artifacts_look_valid,
)

_DEFAULT_REPO = "PolMine/GermaParlTEI"
_DEFAULT_REF = "main"


def _minimal_tei(
    *, speaker: str = "Alice", paragraph: str = "Hallo", stage: str = "(Beifall)"
) -> str:
    return f"""<?xml version=\"1.0\" encoding=\"UTF-8\"?>
<TEI>
  <text>
    <body>
      <div type=\"agenda_item\" n=\"U1\">
        <sp who=\"Alice\" party=\"SPD\" role=\"mp\">
          <speaker>{speaker}</speaker>
          <p>{paragraph}</p>
          <stage type=\"interjection\">{stage}</stage>
        </sp>
      </div>
    </body>
  </text>
</TEI>
"""


def _tarball_with_xml_files(files: dict[str, str]) -> bytes:
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w:gz") as archive:
        for rel_path, content in files.items():
            full_path = f"GermaParlTEI-main/{rel_path}"
            data = content.encode("utf-8")
            info = tarfile.TarInfo(name=full_path)
            info.size = len(data)
            archive.addfile(info, io.BytesIO(data))
    return buf.getvalue()


def _cache_tarball(
    exp_dir: Path,
    *,
    head_sha: str,
    files: dict[str, str],
    repo: str = _DEFAULT_REPO,
    ref: str = _DEFAULT_REF,
) -> Path:
    cache_dir = exp_dir / "raw" / "germaparl_cache"
    cache_dir.mkdir(parents=True, exist_ok=True)
    tarball_path = cache_dir / f"{repo.replace('/', '_')}-{ref}-{head_sha}.tar.gz"
    tarball_path.write_bytes(_tarball_with_xml_files(files))
    return tarball_path


def _cfg(
    exp_dir: Path,
    *,
    remote_head_sha: str,
    extras: dict[str, object] | None = None,
    tokenizer_type: str = "char",
    overwrite_confirm: Callable[[str], bool] | None = None,
) -> PreparerConfig:
    merged_extras: dict[str, object] = {
        "dataset_dir_override": str(exp_dir),
        "germaparl_repo": _DEFAULT_REPO,
        "germaparl_ref": _DEFAULT_REF,
        "germaparl_cache_dir": str(exp_dir / "raw" / "germaparl_cache"),
        "__remote_head_resolver": lambda _repo, _ref: remote_head_sha,
    }
    if overwrite_confirm is not None:
        merged_extras["overwrite_confirm"] = overwrite_confirm
    if extras:
        merged_extras.update(extras)

    return PreparerConfig(
        tokenizer_type=tokenizer_type,  # type: ignore[arg-type]
        logger=logging.getLogger(__name__),
        extras=merged_extras,
    )


def _load_meta(path: Path) -> dict[str, object]:
    with path.open("rb") as handle:
        return pickle.load(handle)


def test_bundestag_char_preparer_builds_minimal_metadata(tmp_path: Path) -> None:
    exp_dir = tmp_path / "bundestag_char"
    exp_dir.mkdir(parents=True)

    _cache_tarball(
        exp_dir,
        head_sha="sha1",
        files={
            "01/BT_01_001.xml": _minimal_tei(paragraph="ABCDE"),
            "01/BT_01_002.xml": _minimal_tei(paragraph="VWXYZ"),
        },
    )

    cfg = _cfg(exp_dir, remote_head_sha="sha1", extras={"split": 0.8})
    report = BundestagCharPreparer().prepare(cfg)

    ds_dir = exp_dir / "datasets"
    input_text = (ds_dir / "input.txt").read_text(encoding="utf-8")
    meta = _load_meta(ds_dir / "meta.pkl")

    assert any("prepared dataset" in msg for msg in report.messages)
    assert set(meta.keys()) == {
        "meta_version",
        "tokenizer_type",
        "tokenizer",
        "vocab_size",
        "stoi",
        "itos",
        "train_tokens",
        "val_tokens",
        "source_head_sha",
        "source_repo",
        "source_ref",
    }
    train_tokens = cast(int, meta["train_tokens"])
    val_tokens = cast(int, meta["val_tokens"])
    assert train_tokens + val_tokens == len(input_text)
    assert meta["source_head_sha"] == "sha1"
    assert meta["source_repo"] == _DEFAULT_REPO
    assert meta["source_ref"] == _DEFAULT_REF


def test_bundestag_char_preparer_skips_when_remote_head_unchanged(
    tmp_path: Path,
) -> None:
    exp_dir = tmp_path / "bundestag_char"
    exp_dir.mkdir(parents=True)

    _cache_tarball(
        exp_dir,
        head_sha="sha1",
        files={"01/BT_01_001.xml": _minimal_tei(paragraph="SAME")},
    )

    preparer = BundestagCharPreparer()
    preparer.prepare(_cfg(exp_dir, remote_head_sha="sha1"))

    report = preparer.prepare(_cfg(exp_dir, remote_head_sha="sha1"))
    assert len(report.skipped_files) == 4
    assert any("skipping" in msg for msg in report.messages)


def test_bundestag_char_preparer_requires_overwrite_callback_on_head_change(
    tmp_path: Path,
) -> None:
    exp_dir = tmp_path / "bundestag_char"
    exp_dir.mkdir(parents=True)

    _cache_tarball(
        exp_dir,
        head_sha="sha1",
        files={"01/BT_01_001.xml": _minimal_tei(paragraph="OLD")},
    )
    _cache_tarball(
        exp_dir,
        head_sha="sha2",
        files={"01/BT_01_001.xml": _minimal_tei(paragraph="NEW")},
    )

    preparer = BundestagCharPreparer()
    preparer.prepare(_cfg(exp_dir, remote_head_sha="sha1"))

    with pytest.raises(DataError, match="explicit overwrite permission"):
        preparer.prepare(_cfg(exp_dir, remote_head_sha="sha2"))


def test_bundestag_char_preparer_declined_overwrite_preserves_artifacts(
    tmp_path: Path,
) -> None:
    exp_dir = tmp_path / "bundestag_char"
    exp_dir.mkdir(parents=True)

    _cache_tarball(
        exp_dir,
        head_sha="sha1",
        files={"01/BT_01_001.xml": _minimal_tei(paragraph="OLD")},
    )
    _cache_tarball(
        exp_dir,
        head_sha="sha2",
        files={"01/BT_01_001.xml": _minimal_tei(paragraph="NEW")},
    )

    preparer = BundestagCharPreparer()
    preparer.prepare(_cfg(exp_dir, remote_head_sha="sha1"))
    ds_dir = exp_dir / "datasets"
    before_input = (ds_dir / "input.txt").read_text(encoding="utf-8")
    before_meta = _load_meta(ds_dir / "meta.pkl")

    prompts: list[str] = []

    def _deny(prompt: str) -> bool:
        prompts.append(prompt)
        return False

    with pytest.raises(DataError, match="cancelled"):
        preparer.prepare(_cfg(exp_dir, remote_head_sha="sha2", overwrite_confirm=_deny))

    after_input = (ds_dir / "input.txt").read_text(encoding="utf-8")
    after_meta = _load_meta(ds_dir / "meta.pkl")
    assert prompts
    assert before_input == after_input
    assert before_meta["source_head_sha"] == after_meta["source_head_sha"] == "sha1"


def test_bundestag_char_preparer_accepts_overwrite_and_updates_head(
    tmp_path: Path,
) -> None:
    exp_dir = tmp_path / "bundestag_char"
    exp_dir.mkdir(parents=True)

    _cache_tarball(
        exp_dir,
        head_sha="sha1",
        files={"01/BT_01_001.xml": _minimal_tei(paragraph="OLD")},
    )
    _cache_tarball(
        exp_dir,
        head_sha="sha2",
        files={"01/BT_01_001.xml": _minimal_tei(paragraph="NEW")},
    )

    preparer = BundestagCharPreparer()
    preparer.prepare(_cfg(exp_dir, remote_head_sha="sha1"))

    prompts: list[str] = []

    def _accept(prompt: str) -> bool:
        prompts.append(prompt)
        return True

    preparer.prepare(_cfg(exp_dir, remote_head_sha="sha2", overwrite_confirm=_accept))

    ds_dir = exp_dir / "datasets"
    text = (ds_dir / "input.txt").read_text(encoding="utf-8")
    meta = _load_meta(ds_dir / "meta.pkl")

    assert prompts
    assert "NEW" in text
    assert "OLD" not in text
    assert meta["source_head_sha"] == "sha2"


def test_bundestag_char_preparer_missing_source_head_is_treated_as_stale(
    tmp_path: Path,
) -> None:
    exp_dir = tmp_path / "bundestag_char"
    exp_dir.mkdir(parents=True)

    _cache_tarball(
        exp_dir,
        head_sha="sha1",
        files={"01/BT_01_001.xml": _minimal_tei(paragraph="DATA")},
    )

    preparer = BundestagCharPreparer()
    preparer.prepare(_cfg(exp_dir, remote_head_sha="sha1"))

    meta_path = exp_dir / "datasets" / "meta.pkl"
    meta = _load_meta(meta_path)
    meta.pop("source_head_sha", None)
    with meta_path.open("wb") as handle:
        pickle.dump(meta, handle)

    prompts: list[str] = []

    def _deny(prompt: str) -> bool:
        prompts.append(prompt)
        return False

    with pytest.raises(DataError, match="cancelled"):
        preparer.prepare(_cfg(exp_dir, remote_head_sha="sha1", overwrite_confirm=_deny))

    assert prompts
    assert "<missing>" in prompts[0]


def test_bundestag_char_preparer_malformed_xml_fails_fast(tmp_path: Path) -> None:
    exp_dir = tmp_path / "bundestag_char"
    exp_dir.mkdir(parents=True)

    _cache_tarball(
        exp_dir,
        head_sha="sha1",
        files={"01/BT_01_001.xml": "<TEI><broken></TEI>"},
    )

    with pytest.raises(DataError, match="Malformed TEI XML"):
        BundestagCharPreparer().prepare(_cfg(exp_dir, remote_head_sha="sha1"))


def test_bundestag_char_preparer_rejects_invalid_split_ratio(tmp_path: Path) -> None:
    exp_dir = tmp_path / "bundestag_char"
    exp_dir.mkdir(parents=True)

    _cache_tarball(
        exp_dir,
        head_sha="sha1",
        files={"01/BT_01_001.xml": _minimal_tei(paragraph="A")},
    )

    with pytest.raises(DataError, match="split ratio must be within"):
        BundestagCharPreparer().prepare(
            _cfg(exp_dir, remote_head_sha="sha1", extras={"split": 1.2})
        )


def test_bundestag_char_preparer_raises_on_wrong_tokenizer(tmp_path: Path) -> None:
    exp_dir = tmp_path / "bundestag_char"
    exp_dir.mkdir(parents=True)

    _cache_tarball(
        exp_dir,
        head_sha="sha1",
        files={"01/BT_01_001.xml": _minimal_tei(paragraph="A")},
    )

    with pytest.raises(DataError, match="only supports char tokenizer"):
        BundestagCharPreparer().prepare(
            _cfg(exp_dir, remote_head_sha="sha1", tokenizer_type="tiktoken")
        )


def test_artifacts_look_valid_returns_true_for_valid_files(tmp_path: Path) -> None:
    file1 = tmp_path / "file1.bin"
    file2 = tmp_path / "file2.bin"
    file1.write_bytes(b"data")
    file2.write_bytes(b"more data")

    assert artifacts_look_valid([file1, file2]) is True


def test_artifacts_look_valid_returns_false_for_missing_or_empty(
    tmp_path: Path,
) -> None:
    file1 = tmp_path / "file1.bin"
    file2 = tmp_path / "file2.bin"
    file1.write_bytes(b"data")

    assert artifacts_look_valid([file1, file2]) is False

    file2.write_bytes(b"")
    assert artifacts_look_valid([file1, file2]) is False
