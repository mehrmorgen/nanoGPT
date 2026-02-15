from __future__ import annotations

import io
import logging
from pathlib import Path
import pickle
import tarfile
from typing import cast

import numpy as np
import pytest

from ml_playground.framework.configuration.models import PreparerConfig
from ml_playground.framework.core.error_handling import DataError
from ml_playground.experiments.bundestag_char.preparer import (
    BundestagCharPreparer,
    artifacts_look_valid,
)


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


def _cfg(
    exp_dir: Path,
    *,
    extras: dict[str, object] | None = None,
    tokenizer_type: str = "char",
) -> PreparerConfig:
    return PreparerConfig(
        tokenizer_type=tokenizer_type,  # type: ignore[arg-type]
        logger=logging.getLogger(__name__),
        extras={"dataset_dir_override": str(exp_dir), **(extras or {})},
    )


def _load_meta(path: Path) -> dict[str, object]:
    with path.open("rb") as handle:
        return pickle.load(handle)


def test_bundestag_char_preparer_auto_prefers_local_seed(tmp_path: Path) -> None:
    exp_dir = tmp_path / "bundestag_char"
    ds_dir = exp_dir / "datasets"
    ds_dir.mkdir(parents=True)
    local_text = "LOCAL TEXT"
    (ds_dir / "input.txt").write_text(local_text, encoding="utf-8")

    # Invalid tarball should never be read because local seed is present.
    cfg = _cfg(
        exp_dir,
        extras={
            "dataset_source": "auto",
            "germaparl_tarball_bytes": b"not a tarball",
        },
    )

    report = BundestagCharPreparer().prepare(cfg)

    assert (ds_dir / "train.bin").exists()
    assert (ds_dir / "val.bin").exists()
    assert (ds_dir / "meta.pkl").exists()
    assert (ds_dir / "input.txt").read_text(encoding="utf-8") == local_text
    assert any("prepared dataset" in msg for msg in report.messages)


def test_bundestag_char_preparer_auto_falls_back_to_germaparl(tmp_path: Path) -> None:
    exp_dir = tmp_path / "bundestag_char"
    exp_dir.mkdir(parents=True)
    tarball = _tarball_with_xml_files(
        {"01/BT_01_001.xml": _minimal_tei(paragraph="REMOTE")}
    )

    cfg = _cfg(
        exp_dir,
        extras={
            "dataset_source": "auto",
            "germaparl_tarball_bytes": tarball,
        },
    )

    BundestagCharPreparer().prepare(cfg)

    text = (exp_dir / "datasets" / "input.txt").read_text(encoding="utf-8")
    assert '<DOC id="BT_01_001">' in text
    assert "<P>REMOTE</P>" in text


def test_bundestag_char_preparer_seed_mode_fails_without_local_input(
    tmp_path: Path,
) -> None:
    exp_dir = tmp_path / "bundestag_char"
    exp_dir.mkdir(parents=True)

    cfg = _cfg(exp_dir, extras={"dataset_source": "seed", "seed_policy": "fail_fast"})

    with pytest.raises(FileNotFoundError):
        BundestagCharPreparer().prepare(cfg)


def test_bundestag_char_preparer_rejects_invalid_dataset_source(tmp_path: Path) -> None:
    exp_dir = tmp_path / "bundestag_char"
    exp_dir.mkdir(parents=True)

    cfg = _cfg(exp_dir, extras={"dataset_source": "invalid"})

    with pytest.raises(DataError, match="Unsupported dataset_source"):
        BundestagCharPreparer().prepare(cfg)


def test_bundestag_char_preparer_rejects_invalid_split_ratio(tmp_path: Path) -> None:
    exp_dir = tmp_path / "bundestag_char"
    exp_dir.mkdir(parents=True)
    tarball = _tarball_with_xml_files({"01/BT_01_001.xml": _minimal_tei(paragraph="A")})

    cfg = _cfg(
        exp_dir,
        extras={
            "dataset_source": "germaparl_tei",
            "germaparl_tarball_bytes": tarball,
            "split": 1.2,
        },
    )

    with pytest.raises(DataError, match="split ratio must be within"):
        BundestagCharPreparer().prepare(cfg)


def test_bundestag_char_preparer_rejects_non_numeric_split(tmp_path: Path) -> None:
    exp_dir = tmp_path / "bundestag_char"
    exp_dir.mkdir(parents=True)
    tarball = _tarball_with_xml_files({"01/BT_01_001.xml": _minimal_tei(paragraph="A")})

    cfg = _cfg(
        exp_dir,
        extras={
            "dataset_source": "germaparl_tei",
            "germaparl_tarball_bytes": tarball,
            "split": {"bad": "value"},
        },
    )

    with pytest.raises(DataError, match="Invalid split ratio"):
        BundestagCharPreparer().prepare(cfg)


def test_bundestag_char_preparer_germaparl_mode_bypasses_local_input(
    tmp_path: Path,
) -> None:
    exp_dir = tmp_path / "bundestag_char"
    ds_dir = exp_dir / "datasets"
    ds_dir.mkdir(parents=True)
    (ds_dir / "input.txt").write_text("LOCAL", encoding="utf-8")

    tarball = _tarball_with_xml_files(
        {"01/BT_01_001.xml": _minimal_tei(paragraph="REMOTE")}
    )
    cfg = _cfg(
        exp_dir,
        extras={
            "dataset_source": "germaparl_tei",
            "germaparl_tarball_bytes": tarball,
        },
    )

    BundestagCharPreparer().prepare(cfg)

    text = (ds_dir / "input.txt").read_text(encoding="utf-8")
    assert "REMOTE" in text
    assert "LOCAL" not in text


def test_bundestag_char_preparer_stage_toggle(tmp_path: Path) -> None:
    exp_dir = tmp_path / "bundestag_char"
    exp_dir.mkdir(parents=True)
    tarball = _tarball_with_xml_files(
        {"01/BT_01_001.xml": _minimal_tei(stage="(Zwischenruf)")}
    )

    cfg = _cfg(
        exp_dir,
        extras={
            "dataset_source": "germaparl_tei",
            "germaparl_tarball_bytes": tarball,
            "germaparl_include_stage": False,
        },
    )

    BundestagCharPreparer().prepare(cfg)

    text = (exp_dir / "datasets" / "input.txt").read_text(encoding="utf-8")
    assert "<STAGE" not in text


def test_bundestag_char_preparer_speaker_attrs_toggle(tmp_path: Path) -> None:
    exp_dir = tmp_path / "bundestag_char"
    exp_dir.mkdir(parents=True)
    tarball = _tarball_with_xml_files({"01/BT_01_001.xml": _minimal_tei()})

    cfg = _cfg(
        exp_dir,
        extras={
            "dataset_source": "germaparl_tei",
            "germaparl_tarball_bytes": tarball,
            "germaparl_include_speaker_attrs": False,
        },
    )

    BundestagCharPreparer().prepare(cfg)

    text = (exp_dir / "datasets" / "input.txt").read_text(encoding="utf-8")
    assert "<SP>" in text
    assert "who=" not in text


def test_bundestag_char_preparer_malformed_xml_fails_fast(tmp_path: Path) -> None:
    exp_dir = tmp_path / "bundestag_char"
    exp_dir.mkdir(parents=True)
    bad_tarball = _tarball_with_xml_files({"01/BT_01_001.xml": "<TEI><broken></TEI>"})

    cfg = _cfg(
        exp_dir,
        extras={
            "dataset_source": "germaparl_tei",
            "germaparl_tarball_bytes": bad_tarball,
        },
    )

    with pytest.raises(DataError, match="Malformed TEI XML"):
        BundestagCharPreparer().prepare(cfg)


def test_bundestag_char_preparer_streaming_metadata_and_split(tmp_path: Path) -> None:
    exp_dir = tmp_path / "bundestag_char"
    exp_dir.mkdir(parents=True)

    xml_files = {
        "01/BT_01_001.xml": _minimal_tei(speaker="Alice", paragraph="abcde", stage="x"),
        "01/BT_01_002.xml": _minimal_tei(speaker="Bob", paragraph="vwxyz", stage="y"),
    }
    tarball = _tarball_with_xml_files(xml_files)

    cfg = _cfg(
        exp_dir,
        extras={
            "dataset_source": "germaparl_tei",
            "germaparl_tarball_bytes": tarball,
            "split": 0.8,
        },
    )

    BundestagCharPreparer().prepare(cfg)

    ds_dir = exp_dir / "datasets"
    input_text = (ds_dir / "input.txt").read_text(encoding="utf-8")
    meta = _load_meta(ds_dir / "meta.pkl")

    train_tokens_obj = meta["train_tokens"]
    val_tokens_obj = meta["val_tokens"]
    stoi_obj = meta["stoi"]
    source_files_processed_obj = meta["source_files_processed"]

    assert isinstance(train_tokens_obj, int)
    assert isinstance(val_tokens_obj, int)
    assert isinstance(stoi_obj, dict)
    assert isinstance(source_files_processed_obj, int)

    train_tokens = train_tokens_obj
    val_tokens = val_tokens_obj
    stoi = cast(dict[str, int], stoi_obj)

    assert train_tokens + val_tokens == len(input_text)
    assert list(stoi.keys()) == sorted(stoi.keys())
    assert source_files_processed_obj == 2
    assert (ds_dir / "train.bin").stat().st_size > 0
    assert (ds_dir / "val.bin").stat().st_size > 0

    train_arr = np.fromfile(ds_dir / "train.bin", dtype=np.uint16)
    val_arr = np.fromfile(ds_dir / "val.bin", dtype=np.uint16)
    assert len(train_arr) == train_tokens
    assert len(val_arr) == val_tokens


def test_bundestag_char_preparer_uses_existing_raw_text_path_when_present(
    tmp_path: Path,
) -> None:
    exp_dir = tmp_path / "bundestag_char"
    ds_dir = exp_dir / "datasets"
    ds_dir.mkdir(parents=True)
    (ds_dir / "input.txt").write_text("seed", encoding="utf-8")

    raw_path = tmp_path / "raw_override.txt"
    raw_path.write_text("RAW-OVERRIDE", encoding="utf-8")

    cfg = PreparerConfig(
        tokenizer_type="char",
        raw_text_path=raw_path,
        logger=logging.getLogger(__name__),
        extras={"dataset_dir_override": str(exp_dir), "dataset_source": "seed"},
    )
    BundestagCharPreparer().prepare(cfg)

    meta = _load_meta(ds_dir / "meta.pkl")
    train_tokens_obj = meta["train_tokens"]
    val_tokens_obj = meta["val_tokens"]
    assert isinstance(train_tokens_obj, int)
    assert isinstance(val_tokens_obj, int)
    assert train_tokens_obj + val_tokens_obj == len("RAW-OVERRIDE")


def test_bundestag_char_preparer_raises_on_wrong_tokenizer(tmp_path: Path) -> None:
    exp_dir = tmp_path / "bundestag_char"
    ds_dir = exp_dir / "datasets"
    ds_dir.mkdir(parents=True)
    (ds_dir / "input.txt").write_text("Test data.", encoding="utf-8")

    cfg = _cfg(exp_dir, tokenizer_type="tiktoken")

    with pytest.raises(ValueError, match="only supports char tokenizer"):
        BundestagCharPreparer().prepare(cfg)


def test_bundestag_char_preparer_skips_if_valid(tmp_path: Path) -> None:
    exp_dir = tmp_path / "bundestag_char"
    ds_dir = exp_dir / "datasets"
    ds_dir.mkdir(parents=True)
    (ds_dir / "train.bin").write_bytes(b"train")
    (ds_dir / "val.bin").write_bytes(b"val")
    (ds_dir / "meta.pkl").write_bytes(b"meta")

    report = BundestagCharPreparer().prepare(_cfg(exp_dir))

    assert len(report.skipped_files) == 3
    assert len(report.created_files) == 0
    assert any("skipping" in msg for msg in report.messages)


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
