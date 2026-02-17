from __future__ import annotations

import logging
from pathlib import Path
import pickle
from typing import Any, Callable, ContextManager

import numpy as np
import pytest

from ml_playground.experiments.bundestag_char import preparer as preparer_module
from ml_playground.experiments.bundestag_char.preparer import (
    BundestagCharPreparer,
    artifacts_look_valid,
)
from ml_playground.framework.configuration.models import PreparerConfig
from ml_playground.framework.core.error_handling import DataError


def _cfg(exp_dir: Path, *, extras: dict[str, object] | None = None) -> PreparerConfig:
    return PreparerConfig(
        tokenizer_type="char",
        logger=logging.getLogger(__name__),
        extras={"dataset_dir_override": str(exp_dir), **(extras or {})},
    )


def _write_dataset(ds_dir: Path, *, text: str, sha: str = "sha0") -> None:
    ds_dir.mkdir(parents=True, exist_ok=True)
    train = np.array([0, 1, 2], dtype=np.uint16)
    val = np.array([3], dtype=np.uint16)
    train.tofile(ds_dir / "train.bin")
    val.tofile(ds_dir / "val.bin")
    (ds_dir / "input.txt").write_text(text, encoding="utf-8")
    meta: dict[str, object] = {
        "meta_version": 1,
        "tokenizer_type": "char",
        "tokenizer": "char",
        "vocab_size": 4,
        "stoi": {"a": 0, "b": 1, "c": 2, "d": 3},
        "itos": {0: "a", 1: "b", 2: "c", 3: "d"},
        "train_tokens": int(train.size),
        "val_tokens": int(val.size),
        "source_head_sha": sha,
        "source_repo": "PolMine/GermaParlTEI",
        "source_ref": "main",
    }
    with (ds_dir / "meta.pkl").open("wb") as handle:
        pickle.dump(meta, handle)


def _load_meta(meta_path: Path) -> dict[str, object]:
    with meta_path.open("rb") as handle:
        payload = pickle.load(handle)
    assert isinstance(payload, dict)
    return payload


def _meta_int(meta: dict[str, object], key: str) -> int:
    value = meta[key]
    assert isinstance(value, int)
    return value


def test_prepare_skips_when_remote_head_unchanged_and_artifacts_valid(
    tmp_path: Path, override_attr: Callable[[object, str, object], ContextManager[None]]
) -> None:
    exp_dir = tmp_path / "bundestag_char"
    ds_dir = exp_dir / "datasets"
    _write_dataset(ds_dir, text="abc", sha="same-sha")

    with override_attr(
        preparer_module,
        "resolve_remote_head_sha",
        lambda repo, ref: "same-sha",
    ):
        report = BundestagCharPreparer().prepare(_cfg(exp_dir))

    assert report.created_files == ()
    assert report.updated_files == ()
    assert set(report.skipped_files) == {
        ds_dir / "input.txt",
        ds_dir / "train.bin",
        ds_dir / "val.bin",
        ds_dir / "meta.pkl",
    }


def test_prepare_requires_confirmation_when_head_changes(
    tmp_path: Path, override_attr: Callable[[object, str, object], ContextManager[None]]
) -> None:
    exp_dir = tmp_path / "bundestag_char"
    ds_dir = exp_dir / "datasets"
    _write_dataset(ds_dir, text="abc", sha="old-sha")

    with override_attr(
        preparer_module, "resolve_remote_head_sha", lambda _r, _f: "new"
    ):
        cfg = _cfg(exp_dir)
        with pytest.raises(DataError, match="overwrite permission"):
            BundestagCharPreparer().prepare(cfg)


def test_prepare_deny_overwrite_keeps_existing_artifacts(
    tmp_path: Path, override_attr: Callable[[object, str, object], ContextManager[None]]
) -> None:
    exp_dir = tmp_path / "bundestag_char"
    ds_dir = exp_dir / "datasets"
    _write_dataset(ds_dir, text="abc", sha="old-sha")

    def _deny(_msg: str) -> bool:
        return False

    with override_attr(
        preparer_module, "resolve_remote_head_sha", lambda _r, _f: "new"
    ):
        cfg = _cfg(exp_dir, extras={"overwrite_confirm": _deny})
        before = (ds_dir / "input.txt").read_text(encoding="utf-8")

        with pytest.raises(DataError, match="Overwrite cancelled"):
            BundestagCharPreparer().prepare(cfg)

    assert (ds_dir / "input.txt").read_text(encoding="utf-8") == before


def test_prepare_accept_overwrite_rebuilds_and_updates_source_head(
    tmp_path: Path, override_attr: Callable[[object, str, object], ContextManager[None]]
) -> None:
    exp_dir = tmp_path / "bundestag_char"
    ds_dir = exp_dir / "datasets"
    cache_dir = exp_dir / "raw" / "germaparl_cache"
    _write_dataset(ds_dir, text="old", sha="old-sha")

    def _serialize(_tar: Path, dst: Path, **_kwargs: Any) -> int:
        dst.parent.mkdir(parents=True, exist_ok=True)
        dst.write_text("ABCDEF", encoding="utf-8")
        return 2

    prompts: list[str] = []

    def _confirm(msg: str) -> bool:
        prompts.append(msg)
        return True

    cfg = _cfg(
        exp_dir,
        extras={
            "overwrite_confirm": _confirm,
            "germaparl_cache_dir": str(cache_dir),
            "split": 0.5,
        },
    )

    with override_attr(
        preparer_module, "resolve_remote_head_sha", lambda _r, _f: "new-sha"
    ):
        with override_attr(
            preparer_module,
            "ensure_germaparl_tarball",
            lambda cache_dir, **_kwargs: cache_dir / "fake.tar.gz",
        ):
            with override_attr(
                preparer_module, "serialize_germaparl_tei_to_text", _serialize
            ):
                report = BundestagCharPreparer().prepare(cfg)

    assert prompts
    assert any("Overwrite existing prepared artifacts?" in p for p in prompts)
    assert (ds_dir / "train.bin").exists()
    assert (ds_dir / "val.bin").exists()
    meta = _load_meta(ds_dir / "meta.pkl")
    assert meta["source_head_sha"] == "new-sha"
    assert meta["source_repo"] == "PolMine/GermaParlTEI"
    assert meta["source_ref"] == "main"
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
    train_tokens = _meta_int(meta, "train_tokens")
    val_tokens = _meta_int(meta, "val_tokens")
    assert train_tokens + val_tokens == len("ABCDEF")
    assert report.created_files or report.updated_files


def test_prepare_rejects_invalid_split(
    tmp_path: Path, override_attr: Callable[[object, str, object], ContextManager[None]]
) -> None:
    exp_dir = tmp_path / "bundestag_char"
    with override_attr(
        preparer_module, "resolve_remote_head_sha", lambda _r, _f: "sha"
    ):
        cfg = _cfg(exp_dir, extras={"split": 1.0})
        with pytest.raises(DataError, match="Invalid split ratio"):
            BundestagCharPreparer().prepare(cfg)


def test_prepare_rejects_non_char_tokenizer(tmp_path: Path) -> None:
    exp_dir = tmp_path / "bundestag_char"
    cfg = PreparerConfig(
        tokenizer_type="word",
        logger=logging.getLogger(__name__),
        extras={"dataset_dir_override": str(exp_dir)},
    )
    with pytest.raises(DataError, match="only supports tokenizer_type='char'"):
        BundestagCharPreparer().prepare(cfg)


def test_prepare_surfaces_malformed_xml_errors(
    tmp_path: Path, override_attr: Callable[[object, str, object], ContextManager[None]]
) -> None:
    exp_dir = tmp_path / "bundestag_char"

    def _raise_malformed(_tar: Path, _dst: Path, **_kwargs: Any) -> int:
        raise DataError(
            "Malformed TEI XML",
            reason="XML parsing failed",
            rationale="Invalid input",
        )

    with override_attr(
        preparer_module, "resolve_remote_head_sha", lambda _r, _f: "sha"
    ):
        with override_attr(
            preparer_module,
            "ensure_germaparl_tarball",
            lambda cache_dir, **_kwargs: cache_dir / "fake.tar.gz",
        ):
            with override_attr(
                preparer_module, "serialize_germaparl_tei_to_text", _raise_malformed
            ):
                cfg = _cfg(exp_dir)
                with pytest.raises(DataError, match="Malformed TEI XML"):
                    BundestagCharPreparer().prepare(cfg)


def test_artifacts_look_valid_returns_false_for_missing_or_empty(
    tmp_path: Path,
) -> None:
    ds_dir = tmp_path / "datasets"
    out = [
        ds_dir / "input.txt",
        ds_dir / "train.bin",
        ds_dir / "val.bin",
        ds_dir / "meta.pkl",
    ]
    assert artifacts_look_valid(out) is False

    ds_dir.mkdir(parents=True, exist_ok=True)
    for path in out:
        path.touch()
    assert artifacts_look_valid(out) is False


def test_artifacts_look_valid_returns_true_for_non_empty(tmp_path: Path) -> None:
    ds_dir = tmp_path / "datasets"
    ds_dir.mkdir(parents=True, exist_ok=True)
    out = [
        ds_dir / "input.txt",
        ds_dir / "train.bin",
        ds_dir / "val.bin",
        ds_dir / "meta.pkl",
    ]
    for path in out:
        path.write_bytes(b"x")
    assert artifacts_look_valid(out) is True


def test_helper_coercion_and_path_resolvers(tmp_path: Path) -> None:
    exp_dir = tmp_path / "exp"
    assert (
        preparer_module._resolve_exp_dir({"dataset_dir_override": str(exp_dir)}, None)
        == exp_dir
    )
    assert preparer_module._resolve_exp_dir({}, exp_dir) == exp_dir
    assert (
        preparer_module._resolve_cache_dir(exp_dir, None)
        == exp_dir / "raw" / "germaparl_cache"
    )
    assert (
        preparer_module._resolve_cache_dir(exp_dir, str(tmp_path / "cache"))
        == tmp_path / "cache"
    )
    assert preparer_module._coerce_bool(None, default=True) is True
    assert preparer_module._coerce_bool(False, default=True) is False
    assert preparer_module._coerce_str(None, "main") == "main"
    assert preparer_module._coerce_str("ref", "main") == "ref"
    assert preparer_module._coerce_split(None) == preparer_module.DEFAULT_SPLIT


def test_helper_coercion_rejects_invalid_values() -> None:
    with pytest.raises(DataError, match="Invalid boolean prepare extra"):
        preparer_module._coerce_bool("yes", default=True)
    with pytest.raises(DataError, match="Invalid string prepare extra"):
        preparer_module._coerce_str("", "main")
    with pytest.raises(DataError, match="Expected numeric split ratio"):
        preparer_module._coerce_split("0.9")
    with pytest.raises(DataError, match="outside the open interval"):
        preparer_module._coerce_split(0.0)


def test_load_existing_meta_handles_errors_and_non_dict(tmp_path: Path) -> None:
    missing = tmp_path / "missing.pkl"
    assert preparer_module._load_existing_meta(missing) == {}

    bad = tmp_path / "bad.pkl"
    bad.write_bytes(b"not-a-pickle")
    assert preparer_module._load_existing_meta(bad) == {}

    scalar = tmp_path / "scalar.pkl"
    with scalar.open("wb") as handle:
        pickle.dump(123, handle)
    assert preparer_module._load_existing_meta(scalar) == {}


def test_extract_stoi_branches() -> None:
    tokenizer = preparer_module.CharTokenizer()
    fallback = preparer_module._extract_stoi({}, tokenizer)
    assert fallback == dict(sorted(tokenizer.stoi.items(), key=lambda item: item[0]))

    explicit = preparer_module._extract_stoi({"stoi": {"b": 2, "a": 1}}, tokenizer)
    assert explicit == {"a": 1, "b": 2}

    with pytest.raises(DataError, match="invalid stoi mapping"):
        preparer_module._extract_stoi({"stoi": {"a": True}}, tokenizer)


def test_overwrite_confirmation_validation_and_message() -> None:
    outputs = [Path("/tmp/input.txt"), Path("/tmp/train.bin")]
    with pytest.raises(DataError, match="overwrite permission"):
        preparer_module._require_overwrite_confirmation(
            extras={},
            repo="PolMine/GermaParlTEI",
            ref="main",
            existing_sha=None,
            remote_sha="new",
            outputs=outputs,
        )
    with pytest.raises(DataError, match="Expected callable"):
        preparer_module._require_overwrite_confirmation(
            extras={preparer_module.OVERWRITE_CONFIRM_KEY: "bad"},
            repo="PolMine/GermaParlTEI",
            ref="main",
            existing_sha=None,
            remote_sha="new",
            outputs=outputs,
        )

    prompts: list[str] = []

    def _confirm(message: str) -> bool:
        prompts.append(message)
        return True

    preparer_module._require_overwrite_confirmation(
        extras={preparer_module.OVERWRITE_CONFIRM_KEY: _confirm},
        repo="PolMine/GermaParlTEI",
        ref="main",
        existing_sha=None,
        remote_sha="new",
        outputs=outputs,
    )
    assert prompts and "Existing source_head_sha: <missing>" in prompts[0]
