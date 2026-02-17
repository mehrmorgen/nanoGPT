from __future__ import annotations

import logging
from pathlib import Path
import pickle
import hashlib
from typing import Any, Callable, ContextManager

import pytest
from hypothesis import HealthCheck, given, settings, strategies as st

from ml_playground.experiments.bundestag_char import preparer as preparer_module
from ml_playground.experiments.bundestag_char.preparer import BundestagCharPreparer
from ml_playground.framework.configuration.models import PreparerConfig
from ml_playground.framework.core.error_handling import DataError


def _cfg(exp_dir: Path, *, extras: dict[str, object] | None = None) -> PreparerConfig:
    return PreparerConfig(
        tokenizer_type="char",
        logger=logging.getLogger(__name__),
        extras={"dataset_dir_override": str(exp_dir), **(extras or {})},
    )


def _load_meta(path: Path) -> dict[str, object]:
    with path.open("rb") as handle:
        payload = pickle.load(handle)
    assert isinstance(payload, dict)
    return payload


def _meta_int(meta: dict[str, object], key: str) -> int:
    value = meta[key]
    assert isinstance(value, int)
    return value


@given(
    text=st.text(
        alphabet=st.characters(blacklist_categories=("Cs",)),
        min_size=8,
        max_size=64,
    ),
    split=st.floats(
        min_value=0.2, max_value=0.8, allow_nan=False, allow_infinity=False
    ),
)
@settings(
    max_examples=25,
    deadline=None,
    suppress_health_check=[HealthCheck.function_scoped_fixture],
)
def test_token_conservation_property(
    tmp_path: Path,
    override_attr: Callable[[object, str, object], ContextManager[None]],
    text: str,
    split: float,
) -> None:
    digest = hashlib.sha1(f"{split}:{text}".encode("utf-8")).hexdigest()[:12]
    exp_dir = tmp_path / f"bundestag_char_{digest}"

    def _serialize(_tar: Path, dst: Path, **_kwargs: Any) -> int:
        dst.parent.mkdir(parents=True, exist_ok=True)
        dst.write_text(text, encoding="utf-8")
        return 1

    with override_attr(
        preparer_module, "resolve_remote_head_sha", lambda _r, _f: "sha1"
    ):
        with override_attr(
            preparer_module,
            "ensure_germaparl_tarball",
            lambda cache_dir, **_kwargs: cache_dir / "fake.tar.gz",
        ):
            with override_attr(
                preparer_module, "serialize_germaparl_tei_to_text", _serialize
            ):
                cfg = _cfg(exp_dir, extras={"split": split})
                BundestagCharPreparer().prepare(cfg)

    meta = _load_meta(exp_dir / "datasets" / "meta.pkl")
    assert _meta_int(meta, "train_tokens") + _meta_int(meta, "val_tokens") == len(text)


def test_idempotent_skip_when_remote_sha_stable(
    tmp_path: Path,
    override_attr: Callable[[object, str, object], ContextManager[None]],
) -> None:
    exp_dir = tmp_path / "bundestag_char"
    calls = {"serialize": 0}

    def _serialize(_tar: Path, dst: Path, **_kwargs: Any) -> int:
        calls["serialize"] += 1
        dst.parent.mkdir(parents=True, exist_ok=True)
        dst.write_text("abcdef", encoding="utf-8")
        return 1

    with override_attr(
        preparer_module, "resolve_remote_head_sha", lambda _r, _f: "stable"
    ):
        with override_attr(
            preparer_module,
            "ensure_germaparl_tarball",
            lambda cache_dir, **_kwargs: cache_dir / "fake.tar.gz",
        ):
            with override_attr(
                preparer_module, "serialize_germaparl_tei_to_text", _serialize
            ):
                cfg = _cfg(exp_dir)
                first = BundestagCharPreparer().prepare(cfg)
                second = BundestagCharPreparer().prepare(cfg)

    assert calls["serialize"] == 1
    assert first.created_files or first.updated_files
    assert second.created_files == ()
    assert second.updated_files == ()


def test_sha_change_requires_overwrite_decision(
    tmp_path: Path,
    override_attr: Callable[[object, str, object], ContextManager[None]],
) -> None:
    exp_dir = tmp_path / "bundestag_char"

    current_sha = {"value": "sha1"}

    def _resolve(_repo: str, _ref: str) -> str:
        return current_sha["value"]

    def _serialize(_tar: Path, dst: Path, **_kwargs: Any) -> int:
        dst.parent.mkdir(parents=True, exist_ok=True)
        dst.write_text("abc", encoding="utf-8")
        return 1

    with override_attr(preparer_module, "resolve_remote_head_sha", _resolve):
        with override_attr(
            preparer_module,
            "ensure_germaparl_tarball",
            lambda cache_dir, **_kwargs: cache_dir / "fake.tar.gz",
        ):
            with override_attr(
                preparer_module, "serialize_germaparl_tei_to_text", _serialize
            ):
                BundestagCharPreparer().prepare(_cfg(exp_dir))

                current_sha["value"] = "sha2"
                with pytest.raises(DataError, match="overwrite permission"):
                    BundestagCharPreparer().prepare(_cfg(exp_dir))
