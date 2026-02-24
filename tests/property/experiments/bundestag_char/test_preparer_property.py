from __future__ import annotations

import io
import logging
from pathlib import Path
import pickle
import tarfile

from hypothesis import HealthCheck, given, settings, strategies as st

from ml_playground.framework.configuration.models import PreparerConfig
from ml_playground.experiments.bundestag_char.preparer import BundestagCharPreparer

_XML_SAFE_CHARS = st.characters(
    min_codepoint=32,
    max_codepoint=126,
    blacklist_characters=["<", ">", "&"],
)


def _tei_xml(paragraph: str) -> str:
    return f"""<?xml version=\"1.0\" encoding=\"UTF-8\"?>
<TEI>
  <text>
    <body>
      <div>
        <sp who=\"Alice\"><speaker>Alice</speaker><p>{paragraph}</p></sp>
      </div>
    </body>
  </text>
</TEI>
"""


def _tarball_with_paragraph(paragraph: str) -> bytes:
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w:gz") as archive:
        data = _tei_xml(paragraph).encode("utf-8")
        info = tarfile.TarInfo(name="GermaParlTEI-main/01/BT_01_001.xml")
        info.size = len(data)
        archive.addfile(info, io.BytesIO(data))
    return buf.getvalue()


def _source_extras(*, tarball_bytes: bytes, remote_head_sha: str) -> dict[str, object]:
    def _resolve_head(repo: str, ref: str) -> str:
        _ = repo, ref
        return remote_head_sha

    return {
        "remote_head_resolver": _resolve_head,
        "germaparl_tarball_bytes": tarball_bytes,
    }


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


@settings(
    max_examples=10,
    deadline=None,
    derandomize=True,
    suppress_health_check=[HealthCheck.function_scoped_fixture],
)
@given(  # type: ignore[reportAny]
    paragraph=st.text(min_size=1, max_size=20, alphabet=_XML_SAFE_CHARS)
)
def test_prepare_when_head_unchanged_then_repeated_run_skips(
    paragraph: str,
    tmp_path: Path,
) -> None:
    exp_dir = tmp_path / "bundestag_char"
    tarball = _tarball_with_paragraph(paragraph)

    preparer = BundestagCharPreparer()
    preparer.prepare(
        _cfg(
            exp_dir,
            extras=_source_extras(tarball_bytes=tarball, remote_head_sha="sha-pbt"),
        )
    )
    report = preparer.prepare(
        _cfg(
            exp_dir,
            extras={
                **_source_extras(tarball_bytes=tarball, remote_head_sha="sha-pbt"),
                "overwrite_confirm": lambda _msg: True,
            },
        )
    )

    assert len(report.skipped_files) == 3
    assert len(report.created_files) == 0
    assert len(report.updated_files) == 0


@settings(
    max_examples=10,
    deadline=None,
    derandomize=True,
    suppress_health_check=[HealthCheck.function_scoped_fixture],
)
@given(  # type: ignore[reportAny]
    paragraph=st.text(min_size=1, max_size=24, alphabet=_XML_SAFE_CHARS),
    split=st.floats(
        min_value=0.0, max_value=1.0, allow_nan=False, allow_infinity=False
    ),
)
def test_prepare_when_split_in_bounds_then_token_accounting_holds(
    paragraph: str,
    split: float,
    tmp_path: Path,
) -> None:
    exp_dir = tmp_path / "bundestag_char"
    tarball = _tarball_with_paragraph(paragraph)

    BundestagCharPreparer().prepare(
        _cfg(
            exp_dir,
            extras={
                **_source_extras(tarball_bytes=tarball, remote_head_sha="sha-split"),
                "split": split,
                "overwrite_confirm": lambda _msg: True,
            },
        )
    )

    ds_dir = exp_dir / "datasets"
    text = (ds_dir / "input.txt").read_text(encoding="utf-8")
    meta = _load_meta(ds_dir / "meta.pkl")
    train_tokens = meta["train_tokens"]
    val_tokens = meta["val_tokens"]
    assert isinstance(train_tokens, int)
    assert isinstance(val_tokens, int)
    assert train_tokens + val_tokens == len(text)
