from __future__ import annotations

import io
from pathlib import Path
import tarfile

import pytest
import requests.exceptions

from ml_playground.experiments.bundestag_char import germaparl_tei
from ml_playground.framework.core.error_handling import DataError


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


def _tei_xml(text: str) -> str:
    return f"""<?xml version=\"1.0\" encoding=\"UTF-8\"?>
<TEI>
  <text><body>
    <div type=\"agenda_item\" n=\"U1\">
      <sp who=\"Alice\" party=\"SPD\"><speaker>Alice</speaker><p>{text}</p></sp>
    </div>
  </body></text>
</TEI>
"""


class _JsonResponse:
    def __init__(self, payload: dict[str, object]) -> None:
        self._payload = payload

    def raise_for_status(self) -> None:
        return None

    def json(self) -> dict[str, object]:
        return self._payload


class _JsonlessResponse:
    def raise_for_status(self) -> None:
        return None


class _DownloadResponse:
    def __init__(self, chunks: list[bytes]) -> None:
        self._chunks = chunks

    def raise_for_status(self) -> None:
        return None

    def iter_content(self, *, chunk_size: int) -> list[bytes]:
        _ = chunk_size
        return self._chunks


class _ContentResponse:
    def __init__(self, content: object) -> None:
        self.content = content

    def raise_for_status(self) -> None:
        return None


def test_resolve_remote_head_sha_success() -> None:
    sha = germaparl_tei.resolve_remote_head_sha(
        "PolMine/GermaParlTEI",
        "main",
        http_get=lambda *_args, **_kwargs: _JsonResponse({"sha": "abc123"}),
    )
    assert sha == "abc123"


def test_resolve_remote_head_sha_wraps_failures() -> None:
    def _boom(*_args: object, **_kwargs: object) -> object:
        raise requests.exceptions.Timeout("timeout")

    with pytest.raises(DataError, match="Failed to resolve remote head SHA"):
        germaparl_tei.resolve_remote_head_sha(
            "PolMine/GermaParlTEI", "main", http_get=_boom
        )


def test_resolve_remote_head_sha_rejects_jsonless_response() -> None:
    with pytest.raises(DataError, match="no json\\(\\) method"):
        germaparl_tei.resolve_remote_head_sha(
            "PolMine/GermaParlTEI",
            "main",
            http_get=lambda *_args, **_kwargs: _JsonlessResponse(),
        )


def test_resolve_remote_head_sha_rejects_non_mapping_payload() -> None:
    class _ListPayload(_JsonResponse):
        def json(self) -> object:  # type: ignore[override]
            return ["not", "a", "mapping"]

    with pytest.raises(DataError, match="payload must be a JSON object"):
        germaparl_tei.resolve_remote_head_sha(
            "PolMine/GermaParlTEI",
            "main",
            http_get=lambda *_args, **_kwargs: _ListPayload({}),
        )


def test_resolve_remote_head_sha_rejects_missing_sha() -> None:
    with pytest.raises(DataError, match="valid commit sha"):
        germaparl_tei.resolve_remote_head_sha(
            "PolMine/GermaParlTEI",
            "main",
            http_get=lambda *_args, **_kwargs: _JsonResponse({"not_sha": "x"}),
        )


def test_ensure_germaparl_tarball_uses_cache(tmp_path: Path) -> None:
    cache = tmp_path / "cache"
    cache.mkdir(parents=True, exist_ok=True)
    existing = cache / "PolMine_GermaParlTEI-main.tar.gz"
    existing.write_bytes(b"cached")

    path = germaparl_tei.ensure_germaparl_tarball(
        cache,
        repo="PolMine/GermaParlTEI",
        ref="main",
        http_get=lambda *_a, **_k: (_ for _ in ()).throw(AssertionError("no call")),
    )

    assert path == existing
    assert path.read_bytes() == b"cached"


def test_ensure_germaparl_tarball_downloads_when_missing(tmp_path: Path) -> None:
    cache = tmp_path / "cache"
    path = germaparl_tei.ensure_germaparl_tarball(
        cache,
        repo="PolMine/GermaParlTEI",
        ref="main",
        http_get=lambda *_a, **_k: _DownloadResponse([b"abc", b"def"]),
    )
    assert path.exists()
    assert path.read_bytes() == b"abcdef"


def test_ensure_germaparl_tarball_uses_content_when_no_iter_content(
    tmp_path: Path,
) -> None:
    cache = tmp_path / "cache"
    path = germaparl_tei.ensure_germaparl_tarball(
        cache,
        repo="PolMine/GermaParlTEI",
        ref="main",
        http_get=lambda *_a, **_k: _ContentResponse(b"xyz"),
    )
    assert path.read_bytes() == b"xyz"


def test_ensure_germaparl_tarball_rejects_non_bytes_content(tmp_path: Path) -> None:
    cache = tmp_path / "cache"
    with pytest.raises(DataError, match="did not include bytes content"):
        germaparl_tei.ensure_germaparl_tarball(
            cache,
            repo="PolMine/GermaParlTEI",
            ref="main",
            http_get=lambda *_a, **_k: _ContentResponse("nope"),
        )


def test_ensure_germaparl_tarball_wraps_request_errors(tmp_path: Path) -> None:
    cache = tmp_path / "cache"

    def _boom(*_args: object, **_kwargs: object) -> object:
        raise requests.exceptions.ConnectionError("down")

    with pytest.raises(DataError, match="Failed to download GermaParlTEI tarball"):
        germaparl_tei.ensure_germaparl_tarball(
            cache,
            repo="PolMine/GermaParlTEI",
            ref="main",
            http_get=_boom,
        )


def test_serialize_germaparl_tei_to_text_is_deterministic(tmp_path: Path) -> None:
    tarball = _tarball_with_xml(
        {
            "01/BT_01_002.xml": _tei_xml("two"),
            "01/BT_01_001.xml": _tei_xml("one"),
        }
    )
    tar_path = tmp_path / "germaparl.tar.gz"
    tar_path.write_bytes(tarball)
    out_path = tmp_path / "input.txt"

    processed = germaparl_tei.serialize_germaparl_tei_to_text(
        tar_path,
        out_path,
        include_stage=True,
        include_speaker_attrs=True,
    )

    text = out_path.read_text(encoding="utf-8")
    assert processed == 2
    assert text.find('id="BT_01_001"') < text.find('id="BT_01_002"')


def test_serialize_germaparl_tei_to_text_fails_on_malformed_xml(tmp_path: Path) -> None:
    tarball = _tarball_with_xml({"01/BT_01_001.xml": "<TEI><broken></TEI>"})
    tar_path = tmp_path / "germaparl.tar.gz"
    tar_path.write_bytes(tarball)

    with pytest.raises(DataError, match="Malformed TEI XML"):
        germaparl_tei.serialize_germaparl_tei_to_text(
            tar_path,
            tmp_path / "input.txt",
            include_stage=True,
            include_speaker_attrs=True,
        )


def test_serialize_germaparl_tei_to_text_progress_and_toggle_fields(
    tmp_path: Path,
) -> None:
    xml = """<?xml version="1.0" encoding="UTF-8"?>
<TEI xmlns="urn:test">
  <text><body>
    <div type="agenda_item" n="U1">
      <sp who="Alice"><speaker>Alice</speaker><p>Hello</p><stage type="noise">[x]</stage></sp>
    </div>
  </body></text>
</TEI>
"""
    tarball = _tarball_with_xml({"01/BT_01_001.xml": xml})
    tar_path = tmp_path / "germaparl.tar.gz"
    tar_path.write_bytes(tarball)
    out_path = tmp_path / "input.txt"
    seen: list[tuple[int, str]] = []

    processed = germaparl_tei.serialize_germaparl_tei_to_text(
        tar_path,
        out_path,
        include_stage=False,
        include_speaker_attrs=False,
        progress_cb=lambda count, doc_id: seen.append((count, doc_id)),
    )

    text = out_path.read_text(encoding="utf-8")
    assert processed == 1
    assert seen == [(1, "BT_01_001")]
    assert "<STAGE" not in text
    assert "<SP>" in text
    assert 'who="' not in text


def test_helper_formatting_functions() -> None:
    assert germaparl_tei._local_name("{urn:test}sp") == "sp"
    assert germaparl_tei._local_name("sp") == "sp"
    assert germaparl_tei._attrs_to_str({}) == ""
    rendered = germaparl_tei._attrs_to_str({"b": "2", "a": "1"})
    assert rendered == ' a="1" b="2"'
