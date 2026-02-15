from __future__ import annotations

import io
from pathlib import Path
import tarfile

import pytest
import requests.exceptions

from ml_playground.framework.core.error_handling import DataError
from ml_playground.experiments.bundestag_char.germaparl_tei import (
    build_codeload_url,
    ensure_germaparl_tarball,
    serialize_germaparl_tei_to_text,
)


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


class _ResponseIter:
    def __init__(self, chunks: list[bytes]) -> None:
        self._chunks = chunks

    def raise_for_status(self) -> None:
        return None

    def iter_content(self, *, chunk_size: int) -> list[bytes]:
        _ = chunk_size
        return self._chunks


class _ResponseContent:
    def __init__(self, content: bytes) -> None:
        self.content = content

    def raise_for_status(self) -> None:
        return None


class _ResponseContentNoRaise:
    def __init__(self, content: bytes) -> None:
        self.content = content


class _ResponseBadIter:
    def raise_for_status(self) -> None:
        return None

    def iter_content(self, *, chunk_size: int) -> object:
        _ = chunk_size
        return 123


class _ResponseNoBytes:
    def raise_for_status(self) -> None:
        return None

    @property
    def content(self) -> object:
        return object()


def _tei_xml(text: str) -> str:
    return f"""<?xml version=\"1.0\" encoding=\"UTF-8\"?>
<TEI>
  <text><body>
    <div type=\"agenda_item\" n=\"U1\">
      <sp who=\"Alice\" party=\"SPD\">
        <speaker>Alice</speaker>
        <p>{text}</p>
        <stage type=\"interjection\">(Beifall)</stage>
      </sp>
    </div>
  </body></text>
</TEI>
"""


def test_build_codeload_url() -> None:
    assert (
        build_codeload_url("PolMine/GermaParlTEI", "main")
        == "https://codeload.github.com/PolMine/GermaParlTEI/tar.gz/refs/heads/main"
    )


def test_ensure_germaparl_tarball_uses_injected_bytes(tmp_path: Path) -> None:
    cache_dir = tmp_path / "cache"
    tarball_bytes = _tarball_with_xml({"01/BT_01_001.xml": _tei_xml("A")})

    path = ensure_germaparl_tarball(
        cache_dir,
        repo="PolMine/GermaParlTEI",
        ref="main",
        tarball_bytes=tarball_bytes,
    )

    assert path.exists()
    assert path.read_bytes() == tarball_bytes


def test_ensure_germaparl_tarball_uses_existing_cache(tmp_path: Path) -> None:
    cache_dir = tmp_path / "cache"
    existing = ensure_germaparl_tarball(
        cache_dir,
        repo="PolMine/GermaParlTEI",
        ref="main",
        tarball_bytes=b"cached",
    )

    def _boom(*_args: object, **_kwargs: object) -> object:
        raise AssertionError("http_get must not be called when cache exists")

    path = ensure_germaparl_tarball(
        cache_dir,
        repo="PolMine/GermaParlTEI",
        ref="main",
        http_get=_boom,
    )

    assert path == existing


def test_ensure_germaparl_tarball_sanitizes_ref_in_cache_filename(
    tmp_path: Path,
) -> None:
    cache_dir = tmp_path / "cache"

    path = ensure_germaparl_tarball(
        cache_dir,
        repo="PolMine/GermaParlTEI",
        ref="feature/data-refresh",
        tarball_bytes=b"cached",
    )

    assert path.name == "PolMine_GermaParlTEI-feature_data-refresh.tar.gz"
    assert path.exists()


def test_ensure_germaparl_tarball_reports_progress_for_cache_and_download(
    tmp_path: Path,
) -> None:
    cache_dir = tmp_path / "cache"
    progress: list[str] = []

    ensure_germaparl_tarball(
        cache_dir,
        repo="PolMine/GermaParlTEI",
        ref="main",
        tarball_bytes=b"cached",
    )
    ensure_germaparl_tarball(
        cache_dir,
        repo="PolMine/GermaParlTEI",
        ref="main",
        progress=progress.append,
    )
    assert any("using cached GermaParl tarball" in msg for msg in progress)

    class _ResponseWithHeaders(_ResponseIter):
        headers: dict[str, str]

        def __init__(self) -> None:
            large_chunk = b"a" * ((128 << 20) + 2)
            super().__init__([large_chunk])
            self.headers = {"Content-Length": str(len(large_chunk))}

    def _http_get(*_args: object, **_kwargs: object) -> _ResponseWithHeaders:
        return _ResponseWithHeaders()

    progress.clear()
    ensure_germaparl_tarball(
        cache_dir,
        repo="PolMine/GermaParlTEI",
        ref="main",
        force_refresh=True,
        http_get=_http_get,
        progress=progress.append,
    )
    assert any("downloading GermaParl tarball" in msg for msg in progress)
    assert any("tarball download progress" in msg for msg in progress)
    assert any("cached GermaParl tarball at" in msg for msg in progress)


def test_ensure_germaparl_tarball_ignores_invalid_content_length_header(
    tmp_path: Path,
) -> None:
    cache_dir = tmp_path / "cache"

    class _ResponseBadLength(_ResponseIter):
        headers: dict[str, str]

        def __init__(self) -> None:
            super().__init__([b"a"])
            self.headers = {"Content-Length": "NaN"}

    def _http_get(*_args: object, **_kwargs: object) -> _ResponseBadLength:
        return _ResponseBadLength()

    path = ensure_germaparl_tarball(
        cache_dir,
        repo="PolMine/GermaParlTEI",
        ref="main",
        force_refresh=True,
        http_get=_http_get,
    )
    assert path.read_bytes() == b"a"


def test_ensure_germaparl_tarball_downloads_via_iter_content(tmp_path: Path) -> None:
    cache_dir = tmp_path / "cache"

    def _http_get(*_args: object, **_kwargs: object) -> _ResponseIter:
        return _ResponseIter([b"abc", b"def"])

    path = ensure_germaparl_tarball(
        cache_dir,
        repo="PolMine/GermaParlTEI",
        ref="main",
        force_refresh=True,
        http_get=_http_get,
    )

    assert path.read_bytes() == b"abcdef"


def test_ensure_germaparl_tarball_ignores_empty_chunks(tmp_path: Path) -> None:
    cache_dir = tmp_path / "cache"

    def _http_get(*_args: object, **_kwargs: object) -> _ResponseIter:
        return _ResponseIter([b"", b"abc", b""])

    path = ensure_germaparl_tarball(
        cache_dir,
        repo="PolMine/GermaParlTEI",
        ref="main",
        force_refresh=True,
        http_get=_http_get,
    )

    assert path.read_bytes() == b"abc"


def test_ensure_germaparl_tarball_downloads_via_content_fallback(
    tmp_path: Path,
) -> None:
    cache_dir = tmp_path / "cache"

    def _http_get(*_args: object, **_kwargs: object) -> _ResponseContent:
        return _ResponseContent(b"xyz")

    path = ensure_germaparl_tarball(
        cache_dir,
        repo="PolMine/GermaParlTEI",
        ref="main",
        force_refresh=True,
        http_get=_http_get,
    )

    assert path.read_bytes() == b"xyz"


def test_ensure_germaparl_tarball_works_without_raise_for_status(
    tmp_path: Path,
) -> None:
    cache_dir = tmp_path / "cache"

    def _http_get(*_args: object, **_kwargs: object) -> _ResponseContentNoRaise:
        return _ResponseContentNoRaise(b"ok")

    path = ensure_germaparl_tarball(
        cache_dir,
        repo="PolMine/GermaParlTEI",
        ref="main",
        force_refresh=True,
        http_get=_http_get,
    )

    assert path.read_bytes() == b"ok"


def test_ensure_germaparl_tarball_rejects_non_iterable_iter_content(
    tmp_path: Path,
) -> None:
    cache_dir = tmp_path / "cache"

    def _http_get(*_args: object, **_kwargs: object) -> _ResponseBadIter:
        return _ResponseBadIter()

    with pytest.raises(DataError, match="iter_content"):
        ensure_germaparl_tarball(
            cache_dir,
            repo="PolMine/GermaParlTEI",
            ref="main",
            force_refresh=True,
            http_get=_http_get,
        )


def test_ensure_germaparl_tarball_rejects_non_bytes_content(tmp_path: Path) -> None:
    cache_dir = tmp_path / "cache"

    def _http_get(*_args: object, **_kwargs: object) -> _ResponseNoBytes:
        return _ResponseNoBytes()

    with pytest.raises(DataError, match="bytes content"):
        ensure_germaparl_tarball(
            cache_dir,
            repo="PolMine/GermaParlTEI",
            ref="main",
            force_refresh=True,
            http_get=_http_get,
        )


def test_ensure_germaparl_tarball_wraps_request_failures(tmp_path: Path) -> None:
    cache_dir = tmp_path / "cache"

    def _http_get(*_args: object, **_kwargs: object) -> object:
        raise requests.exceptions.Timeout("timeout")

    with pytest.raises(DataError, match="Failed to download GermaParlTEI tarball"):
        ensure_germaparl_tarball(
            cache_dir,
            repo="PolMine/GermaParlTEI",
            ref="main",
            force_refresh=True,
            http_get=_http_get,
        )


def test_serialize_germaparl_tei_to_text_is_deterministic_and_respects_max_files(
    tmp_path: Path,
) -> None:
    tarball = _tarball_with_xml(
        {
            "01/BT_01_002.xml": _tei_xml("two"),
            "01/BT_01_001.xml": _tei_xml("one"),
        }
    )
    tar_path = tmp_path / "germaparl.tar.gz"
    tar_path.write_bytes(tarball)
    out_path = tmp_path / "input.txt"

    processed = serialize_germaparl_tei_to_text(
        tar_path,
        out_path,
        include_stage=True,
        include_speaker_attrs=True,
        max_files=1,
    )

    text = out_path.read_text(encoding="utf-8")
    assert processed == 1
    assert '<DOC id="BT_01_001">' in text
    assert "<P>one</P>" in text
    assert "<STAGE" in text
    assert 'who="Alice"' in text


def test_serialize_germaparl_tei_to_text_toggles_stage_and_speaker_attrs(
    tmp_path: Path,
) -> None:
    tarball = _tarball_with_xml({"01/BT_01_001.xml": _tei_xml("hello")})
    tar_path = tmp_path / "germaparl.tar.gz"
    tar_path.write_bytes(tarball)
    out_path = tmp_path / "input.txt"

    serialize_germaparl_tei_to_text(
        tar_path,
        out_path,
        include_stage=False,
        include_speaker_attrs=False,
    )

    text = out_path.read_text(encoding="utf-8")
    assert "<STAGE" not in text
    assert "<SP>" in text
    assert "who=" not in text


def test_serialize_germaparl_tei_to_text_fails_on_malformed_xml(tmp_path: Path) -> None:
    tarball = _tarball_with_xml({"01/BT_01_001.xml": "<TEI><broken></TEI>"})
    tar_path = tmp_path / "germaparl.tar.gz"
    tar_path.write_bytes(tarball)

    with pytest.raises(DataError, match="Malformed TEI XML"):
        serialize_germaparl_tei_to_text(
            tar_path,
            tmp_path / "input.txt",
            include_stage=True,
            include_speaker_attrs=True,
        )


def test_serialize_germaparl_tei_to_text_supports_namespaced_tags(
    tmp_path: Path,
) -> None:
    xml = """<?xml version="1.0" encoding="UTF-8"?>
<TEI xmlns="http://www.tei-c.org/ns/1.0">
  <text><body>
    <div><sp><speaker>A</speaker><p>B</p></sp></div>
  </body></text>
</TEI>
"""
    tarball = _tarball_with_xml({"01/BT_01_001.xml": xml})
    tar_path = tmp_path / "germaparl.tar.gz"
    tar_path.write_bytes(tarball)
    out_path = tmp_path / "input.txt"

    processed = serialize_germaparl_tei_to_text(
        tar_path,
        out_path,
        include_stage=True,
        include_speaker_attrs=True,
    )

    text = out_path.read_text(encoding="utf-8")
    assert processed == 1
    assert "<SPEAKER>A</SPEAKER>" in text
    assert "<P>B</P>" in text


def test_serialize_germaparl_tei_to_text_skips_non_sp_and_empty_text_children(
    tmp_path: Path,
) -> None:
    xml = """<?xml version="1.0" encoding="UTF-8"?>
<TEI>
  <text><body>
    <div>
      <note>ignore me</note>
      <sp><speaker> </speaker><p>content</p></sp>
    </div>
  </body></text>
</TEI>
"""
    tarball = _tarball_with_xml({"01/BT_01_001.xml": xml})
    tar_path = tmp_path / "germaparl.tar.gz"
    tar_path.write_bytes(tarball)
    out_path = tmp_path / "input.txt"

    serialize_germaparl_tei_to_text(
        tar_path,
        out_path,
        include_stage=True,
        include_speaker_attrs=True,
    )
    text = out_path.read_text(encoding="utf-8")
    assert "ignore me" not in text
    assert "<P>content</P>" in text
