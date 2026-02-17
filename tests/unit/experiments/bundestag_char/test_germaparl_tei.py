from __future__ import annotations

import io
from pathlib import Path
import tarfile

import pytest
import requests.exceptions

from ml_playground.framework.core.error_handling import DataError
from ml_playground.experiments.bundestag_char.germaparl_tei import (
    build_codeload_url,
    build_github_commit_api_url,
    ensure_germaparl_tarball,
    resolve_remote_head_sha,
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


class _ResponseJSON:
    def __init__(self, payload: object) -> None:
        self._payload = payload

    def raise_for_status(self) -> None:
        return None

    def json(self) -> object:
        return self._payload


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


class _ResponseNoJSON:
    def __init__(self, text: str) -> None:
        self.text = text

    def raise_for_status(self) -> None:
        return None


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


def test_build_github_commit_api_url() -> None:
    assert (
        build_github_commit_api_url("PolMine/GermaParlTEI", "main")
        == "https://api.github.com/repos/PolMine/GermaParlTEI/commits/main"
    )


def test_resolve_remote_head_sha_success() -> None:
    def _http_get(*_args: object, **_kwargs: object) -> _ResponseJSON:
        return _ResponseJSON({"sha": "abc123"})

    assert (
        resolve_remote_head_sha("PolMine/GermaParlTEI", "main", http_get=_http_get)
        == "abc123"
    )


def test_resolve_remote_head_sha_supports_text_json() -> None:
    def _http_get(*_args: object, **_kwargs: object) -> _ResponseNoJSON:
        return _ResponseNoJSON('{"sha": "deadbeef"}')

    assert (
        resolve_remote_head_sha("PolMine/GermaParlTEI", "main", http_get=_http_get)
        == "deadbeef"
    )


def test_resolve_remote_head_sha_rejects_missing_sha() -> None:
    def _http_get(*_args: object, **_kwargs: object) -> _ResponseJSON:
        return _ResponseJSON({"not_sha": "x"})

    with pytest.raises(DataError, match="sha"):
        resolve_remote_head_sha("PolMine/GermaParlTEI", "main", http_get=_http_get)


def test_resolve_remote_head_sha_wraps_request_failures() -> None:
    def _http_get(*_args: object, **_kwargs: object) -> object:
        raise requests.exceptions.Timeout("timeout")

    with pytest.raises(DataError, match="Failed to resolve remote head"):
        resolve_remote_head_sha("PolMine/GermaParlTEI", "main", http_get=_http_get)


def test_ensure_germaparl_tarball_uses_existing_cache(tmp_path: Path) -> None:
    cache_dir = tmp_path / "cache"
    cache_dir.mkdir(parents=True)
    existing = cache_dir / "PolMine_GermaParlTEI-main-sha1.tar.gz"
    existing.write_bytes(b"cached")

    def _boom(*_args: object, **_kwargs: object) -> object:
        raise AssertionError("http_get must not be called when cache exists")

    path = ensure_germaparl_tarball(
        cache_dir,
        repo="PolMine/GermaParlTEI",
        ref="main",
        head_sha="sha1",
        http_get=_boom,
    )

    assert path == existing


def test_ensure_germaparl_tarball_downloads_via_iter_content(tmp_path: Path) -> None:
    cache_dir = tmp_path / "cache"
    progress: list[int] = []

    def _http_get(*_args: object, **_kwargs: object) -> _ResponseIter:
        return _ResponseIter([b"abc", b"def"])

    path = ensure_germaparl_tarball(
        cache_dir,
        repo="PolMine/GermaParlTEI",
        ref="main",
        head_sha="sha2",
        http_get=_http_get,
        progress_cb=progress.append,
    )

    assert path.read_bytes() == b"abcdef"
    assert progress


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
        head_sha="sha3",
        http_get=_http_get,
    )

    assert path.read_bytes() == b"xyz"


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
            head_sha="sha4",
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
            head_sha="sha5",
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
            head_sha="sha6",
            http_get=_http_get,
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

    progress: list[tuple[int, int]] = []
    stats = serialize_germaparl_tei_to_text(
        tar_path,
        out_path,
        include_stage=True,
        include_speaker_attrs=True,
        progress_cb=lambda processed, total: progress.append((processed, total)),
    )

    text = out_path.read_text(encoding="utf-8")
    assert stats.files_processed == 2
    assert stats.input_chars == len(text)
    assert '<DOC id="BT_01_001">' in text
    assert "<P>one</P>" in text
    assert "<STAGE" in text
    assert 'who="Alice"' in text
    assert progress and progress[-1] == (2, 2)


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

    stats = serialize_germaparl_tei_to_text(
        tar_path,
        out_path,
        include_stage=True,
        include_speaker_attrs=True,
    )

    text = out_path.read_text(encoding="utf-8")
    assert stats.files_processed == 1
    assert "<SPEAKER>A</SPEAKER>" in text
    assert "<P>B</P>" in text
