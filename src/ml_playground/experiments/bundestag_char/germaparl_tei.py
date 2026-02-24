from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping
from html import escape
from pathlib import Path
import io
import tarfile
from typing import cast
import xml.etree.ElementTree as ET

import requests
import requests.exceptions

from ml_playground.framework.core.error_handling import DataError

CHUNK_SIZE = 1 << 20
_DOWNLOAD_PROGRESS_BYTES = 128 << 20


def build_codeload_url(repo: str, ref: str) -> str:
    return f"https://codeload.github.com/{repo}/tar.gz/refs/heads/{ref}"


def ensure_germaparl_tarball(
    cache_dir: Path,
    *,
    repo: str,
    ref: str,
    force_refresh: bool = False,
    tarball_bytes: bytes | None = None,
    http_get: Callable[..., object] | None = None,
    progress: Callable[[str], None] | None = None,
) -> Path:
    cache_dir.mkdir(parents=True, exist_ok=True)
    safe_repo = repo.replace("/", "_")
    safe_ref = ref.replace("/", "_")
    tarball_path = cache_dir / f"{safe_repo}-{safe_ref}.tar.gz"

    if tarball_bytes is not None:
        tarball_path.write_bytes(tarball_bytes)
        if progress is not None:
            progress(
                f"[bundestag_char] wrote injected GermaParl tarball to {tarball_path}"
            )
        return tarball_path

    if tarball_path.exists() and not force_refresh:
        if progress is not None:
            progress(f"[bundestag_char] using cached GermaParl tarball: {tarball_path}")
        return tarball_path

    get_fn = http_get if callable(http_get) else requests.get
    url = build_codeload_url(repo, ref)
    tmp_path = tarball_path.with_name(f".{tarball_path.name}.tmp")
    try:
        if progress is not None:
            progress(f"[bundestag_char] downloading GermaParl tarball from {url}")
        response = get_fn(url, stream=True, timeout=120)
        raise_for_status = getattr(response, "raise_for_status", None)
        if callable(raise_for_status):
            raise_for_status()
        expected_bytes: int | None = None
        headers = getattr(response, "headers", None)
        if isinstance(headers, Mapping):
            content_length = headers.get("Content-Length")
            if isinstance(content_length, str):
                try:
                    parsed_length = int(content_length)
                    if parsed_length > 0:
                        expected_bytes = parsed_length
                except ValueError:
                    expected_bytes = None
        with tmp_path.open("wb") as handle:
            chunks = getattr(response, "iter_content", None)
            if callable(chunks):
                chunk_iter = chunks(chunk_size=CHUNK_SIZE)
                if not isinstance(chunk_iter, Iterable):
                    raise DataError(
                        "HTTP response iter_content did not return an iterable",
                        reason="iter_content return value is not iterable",
                        rationale="Tarball download requires streamable byte chunks",
                    )
                downloaded_bytes = 0
                next_progress = _DOWNLOAD_PROGRESS_BYTES
                for chunk in cast(Iterable[bytes], chunk_iter):
                    if chunk:
                        handle.write(chunk)
                        downloaded_bytes += len(chunk)
                        if progress is not None and downloaded_bytes >= next_progress:
                            if expected_bytes is None:
                                progress(
                                    "[bundestag_char] tarball download progress: "
                                    f"{downloaded_bytes / (1 << 20):.1f} MiB"
                                )
                            else:
                                percent = 100.0 * downloaded_bytes / expected_bytes
                                progress(
                                    "[bundestag_char] tarball download progress: "
                                    f"{percent:.1f}%"
                                )
                            next_progress += _DOWNLOAD_PROGRESS_BYTES
            else:
                content = getattr(response, "content", None)
                if not isinstance(content, (bytes, bytearray)):
                    raise DataError(
                        "HTTP response did not include bytes content for GermaParlTEI tarball",
                        reason="No iter_content and no bytes-like content available",
                        rationale="GermaParlTEI ingestion requires a tar.gz payload",
                    )
                handle.write(bytes(content))
        tmp_path.replace(tarball_path)
        if progress is not None:
            progress(f"[bundestag_char] cached GermaParl tarball at {tarball_path}")
    except requests.exceptions.RequestException as exc:
        raise DataError(
            f"Failed to download GermaParlTEI tarball: {exc}",
            reason=f"HTTP request raised {exc.__class__.__name__}",
            rationale="GermaParlTEI preparation requires downloadable source data when no cache exists",
        ) from exc
    finally:
        tmp_path.unlink(missing_ok=True)
    return tarball_path


def serialize_germaparl_tei_to_text(
    tarball_path: Path,
    dst_text_path: Path,
    *,
    include_stage: bool,
    include_speaker_attrs: bool,
    max_files: int | None = None,
    progress: Callable[[str], None] | None = None,
) -> int:
    file_count = 0
    dst_text_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = dst_text_path.with_name(f".{dst_text_path.name}.tmp")

    with (
        tarfile.open(tarball_path, mode="r:gz") as archive,
        tmp_path.open("w", encoding="utf-8") as out,
    ):
        xml_members = [
            member
            for member in archive.getmembers()
            if member.isfile() and member.name.endswith(".xml")
        ]
        xml_members.sort(key=lambda member: member.name)
        if max_files is not None:
            xml_members = xml_members[:max_files]
        if progress is not None:
            progress(
                "[bundestag_char] serializing "
                f"{len(xml_members)} XML files from {tarball_path.name}"
            )

        total_members = len(xml_members)
        for idx, member in enumerate(xml_members, start=1):
            extracted = archive.extractfile(member)
            if extracted is None:
                continue
            xml_bytes = extracted.read()
            doc_id = Path(member.name).stem
            _write_tei_document_lines(
                xml_bytes,
                out,
                doc_id=doc_id,
                include_stage=include_stage,
                include_speaker_attrs=include_speaker_attrs,
            )
            file_count += 1
            if progress is not None and (
                idx == 1 or idx == total_members or idx % 250 == 0
            ):
                progress(f"[bundestag_char] serialized {idx}/{total_members} XML files")

    tmp_path.replace(dst_text_path)
    return file_count


def _write_tei_document_lines(
    xml_bytes: bytes,
    out: io.TextIOBase,
    *,
    doc_id: str,
    include_stage: bool,
    include_speaker_attrs: bool,
) -> None:
    try:
        root = ET.fromstring(xml_bytes)
    except ET.ParseError as exc:
        raise DataError(
            f"Malformed TEI XML encountered for document {doc_id}: {exc}",
            reason="XML parsing failed",
            rationale="Preparing deterministic text requires syntactically valid TEI source files",
        ) from exc

    out.write(f'<DOC id="{escape(doc_id, quote=True)}">\n')
    for div in root.iter():
        if _local_name(div.tag) != "div":
            continue
        out.write(f"<DIV{_attrs_to_str(div.attrib)}>\n")
        for sp in div:
            if _local_name(sp.tag) != "sp":
                continue
            sp_attrs = sp.attrib if include_speaker_attrs else {}
            out.write(f"<SP{_attrs_to_str(sp_attrs)}>\n")
            for child in sp:
                local = _local_name(child.tag)
                text = _normalize_text("".join(child.itertext()))
                if not text:
                    continue
                if local == "speaker":
                    out.write(f"<SPEAKER>{escape(text)}</SPEAKER>\n")
                elif local == "p":
                    out.write(f"<P>{escape(text)}</P>\n")
                elif local == "stage" and include_stage:
                    out.write(
                        f"<STAGE{_attrs_to_str(child.attrib)}>{escape(text)}</STAGE>\n"
                    )
            out.write("</SP>\n")
        out.write("</DIV>\n")
    out.write("</DOC>\n")


def _local_name(tag: str) -> str:
    if "}" in tag:
        return tag.rsplit("}", maxsplit=1)[-1]
    return tag


def _attrs_to_str(attrs: dict[str, str]) -> str:
    if not attrs:
        return ""
    parts: list[str] = []
    for key in sorted(attrs):
        value = attrs[key]
        parts.append(f' {key}="{escape(value, quote=True)}"')
    return "".join(parts)


def _normalize_text(text: str) -> str:
    return " ".join(text.split())
