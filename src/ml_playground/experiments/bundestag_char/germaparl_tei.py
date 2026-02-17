from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass
from html import escape
import io
import json
from pathlib import Path
import tarfile
from typing import cast
import xml.etree.ElementTree as ET

import requests
import requests.exceptions

from ml_playground.framework.core.error_handling import DataError

CHUNK_SIZE = 1 << 20
_PROGRESS_EVERY_N_DOCS = 100


@dataclass(frozen=True)
class SerializeStats:
    files_processed: int
    input_chars: int


def build_codeload_url(repo: str, ref: str) -> str:
    return f"https://codeload.github.com/{repo}/tar.gz/refs/heads/{ref}"


def build_github_commit_api_url(repo: str, ref: str) -> str:
    return f"https://api.github.com/repos/{repo}/commits/{ref}"


def resolve_remote_head_sha(
    repo: str,
    ref: str,
    *,
    http_get: Callable[..., object] | None = None,
) -> str:
    get_fn = http_get if callable(http_get) else requests.get
    url = build_github_commit_api_url(repo, ref)
    headers = {
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }

    try:
        response = get_fn(url, timeout=30, headers=headers)
        raise_for_status = getattr(response, "raise_for_status", None)
        if callable(raise_for_status):
            raise_for_status()
    except requests.exceptions.RequestException as exc:
        raise DataError(
            f"Failed to resolve remote head for {repo}@{ref}: {exc}",
            reason=f"GitHub API request raised {exc.__class__.__name__}",
            rationale="Freshness checks require querying the current remote commit SHA",
        ) from exc

    payload: Mapping[str, object]
    json_reader = getattr(response, "json", None)
    if callable(json_reader):
        payload_obj = json_reader()
        if not isinstance(payload_obj, Mapping):
            raise DataError(
                f"Invalid GitHub API response for {repo}@{ref}",
                reason="Response JSON is not an object",
                rationale="Remote head resolution expects a JSON object containing a sha field",
            )
        payload = cast(Mapping[str, object], payload_obj)
    else:
        text_obj = getattr(response, "text", None)
        if not isinstance(text_obj, str):
            raise DataError(
                f"Invalid GitHub API response for {repo}@{ref}",
                reason="Response does not provide .json() or text payload",
                rationale="Remote head resolution needs parseable response data",
            )
        try:
            payload_obj = json.loads(text_obj)
        except json.JSONDecodeError as exc:
            raise DataError(
                f"Invalid GitHub API response for {repo}@{ref}",
                reason=f"Failed to decode response JSON: {exc}",
                rationale="Remote head resolution requires valid JSON response data",
            ) from exc
        if not isinstance(payload_obj, Mapping):
            raise DataError(
                f"Invalid GitHub API response for {repo}@{ref}",
                reason="Decoded JSON is not an object",
                rationale="Remote head resolution expects a JSON object containing a sha field",
            )
        payload = cast(Mapping[str, object], payload_obj)

    sha_obj = payload.get("sha")
    if not isinstance(sha_obj, str) or not sha_obj:
        raise DataError(
            f"Invalid GitHub API response for {repo}@{ref}",
            reason="Missing or non-string sha field in response",
            rationale="Freshness checks compare stored source_head_sha with the remote head SHA",
        )

    return sha_obj


def ensure_germaparl_tarball(
    cache_dir: Path,
    *,
    repo: str,
    ref: str,
    head_sha: str,
    http_get: Callable[..., object] | None = None,
    progress_cb: Callable[[int], None] | None = None,
) -> Path:
    cache_dir.mkdir(parents=True, exist_ok=True)
    safe_repo = repo.replace("/", "_")
    safe_head = head_sha.replace("/", "_")
    tarball_path = cache_dir / f"{safe_repo}-{ref}-{safe_head}.tar.gz"

    if tarball_path.exists() and tarball_path.stat().st_size > 0:
        return tarball_path

    get_fn = http_get if callable(http_get) else requests.get
    url = build_codeload_url(repo, ref)
    tmp_path = tarball_path.with_name(f".{tarball_path.name}.tmp")

    try:
        response = get_fn(url, stream=True, timeout=180)
        raise_for_status = getattr(response, "raise_for_status", None)
        if callable(raise_for_status):
            raise_for_status()

        bytes_written = 0
        with tmp_path.open("wb") as handle:
            chunks = getattr(response, "iter_content", None)
            if callable(chunks):
                chunk_iter = cast(object, chunks(chunk_size=CHUNK_SIZE))
                if not isinstance(chunk_iter, Iterable):
                    raise DataError(
                        "HTTP response iter_content did not return an iterable",
                        reason="iter_content return value is not iterable",
                        rationale="Tarball download requires streamable byte chunks",
                    )
                for chunk in cast(Iterable[bytes], chunk_iter):
                    if not chunk:
                        continue
                    handle.write(chunk)
                    bytes_written += len(chunk)
                    if progress_cb is not None:
                        progress_cb(bytes_written)
            else:
                content = getattr(response, "content", None)
                if not isinstance(content, (bytes, bytearray)):
                    raise DataError(
                        "HTTP response did not include bytes content for GermaParlTEI tarball",
                        reason="No iter_content and no bytes-like content available",
                        rationale="GermaParlTEI ingestion requires a tar.gz payload",
                    )
                payload = bytes(content)
                handle.write(payload)
                bytes_written = len(payload)
                if progress_cb is not None:
                    progress_cb(bytes_written)

        if bytes_written == 0:
            raise DataError(
                f"Downloaded empty GermaParlTEI tarball for {repo}@{ref}",
                reason="Tarball download produced zero bytes",
                rationale="Preparing the dataset requires non-empty source archive data",
            )

        tmp_path.replace(tarball_path)
    except requests.exceptions.RequestException as exc:
        raise DataError(
            f"Failed to download GermaParlTEI tarball: {exc}",
            reason=f"HTTP request raised {exc.__class__.__name__}",
            rationale="GermaParlTEI preparation requires downloadable source data",
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
    progress_cb: Callable[[int, int], None] | None = None,
) -> SerializeStats:
    dst_text_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = dst_text_path.with_name(f".{dst_text_path.name}.tmp")

    processed = 0
    input_chars = 0

    try:
        with tarfile.open(tarball_path, mode="r:gz") as archive:
            xml_members = [
                member
                for member in archive.getmembers()
                if member.isfile() and member.name.endswith(".xml")
            ]
            xml_members.sort(key=lambda member: member.name)

            if not xml_members:
                raise DataError(
                    f"No XML files found in {tarball_path}",
                    reason="Archive does not contain TEI XML members",
                    rationale="GermaParlTEI preparation requires XML source documents",
                )

            total_members = len(xml_members)
            with tmp_path.open("w", encoding="utf-8") as out:
                for member in xml_members:
                    extracted = archive.extractfile(member)
                    if extracted is None:
                        continue
                    xml_bytes = extracted.read()
                    doc_id = Path(member.name).stem
                    input_chars += _write_tei_document_lines(
                        xml_bytes,
                        out,
                        doc_id=doc_id,
                        include_stage=include_stage,
                        include_speaker_attrs=include_speaker_attrs,
                    )
                    processed += 1
                    if progress_cb is not None and (
                        processed % _PROGRESS_EVERY_N_DOCS == 0
                        or processed == total_members
                    ):
                        progress_cb(processed, total_members)

        tmp_path.replace(dst_text_path)
    except tarfile.TarError as exc:
        raise DataError(
            f"Failed to read GermaParlTEI tarball at {tarball_path}: {exc}",
            reason=f"Tar archive parsing raised {exc.__class__.__name__}",
            rationale="Dataset preparation requires a valid tar.gz archive",
        ) from exc
    finally:
        tmp_path.unlink(missing_ok=True)

    return SerializeStats(files_processed=processed, input_chars=input_chars)


def _write_tei_document_lines(
    xml_bytes: bytes,
    out: io.TextIOBase,
    *,
    doc_id: str,
    include_stage: bool,
    include_speaker_attrs: bool,
) -> int:
    try:
        root = ET.fromstring(xml_bytes)
    except ET.ParseError as exc:
        raise DataError(
            f"Malformed TEI XML encountered for document {doc_id}: {exc}",
            reason="XML parsing failed",
            rationale="Preparing deterministic text requires syntactically valid TEI source files",
        ) from exc

    chars_written = 0
    chars_written += _write_line(out, f'<DOC id="{escape(doc_id, quote=True)}">\n')

    for div in root.iter():
        if _local_name(div.tag) != "div":
            continue

        chars_written += _write_line(out, f"<DIV{_attrs_to_str(div.attrib)}>\n")
        for sp in div.iter():
            if _local_name(sp.tag) != "sp":
                continue

            sp_attrs = sp.attrib if include_speaker_attrs else {}
            chars_written += _write_line(out, f"<SP{_attrs_to_str(sp_attrs)}>\n")

            emitted = False
            for child in sp:
                local = _local_name(child.tag)
                text = _normalize_text("".join(child.itertext()))
                if not text:
                    continue
                if local == "speaker":
                    chars_written += _write_line(
                        out, f"<SPEAKER>{escape(text)}</SPEAKER>\n"
                    )
                    emitted = True
                elif local == "p":
                    chars_written += _write_line(out, f"<P>{escape(text)}</P>\n")
                    emitted = True
                elif local == "stage" and include_stage:
                    chars_written += _write_line(
                        out,
                        f"<STAGE{_attrs_to_str(child.attrib)}>{escape(text)}</STAGE>\n",
                    )
                    emitted = True

            if not emitted:
                fallback = _normalize_text("".join(sp.itertext()))
                if fallback:
                    chars_written += _write_line(
                        out,
                        f"<P>{escape(fallback)}</P>\n",
                    )

            chars_written += _write_line(out, "</SP>\n")
        chars_written += _write_line(out, "</DIV>\n")

    chars_written += _write_line(out, "</DOC>\n")
    return chars_written


def _write_line(out: io.TextIOBase, text: str) -> int:
    out.write(text)
    return len(text)


def _local_name(tag: str) -> str:
    if "}" in tag:
        return tag.rsplit("}", maxsplit=1)[-1]
    return tag


def _attrs_to_str(attrs: Mapping[str, str]) -> str:
    if not attrs:
        return ""
    parts: list[str] = []
    for key in sorted(attrs):
        value = attrs[key]
        parts.append(f' {key}="{escape(value, quote=True)}"')
    return "".join(parts)


def _normalize_text(text: str) -> str:
    return " ".join(text.split())
