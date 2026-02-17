from __future__ import annotations

from collections.abc import Iterator, Mapping
from dataclasses import dataclass
from pathlib import Path
import pickle
import time
from typing import cast

import numpy as np

from ml_playground.experiments.bundestag_char.germaparl_tei import (
    SerializeStats,
    ensure_germaparl_tarball,
    resolve_remote_head_sha,
    serialize_germaparl_tei_to_text,
)
from ml_playground.framework.configuration.models import PreparerConfig
from ml_playground.framework.core.error_handling import DataError, validate_file_exists
from ml_playground.framework.core.logging_protocol import LoggerLike
from ml_playground.framework.data_pipeline.transforms.io import (
    diff_file_states,
    snapshot_file_states,
)
from ml_playground.framework.experiment_registry.protocol import (
    Preparer as _PreparerProto,
    PrepareReport,
)

_CHUNK_CHARS = 1 << 20
_DEFAULT_GERMAPARL_REPO = "PolMine/GermaParlTEI"
_DEFAULT_GERMAPARL_REF = "main"
_OVERWRITE_CONFIRM_EXTRA_KEY = "overwrite_confirm"
_REMOTE_HEAD_RESOLVER_EXTRA_KEY = "__remote_head_resolver"
_GERMAPARL_HTTP_GET_EXTRA_KEY = "__germaparl_http_get"


@dataclass(frozen=True)
class PreparedSourceState:
    source_repo: str | None
    source_ref: str | None
    source_head_sha: str | None


class BundestagCharPreparer(_PreparerProto):
    def __init__(self, base_dir: Path | None = None) -> None:
        self._base_dir = base_dir

    def prepare(self, cfg: PreparerConfig) -> PrepareReport:  # type: ignore[override]
        started_at = time.monotonic()
        extras = cast(Mapping[str, object], getattr(cfg, "extras", {}) or {})

        exp_dir = _resolve_experiment_dir(
            extras=extras,
            base_dir=self._base_dir,
            fallback=Path(__file__).resolve().parent,
        )

        ds_dir = exp_dir / "datasets"
        ds_dir.mkdir(parents=True, exist_ok=True)

        input_path = ds_dir / "input.txt"
        train_path = ds_dir / "train.bin"
        val_path = ds_dir / "val.bin"
        meta_path = ds_dir / "meta.pkl"
        outputs = [input_path, train_path, val_path, meta_path]

        repo = _coerce_str(
            extras.get("germaparl_repo"), default=_DEFAULT_GERMAPARL_REPO
        )
        ref = _coerce_str(extras.get("germaparl_ref"), default=_DEFAULT_GERMAPARL_REF)
        split_ratio = _resolve_split_ratio(extras.get("split"))

        remote_head_sha = _resolve_remote_head(extras=extras, repo=repo, ref=ref)
        existing_state = _load_existing_source_state(meta_path)
        artifacts_valid = _artifacts_look_valid(outputs)

        if _should_skip_prepare(
            artifacts_valid=artifacts_valid,
            existing_state=existing_state,
            repo=repo,
            ref=ref,
            remote_head_sha=remote_head_sha,
        ):
            msgs = (
                f"[bundestag_char] dataset already prepared at {ds_dir}; skipping (head={remote_head_sha}).",
                "[bundestag_char.outputs.created] []",
                "[bundestag_char.outputs.updated] []",
                f"[bundestag_char.outputs.skipped] {[str(p) for p in outputs]}",
            )
            return PrepareReport(
                created_files=tuple(),
                updated_files=tuple(),
                skipped_files=tuple(outputs),
                messages=msgs,
            )

        if _any_paths_exist(outputs):
            _require_overwrite_confirmation(
                extras=extras,
                existing_state=existing_state,
                repo=repo,
                ref=ref,
                remote_head_sha=remote_head_sha,
                impacted_files=outputs,
            )

        pre = snapshot_file_states(outputs)

        if cfg.tokenizer_type != "char":
            raise DataError(
                "BundestagCharPreparer only supports char tokenizer configured via prepare.tokenizer_type",
                reason=f"Unsupported tokenizer_type: {cfg.tokenizer_type}",
                rationale="This experiment writes uint16 character token ids and requires char tokenization",
            )

        serialize_stats = _prepare_germaparl_input(
            dst_input=input_path,
            exp_dir=exp_dir,
            extras=extras,
            repo=repo,
            ref=ref,
            head_sha=remote_head_sha,
            logger=cfg.logger,
        )

        validate_file_exists(input_path, "Input text file")

        raw_path = (
            Path(cfg.raw_text_path)
            if cfg.raw_text_path is not None and Path(cfg.raw_text_path).exists()
            else input_path
        )

        train_tokens, val_tokens, vocab, input_chars = _stream_encode_char_dataset(
            raw_path=raw_path,
            train_path=train_path,
            val_path=val_path,
            split_ratio=split_ratio,
            logger=cfg.logger,
        )

        meta = _build_char_metadata(
            vocab=vocab,
            train_tokens=train_tokens,
            val_tokens=val_tokens,
            source_head_sha=remote_head_sha,
            source_repo=repo,
            source_ref=ref,
        )
        _write_meta_atomic(meta_path, meta)

        created, updated, skipped = diff_file_states(outputs, pre)
        created_paths = [Path(path) for path in created]
        updated_paths = [Path(path) for path in updated]
        skipped_paths = [Path(path) for path in skipped]

        elapsed = time.monotonic() - started_at
        msgs = (
            f"[bundestag_char] prepared dataset at {ds_dir}",
            f"[bundestag_char.source] {repo}@{ref} head={remote_head_sha} files_processed={serialize_stats.files_processed}",
            f"[bundestag_char.tokens] input_chars={input_chars}, train_tokens={train_tokens}, val_tokens={val_tokens}, elapsed_seconds={elapsed:.2f}",
            f"[bundestag_char.outputs.created] {[str(p) for p in created_paths]}",
            f"[bundestag_char.outputs.updated] {[str(p) for p in updated_paths]}",
            f"[bundestag_char.outputs.skipped] {[str(p) for p in skipped_paths]}",
        )

        return PrepareReport(
            created_files=tuple(created_paths),
            updated_files=tuple(updated_paths),
            skipped_files=tuple(skipped_paths),
            messages=msgs,
        )


def _resolve_experiment_dir(
    *,
    extras: Mapping[str, object],
    base_dir: Path | None,
    fallback: Path,
) -> Path:
    base_dir_override = extras.get("dataset_dir_override")
    if isinstance(base_dir_override, (str, Path)):
        return Path(base_dir_override)
    if base_dir is not None:
        return base_dir
    return fallback


def _coerce_str(raw: object | None, *, default: str) -> str:
    if raw is None:
        return default
    if isinstance(raw, str) and raw:
        return raw
    raise DataError(
        f"Invalid preparer extra value: {raw!r}",
        reason="Expected non-empty string value",
        rationale="GermaParl repository and ref values must be valid strings",
    )


def _coerce_bool(raw: object | None, *, default: bool, key: str) -> bool:
    if raw is None:
        return default
    if isinstance(raw, bool):
        return raw
    raise DataError(
        f"Invalid boolean extra for {key}: {raw!r}",
        reason=f"{key} must be a boolean value",
        rationale="Boolean knobs must be explicit true/false to keep data generation deterministic",
    )


def _resolve_remote_head(*, extras: Mapping[str, object], repo: str, ref: str) -> str:
    resolver_obj = extras.get(_REMOTE_HEAD_RESOLVER_EXTRA_KEY)
    if callable(resolver_obj):
        resolved = resolver_obj(repo, ref)
        if isinstance(resolved, str) and resolved:
            return resolved
        raise DataError(
            f"Invalid remote head from injected resolver for {repo}@{ref}: {resolved!r}",
            reason="Injected remote head resolver returned invalid value",
            rationale="Freshness checks require a non-empty commit SHA string",
        )
    return resolve_remote_head_sha(repo, ref)


def _load_existing_source_state(meta_path: Path) -> PreparedSourceState | None:
    if not meta_path.exists() or meta_path.stat().st_size == 0:
        return None

    try:
        with meta_path.open("rb") as handle:
            payload = pickle.load(handle)
    except (OSError, pickle.UnpicklingError):
        return None

    if not isinstance(payload, Mapping):
        return None

    repo_obj = payload.get("source_repo")
    ref_obj = payload.get("source_ref")
    sha_obj = payload.get("source_head_sha")

    repo = repo_obj if isinstance(repo_obj, str) else None
    ref = ref_obj if isinstance(ref_obj, str) else None
    sha = sha_obj if isinstance(sha_obj, str) else None

    return PreparedSourceState(source_repo=repo, source_ref=ref, source_head_sha=sha)


def _should_skip_prepare(
    *,
    artifacts_valid: bool,
    existing_state: PreparedSourceState | None,
    repo: str,
    ref: str,
    remote_head_sha: str,
) -> bool:
    if not artifacts_valid:
        return False
    if existing_state is None:
        return False
    if existing_state.source_repo != repo or existing_state.source_ref != ref:
        return False
    if existing_state.source_head_sha != remote_head_sha:
        return False
    return True


def _any_paths_exist(paths: list[Path]) -> bool:
    return any(path.exists() for path in paths)


def _require_overwrite_confirmation(
    *,
    extras: Mapping[str, object],
    existing_state: PreparedSourceState | None,
    repo: str,
    ref: str,
    remote_head_sha: str,
    impacted_files: list[Path],
) -> None:
    confirm_obj = extras.get(_OVERWRITE_CONFIRM_EXTRA_KEY)
    if not callable(confirm_obj):
        raise DataError(
            "bundestag_char prepared artifacts already exist and require explicit overwrite permission",
            reason="overwrite confirmation callback was not injected",
            rationale="Non-interactive prepare runs must provide an explicit overwrite decision callback",
        )

    existing_repo = existing_state.source_repo if existing_state else None
    existing_ref = existing_state.source_ref if existing_state else None
    existing_sha = existing_state.source_head_sha if existing_state else None

    message = "\n".join(
        (
            "bundestag_char prepared artifacts already exist and require overwrite.",
            f"Source repo/ref: {repo}@{ref}",
            f"Existing source repo/ref: {(existing_repo or '<missing>')}@{(existing_ref or '<missing>')}",
            f"Existing source_head_sha: {existing_sha or '<missing>'}",
            f"Remote source_head_sha: {remote_head_sha}",
            "Impacted files: "
            + ", ".join(str(path.resolve()) for path in impacted_files),
            "Overwrite existing prepared artifacts?",
        )
    )

    try:
        accepted = bool(confirm_obj(message))
    except Exception as exc:  # pragma: no cover - defensive wrapper
        raise DataError(
            "Failed to evaluate overwrite confirmation for bundestag_char prepare",
            reason=f"Confirmation callback raised {exc.__class__.__name__}: {exc}",
            rationale="Overwrite confirmation must deterministically return a boolean decision",
        ) from exc

    if not accepted:
        raise DataError(
            "bundestag_char prepare cancelled by user (overwrite declined)",
            reason="User declined overwrite confirmation",
            rationale="Existing prepared artifacts were preserved as requested",
        )


def _prepare_germaparl_input(
    *,
    dst_input: Path,
    exp_dir: Path,
    extras: Mapping[str, object],
    repo: str,
    ref: str,
    head_sha: str,
    logger: LoggerLike,
) -> SerializeStats:
    cache_dir_raw = extras.get("germaparl_cache_dir")
    cache_dir = (
        Path(cache_dir_raw)
        if isinstance(cache_dir_raw, (str, Path))
        else exp_dir / "raw" / "germaparl_cache"
    )
    include_stage = _coerce_bool(
        extras.get("germaparl_include_stage"),
        default=True,
        key="germaparl_include_stage",
    )
    include_speaker_attrs = _coerce_bool(
        extras.get("germaparl_include_speaker_attrs"),
        default=True,
        key="germaparl_include_speaker_attrs",
    )

    http_get_obj = extras.get(_GERMAPARL_HTTP_GET_EXTRA_KEY)
    http_get = http_get_obj if callable(http_get_obj) else None

    logger.info("[bundestag_char] resolving GermaParl source %s@%s", repo, ref)
    logger.info("[bundestag_char] remote source head: %s", head_sha)

    last_download_log_mib = -1

    def _download_progress(bytes_written: int) -> None:
        nonlocal last_download_log_mib
        mib = int(bytes_written / (1 << 20))
        milestone_mib = (mib // 64) * 64
        if milestone_mib > last_download_log_mib:
            last_download_log_mib = milestone_mib
            logger.info(
                "[bundestag_char] downloading GermaParl archive: %.1f MiB",
                bytes_written / (1 << 20),
            )

    tarball_path = ensure_germaparl_tarball(
        cache_dir,
        repo=repo,
        ref=ref,
        head_sha=head_sha,
        http_get=http_get,
        progress_cb=_download_progress,
    )

    logger.info("[bundestag_char] using GermaParl archive at %s", tarball_path)

    def _serialize_progress(processed: int, total: int) -> None:
        logger.info(
            "[bundestag_char] serialized TEI files: %d/%d",
            processed,
            total,
        )

    stats = serialize_germaparl_tei_to_text(
        tarball_path,
        dst_input,
        include_stage=include_stage,
        include_speaker_attrs=include_speaker_attrs,
        progress_cb=_serialize_progress,
    )

    logger.info(
        "[bundestag_char] wrote input corpus to %s (files_processed=%d, input_chars=%d)",
        dst_input,
        stats.files_processed,
        stats.input_chars,
    )

    return stats


def _artifacts_look_valid(outputs: list[Path]) -> bool:
    for path in outputs:
        if not path.exists():
            return False
        if path.stat().st_size == 0:
            return False
    return True


def artifacts_look_valid(outputs: list[Path]) -> bool:
    return _artifacts_look_valid(outputs)


def _resolve_split_ratio(raw_value: object) -> float:
    if raw_value is None:
        return 0.9
    if not isinstance(raw_value, (int, float, str)):
        raise DataError(
            f"Invalid split ratio in extras: {raw_value!r}",
            reason="Split ratio must be numeric or string convertible to float",
            rationale="Training/validation split must be numeric to derive dataset boundaries",
        )
    try:
        ratio = float(raw_value)
    except (TypeError, ValueError) as exc:
        raise DataError(
            f"Invalid split ratio in extras: {raw_value!r}",
            reason=f"Unable to coerce provided split to float: {exc}",
            rationale="Split ratio must be numeric to determine split boundary",
        ) from exc
    if ratio < 0.0 or ratio > 1.0:
        raise DataError(
            f"split ratio must be within [0.0, 1.0]; received {ratio}",
            reason="Split ratio outside inclusive [0.0, 1.0] range",
            rationale="Dataset preparation assumes ratios describe a valid probability interval",
        )
    return ratio


def _iter_text_chunks(path: Path, chunk_chars: int = _CHUNK_CHARS) -> Iterator[str]:
    with path.open("r", encoding="utf-8") as handle:
        while True:
            chunk = handle.read(chunk_chars)
            if not chunk:
                break
            yield chunk


def _stream_encode_char_dataset(
    *,
    raw_path: Path,
    train_path: Path,
    val_path: Path,
    split_ratio: float,
    logger: LoggerLike,
) -> tuple[int, int, dict[str, int], int]:
    logger.info("[bundestag_char] scanning corpus to build character vocabulary")

    vocab_chars: set[str] = set()
    total_chars = 0
    chunks_scanned = 0
    for chunk in _iter_text_chunks(raw_path):
        total_chars += len(chunk)
        vocab_chars.update(chunk)
        chunks_scanned += 1
        if chunks_scanned % 64 == 0:
            logger.info(
                "[bundestag_char] vocabulary scan progress: chars=%d", total_chars
            )

    if total_chars == 0:
        raise DataError(
            f"Input corpus is empty at {raw_path}",
            reason="Input text contains zero characters",
            rationale="Character-level preparation requires at least one tokenizable character",
        )

    if len(vocab_chars) > np.iinfo(np.uint16).max:
        raise DataError(
            "Character vocabulary exceeds uint16 capacity",
            reason=f"vocab_size={len(vocab_chars)} exceeds {np.iinfo(np.uint16).max}",
            rationale="train.bin/val.bin are encoded as uint16 token ids",
        )

    vocab = {ch: idx for idx, ch in enumerate(sorted(vocab_chars))}
    split_index = int(total_chars * split_ratio)

    logger.info(
        "[bundestag_char] encoding corpus (chars=%d, split_index=%d, vocab_size=%d)",
        total_chars,
        split_index,
        len(vocab),
    )

    train_path.parent.mkdir(parents=True, exist_ok=True)
    train_tokens = 0
    val_tokens = 0
    seen = 0
    chunks_encoded = 0

    with train_path.open("wb") as train_out, val_path.open("wb") as val_out:
        for chunk in _iter_text_chunks(raw_path):
            encoded = np.fromiter(
                (vocab[ch] for ch in chunk), dtype=np.uint16, count=len(chunk)
            )
            chunk_start = seen
            chunk_end = seen + len(encoded)
            if chunk_end <= split_index:
                train_out.write(encoded.tobytes())
                train_tokens += len(encoded)
            elif chunk_start >= split_index:
                val_out.write(encoded.tobytes())
                val_tokens += len(encoded)
            else:
                train_len = split_index - chunk_start
                if train_len > 0:
                    train_out.write(encoded[:train_len].tobytes())
                    train_tokens += train_len
                val_part = encoded[train_len:]
                if len(val_part) > 0:
                    val_out.write(val_part.tobytes())
                    val_tokens += len(val_part)
            seen = chunk_end
            chunks_encoded += 1
            if chunks_encoded % 64 == 0:
                logger.info(
                    "[bundestag_char] encoding progress: processed_chars=%d train_tokens=%d val_tokens=%d",
                    seen,
                    train_tokens,
                    val_tokens,
                )

    return train_tokens, val_tokens, vocab, total_chars


def _build_char_metadata(
    *,
    vocab: Mapping[str, int],
    train_tokens: int,
    val_tokens: int,
    source_head_sha: str,
    source_repo: str,
    source_ref: str,
) -> dict[str, object]:
    itos = {idx: ch for ch, idx in vocab.items()}
    return {
        "meta_version": 1,
        "tokenizer_type": "char",
        "tokenizer": "char",
        "vocab_size": len(vocab),
        "stoi": dict(vocab),
        "itos": itos,
        "train_tokens": train_tokens,
        "val_tokens": val_tokens,
        "source_head_sha": source_head_sha,
        "source_repo": source_repo,
        "source_ref": source_ref,
    }


def _write_meta_atomic(path: Path, meta: Mapping[str, object]) -> None:
    tmp_path = path.with_name(f".{path.name}.tmp")
    try:
        with tmp_path.open("wb") as handle:
            pickle.dump(dict(meta), handle)
        tmp_path.replace(path)
    finally:
        tmp_path.unlink(missing_ok=True)
