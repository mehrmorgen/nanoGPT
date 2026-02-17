from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping
from pathlib import Path
import pickle
import time
from typing import cast

import numpy as np

from ml_playground.framework.configuration.models import PreparerConfig
from ml_playground.framework.core.error_handling import DataError
from ml_playground.framework.core.tokenizer import CharTokenizer
from ml_playground.framework.data_pipeline.transforms.io import (
    diff_file_states,
    snapshot_file_states,
)
from ml_playground.framework.data_pipeline.transforms.tokenization import (
    prepare_with_tokenizer,
)
from ml_playground.framework.experiment_registry.protocol import (
    PrepareReport,
    Preparer as _PreparerProto,
)

from .germaparl_tei import (
    ensure_germaparl_tarball,
    resolve_remote_head_sha,
    serialize_germaparl_tei_to_text,
)

DEFAULT_REPO = "PolMine/GermaParlTEI"
DEFAULT_REF = "main"
DEFAULT_SPLIT = 0.9
OVERWRITE_CONFIRM_KEY = "overwrite_confirm"


class BundestagCharPreparer(_PreparerProto):
    def __init__(self, base_dir: Path | None = None) -> None:
        self._base_dir = base_dir

    def prepare(self, cfg: PreparerConfig) -> PrepareReport:  # type: ignore[override]
        started = time.monotonic()
        extras = cast(Mapping[str, object], getattr(cfg, "extras", {}) or {})
        exp_dir = _resolve_exp_dir(extras, self._base_dir)
        ds_dir = exp_dir / "datasets"
        ds_dir.mkdir(parents=True, exist_ok=True)

        input_path = ds_dir / "input.txt"
        train_path = ds_dir / "train.bin"
        val_path = ds_dir / "val.bin"
        meta_path = ds_dir / "meta.pkl"
        outputs = [input_path, train_path, val_path, meta_path]
        pre = snapshot_file_states(outputs)

        if cfg.tokenizer_type != "char":
            raise DataError(
                "BundestagCharPreparer only supports tokenizer_type='char'",
                reason=f"Received tokenizer_type={cfg.tokenizer_type!r}",
                rationale="This experiment is intentionally char-only for deterministic behavior",
            )

        repo = _coerce_str(extras.get("germaparl_repo"), DEFAULT_REPO)
        ref = _coerce_str(extras.get("germaparl_ref"), DEFAULT_REF)
        split = _coerce_split(extras.get("split"))
        include_stage = _coerce_bool(
            extras.get("germaparl_include_stage"), default=True
        )
        include_speaker = _coerce_bool(
            extras.get("germaparl_include_speaker_attrs"), default=True
        )
        cache_dir = _resolve_cache_dir(exp_dir, extras.get("germaparl_cache_dir"))
        remote_head = resolve_remote_head_sha(repo, ref)

        existing_meta = _load_existing_meta(meta_path)
        artifacts_valid = _artifacts_look_valid(outputs)
        if (
            artifacts_valid
            and existing_meta.get("source_repo") == repo
            and existing_meta.get("source_ref") == ref
            and existing_meta.get("source_head_sha") == remote_head
        ):
            skip_messages = (
                f"[bundestag_char] dataset already prepared at {ds_dir}; remote head unchanged ({remote_head}).",
                "[bundestag_char.outputs.created] []",
                "[bundestag_char.outputs.updated] []",
                f"[bundestag_char.outputs.skipped] {[str(p) for p in outputs]}",
            )
            return PrepareReport(
                created_files=tuple(),
                updated_files=tuple(),
                skipped_files=tuple(outputs),
                messages=skip_messages,
            )

        if any(path.exists() for path in outputs):
            _require_overwrite_confirmation(
                extras=extras,
                repo=repo,
                ref=ref,
                existing_sha=cast(str | None, existing_meta.get("source_head_sha")),
                remote_sha=remote_head,
                outputs=outputs,
            )

        cfg.logger.info(
            "[bundestag_char] preparing dataset from %s@%s (remote head %s)",
            repo,
            ref,
            remote_head,
        )
        tarball_path = ensure_germaparl_tarball(cache_dir, repo=repo, ref=ref)
        cfg.logger.info("[bundestag_char] using tarball: %s", tarball_path)

        next_progress = {"at": 100}

        def _progress_cb(count: int, _doc_id: str) -> None:
            if count >= next_progress["at"]:
                cfg.logger.info("[bundestag_char] serialized %s TEI documents", count)
                next_progress["at"] += 100

        source_files = serialize_germaparl_tei_to_text(
            tarball_path,
            input_path,
            include_stage=include_stage,
            include_speaker_attrs=include_speaker,
            progress_cb=_progress_cb,
        )
        input_text = input_path.read_text(encoding="utf-8")
        if not input_text:
            raise DataError(
                "GermaParlTEI serialization produced empty input text",
                reason="input.txt was created but contained no characters",
                rationale="Character-level tokenization requires non-empty source text",
            )

        tokenizer = CharTokenizer()
        train_arr, val_arr, token_meta, tok = prepare_with_tokenizer(
            input_text, tokenizer, split=split
        )
        if not isinstance(tok, CharTokenizer):
            raise DataError(
                "Unexpected tokenizer instance returned by tokenization pipeline",
                reason=f"Expected CharTokenizer, got {type(tok).__name__}",
                rationale="This experiment is constrained to char tokenization",
            )

        stoi = _extract_stoi(token_meta, tok)
        itos = {index: token for token, index in stoi.items()}
        minimal_meta: dict[str, object] = {
            "meta_version": 1,
            "tokenizer_type": "char",
            "tokenizer": "char",
            "vocab_size": len(stoi),
            "stoi": stoi,
            "itos": itos,
            "train_tokens": int(train_arr.size),
            "val_tokens": int(val_arr.size),
            "source_head_sha": remote_head,
            "source_repo": repo,
            "source_ref": ref,
        }

        ds_dir.mkdir(parents=True, exist_ok=True)
        train_arr.astype(np.uint16, copy=False).tofile(train_path)
        val_arr.astype(np.uint16, copy=False).tofile(val_path)
        with meta_path.open("wb") as handle:
            pickle.dump(minimal_meta, handle)

        created, updated, skipped = diff_file_states(outputs, pre)
        created_paths = tuple(Path(path) for path in created)
        updated_paths = tuple(Path(path) for path in updated)
        skipped_paths = tuple(Path(path) for path in skipped)
        elapsed = time.monotonic() - started

        build_messages: list[str] = [
            f"[bundestag_char] prepared dataset at {ds_dir}",
            f"[bundestag_char] source_files_processed={source_files}, input_chars={len(input_text)}, train_tokens={train_arr.size}, val_tokens={val_arr.size}, elapsed_s={elapsed:.2f}",
            f"[bundestag_char.outputs.created] {[str(p) for p in created_paths]}",
            f"[bundestag_char.outputs.updated] {[str(p) for p in updated_paths]}",
            f"[bundestag_char.outputs.skipped] {[str(p) for p in skipped_paths]}",
        ]
        return PrepareReport(
            created_files=created_paths,
            updated_files=updated_paths,
            skipped_files=skipped_paths,
            messages=tuple(build_messages),
        )


def _resolve_exp_dir(extras: Mapping[str, object], base_dir: Path | None) -> Path:
    value = extras.get("dataset_dir_override")
    if isinstance(value, (str, Path)):
        return Path(value)
    if base_dir is not None:
        return base_dir
    return Path(__file__).resolve().parent


def _resolve_cache_dir(exp_dir: Path, cache_override: object) -> Path:
    if isinstance(cache_override, (str, Path)):
        return Path(cache_override)
    return exp_dir / "raw" / "germaparl_cache"


def _coerce_bool(value: object, *, default: bool) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    raise DataError(
        "Invalid boolean prepare extra",
        reason=f"Expected bool value but received {type(value).__name__}",
        rationale="GermaParl toggles require explicit boolean values",
    )


def _coerce_str(value: object, default: str) -> str:
    if value is None:
        return default
    if isinstance(value, str) and value.strip():
        return value
    raise DataError(
        "Invalid string prepare extra",
        reason=f"Expected non-empty string but received {value!r}",
        rationale="Source repository and ref must be concrete strings",
    )


def _coerce_split(value: object) -> float:
    if value is None:
        return DEFAULT_SPLIT
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        split = float(value)
    else:
        raise DataError(
            "Invalid split ratio",
            reason=f"Expected numeric split ratio but received {type(value).__name__}",
            rationale="Train/validation split requires a numeric fraction in (0, 1)",
        )
    if split <= 0.0 or split >= 1.0:
        raise DataError(
            "Invalid split ratio",
            reason=f"Split ratio {split} is outside the open interval (0, 1)",
            rationale="Split must leave at least one token for each dataset partition",
        )
    return split


def _load_existing_meta(meta_path: Path) -> dict[str, object]:
    if not meta_path.exists():
        return {}
    try:
        with meta_path.open("rb") as handle:
            payload = pickle.load(handle)
    except Exception:
        return {}
    if isinstance(payload, dict):
        return cast(dict[str, object], payload)
    return {}


def _extract_stoi(
    meta: Mapping[str, object], tokenizer: CharTokenizer
) -> dict[str, int]:
    stoi_raw = meta.get("stoi")
    if isinstance(stoi_raw, Mapping):
        normalized: dict[str, int] = {}
        for token, index in stoi_raw.items():
            if isinstance(index, int) and not isinstance(index, bool):
                normalized[str(token)] = index
            else:
                raise DataError(
                    "Tokenizer metadata contains invalid stoi mapping",
                    reason=f"Token index for {token!r} is not an integer",
                    rationale="Metadata must provide deterministic integer token ids",
                )
        return dict(sorted(normalized.items(), key=lambda item: item[0]))
    return dict(sorted(tokenizer.stoi.items(), key=lambda item: item[0]))


def _require_overwrite_confirmation(
    *,
    extras: Mapping[str, object],
    repo: str,
    ref: str,
    existing_sha: str | None,
    remote_sha: str,
    outputs: Iterable[Path],
) -> None:
    confirm_raw = extras.get(OVERWRITE_CONFIRM_KEY)
    if confirm_raw is None:
        raise DataError(
            "bundestag_char prepared artifacts already exist and require overwrite permission",
            reason="No overwrite confirmation callback was injected",
            rationale="Non-interactive callers must provide explicit overwrite policy",
        )
    if not callable(confirm_raw):
        raise DataError(
            "Invalid overwrite confirmation callback",
            reason=f"Expected callable, got {type(confirm_raw).__name__}",
            rationale="Overwrite confirmation must be injected as a callable",
        )
    existing_display = existing_sha if isinstance(existing_sha, str) else "<missing>"
    message = "\n".join(
        (
            "bundestag_char prepared artifacts already exist and require overwrite.",
            f"Source repo/ref: {repo}@{ref}",
            f"Existing source_head_sha: {existing_display}",
            f"Remote source_head_sha: {remote_sha}",
            f"Impacted files: {', '.join(str(path) for path in outputs)}",
            "Overwrite existing prepared artifacts?",
        )
    )
    confirmed = cast(Callable[[str], bool], confirm_raw)(message)
    if not confirmed:
        raise DataError(
            "Overwrite cancelled by user",
            reason="Confirmation callback returned False",
            rationale="Existing prepared artifacts must remain unchanged without explicit approval",
        )


def _artifacts_look_valid(outputs: Iterable[Path]) -> bool:
    for path in outputs:
        if not path.exists():
            return False
        if path.stat().st_size == 0:
            return False
    return True


def artifacts_look_valid(outputs: Iterable[Path]) -> bool:
    return _artifacts_look_valid(outputs)
