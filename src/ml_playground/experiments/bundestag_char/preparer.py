from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path
import pickle
from typing import Iterable, Mapping, cast

import numpy as np

from ml_playground.framework.configuration.models import PreparerConfig
from ml_playground.framework.data_pipeline.transforms.io import (
    coerce_seed_policy,
    diff_file_states,
    seed_text_file_with_policy,
    snapshot_file_states,
)
from ml_playground.framework.core.error_handling import DataError
from ml_playground.framework.experiment_registry.protocol import (
    Preparer as _PreparerProto,
    PrepareReport,
)
from ml_playground.framework.core.error_handling import validate_file_exists
from ml_playground.experiments.bundestag_char.germaparl_tei import (
    ensure_germaparl_tarball,
    serialize_germaparl_tei_to_text,
)

_CHUNK_CHARS = 1 << 20


class BundestagCharPreparer(_PreparerProto):
    def __init__(self, base_dir: Path | None = None) -> None:
        self._base_dir = base_dir

    def prepare(self, cfg: PreparerConfig) -> PrepareReport:  # type: ignore[override]
        extras = cast(Mapping[str, object], getattr(cfg, "extras", {}) or {})
        base_dir_override = extras.get("dataset_dir_override")
        if isinstance(base_dir_override, (str, Path)):
            exp_dir = Path(base_dir_override)
        elif self._base_dir is not None:
            exp_dir = self._base_dir
        else:
            exp_dir = Path(__file__).resolve().parent

        ds_dir = exp_dir / "datasets"
        ds_dir.mkdir(parents=True, exist_ok=True)
        outputs = [ds_dir / "train.bin", ds_dir / "val.bin", ds_dir / "meta.pkl"]

        if _artifacts_look_valid(outputs):
            msgs = (
                f"[bundestag_char] dataset already prepared at {ds_dir}; skipping.",
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

        pre = snapshot_file_states(outputs)

        input_file_path = ds_dir / "input.txt"
        bundled = Path(__file__).parent / "input.txt"
        candidates = [
            Path("/datasets/Bundestag.csv"),
            ds_dir / "input.txt",
            exp_dir / "input.txt",
            exp_dir / "page1.txt",
            bundled,
        ]
        dataset_source = _coerce_dataset_source(extras.get("dataset_source"))
        source_metadata: dict[str, object]

        if dataset_source == "seed":
            _seed_from_local(input_file_path, candidates, extras)
            source_metadata = {"source_kind": "seed"}
        elif dataset_source == "germaparl_tei":
            processed = _prepare_germaparl_input(input_file_path, exp_dir, extras)
            source_metadata = {
                "source_kind": "germaparl_tei",
                "source_repo": str(
                    extras.get("germaparl_repo") or "PolMine/GermaParlTEI"
                ),
                "source_ref": str(extras.get("germaparl_ref") or "main"),
                "source_files_processed": processed,
            }
        else:
            if _any_candidates_exist(input_file_path, candidates):
                _seed_from_local(input_file_path, candidates, extras)
                source_metadata = {"source_kind": "seed"}
            else:
                processed = _prepare_germaparl_input(input_file_path, exp_dir, extras)
                source_metadata = {
                    "source_kind": "germaparl_tei",
                    "source_repo": str(
                        extras.get("germaparl_repo") or "PolMine/GermaParlTEI"
                    ),
                    "source_ref": str(extras.get("germaparl_ref") or "main"),
                    "source_files_processed": processed,
                }

        validate_file_exists(input_file_path, "Input text file")

        raw_path = (
            Path(cfg.raw_text_path)
            if cfg.raw_text_path is not None and Path(cfg.raw_text_path).exists()
            else input_file_path
        )

        tokenizer_type = cfg.tokenizer_type
        if tokenizer_type != "char":
            raise ValueError(
                "BundestagCharPreparer only supports char tokenizer configured via prepare.tokenizer_type"
            )
        split_ratio = _resolve_split_ratio(extras.get("split"))
        train_tokens, val_tokens, vocab = _stream_encode_char_dataset(
            raw_path=raw_path,
            train_path=ds_dir / "train.bin",
            val_path=ds_dir / "val.bin",
            split_ratio=split_ratio,
        )
        meta = _build_char_metadata(
            vocab=vocab,
            train_tokens=train_tokens,
            val_tokens=val_tokens,
            source_metadata=source_metadata,
        )
        _write_meta_atomic(ds_dir / "meta.pkl", meta)

        created, updated, skipped = diff_file_states(outputs, pre)
        created_paths = [Path(path) for path in created]
        updated_paths = [Path(path) for path in updated]
        skipped_paths = [Path(path) for path in skipped]

        msgs = (
            f"[bundestag_char] prepared dataset at {ds_dir}",
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


def _artifacts_look_valid(outputs: Iterable[Path]) -> bool:
    for path in outputs:
        if not path.exists():
            return False
        if path.stat().st_size == 0:
            return False
    return True


def artifacts_look_valid(outputs: Iterable[Path]) -> bool:
    return _artifacts_look_valid(outputs)


def _coerce_dataset_source(value: object) -> str:
    if value is None:
        return "auto"
    if isinstance(value, str) and value in {"auto", "seed", "germaparl_tei"}:
        return value
    raise DataError(
        f"Unsupported dataset_source: {value!r}",
        reason="dataset_source must be one of 'auto', 'seed', 'germaparl_tei'",
        rationale="Bundestag char preparer requires explicit source strategy",
    )


def _seed_from_local(
    dst: Path, candidates: list[Path], extras: Mapping[str, object]
) -> None:
    seed_policy_input: object | None = extras.get("seed_policy")
    seed_policy = coerce_seed_policy(seed_policy_input)
    seed_text_file_with_policy(dst, candidates, policy=seed_policy)


def _any_candidates_exist(dst: Path, candidates: list[Path]) -> bool:
    if dst.exists():
        return True
    for cand in candidates:
        if cand.exists():
            return True
    return False


def _prepare_germaparl_input(
    dst_input: Path, exp_dir: Path, extras: Mapping[str, object]
) -> int:
    repo = str(extras.get("germaparl_repo") or "PolMine/GermaParlTEI")
    ref = str(extras.get("germaparl_ref") or "main")
    cache_dir_raw = extras.get("germaparl_cache_dir")
    cache_dir = (
        Path(cache_dir_raw)
        if isinstance(cache_dir_raw, (str, Path))
        else exp_dir / "raw" / "germaparl_cache"
    )
    force_refresh = bool(extras.get("germaparl_force_refresh", False))
    include_stage = bool(extras.get("germaparl_include_stage", True))
    include_speaker_attrs = bool(extras.get("germaparl_include_speaker_attrs", True))
    max_files_raw = extras.get("germaparl_max_files")
    max_files = int(max_files_raw) if isinstance(max_files_raw, int) else None
    tarball_obj = extras.get("germaparl_tarball_bytes")
    tarball_bytes = (
        bytes(tarball_obj) if isinstance(tarball_obj, (bytes, bytearray)) else None
    )
    http_get = extras.get("germaparl_http_get")

    tarball_path = ensure_germaparl_tarball(
        cache_dir,
        repo=repo,
        ref=ref,
        force_refresh=force_refresh,
        tarball_bytes=tarball_bytes,
        http_get=http_get if callable(http_get) else None,
    )
    return serialize_germaparl_tei_to_text(
        tarball_path,
        dst_input,
        include_stage=include_stage,
        include_speaker_attrs=include_speaker_attrs,
        max_files=max_files,
    )


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
) -> tuple[int, int, dict[str, int]]:
    vocab_chars: set[str] = set()
    total_chars = 0
    for chunk in _iter_text_chunks(raw_path):
        total_chars += len(chunk)
        vocab_chars.update(chunk)

    vocab = {ch: idx for idx, ch in enumerate(sorted(vocab_chars))}
    split_index = int(total_chars * split_ratio)

    train_path.parent.mkdir(parents=True, exist_ok=True)
    train_tokens = 0
    val_tokens = 0
    seen = 0
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
    return train_tokens, val_tokens, vocab


def _build_char_metadata(
    *,
    vocab: Mapping[str, int],
    train_tokens: int,
    val_tokens: int,
    source_metadata: Mapping[str, object],
) -> dict[str, object]:
    itos = {idx: ch for ch, idx in vocab.items()}
    meta: dict[str, object] = {
        "meta_version": 1,
        "tokenizer_type": "char",
        "tokenizer": "char",
        "vocab_size": len(vocab),
        "train_tokens": train_tokens,
        "val_tokens": val_tokens,
        "stoi": dict(vocab),
        "itos": itos,
    }
    meta.update(source_metadata)
    return meta


def _write_meta_atomic(path: Path, meta: Mapping[str, object]) -> None:
    tmp_path = path.with_name(f".{path.name}.tmp")
    try:
        with tmp_path.open("wb") as handle:
            pickle.dump(dict(meta), handle)
        tmp_path.replace(path)
    finally:
        tmp_path.unlink(missing_ok=True)
