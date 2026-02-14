from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Iterable

from ml_playground.framework.configuration.models import PreparerConfig, MetadataConfig
from ml_playground.framework.configuration.models import DataConfig
from ml_playground.framework.data_pipeline.transforms.tokenization import (
    prepare_with_tokenizer,
)
from ml_playground.framework.data_pipeline.transforms.io import (
    diff_file_states,
    snapshot_file_states,
    write_bin_and_meta,
)
from ml_playground.framework.data_pipeline.transforms.tokenization import (
    TokenizerKind,
    coerce_tokenizer_type,
)
from ml_playground.framework.core.error_handling import DataError
from ml_playground.framework.core.tokenizer_protocol import Tokenizer
from ml_playground.framework.core.file_state import FileState


@dataclass(frozen=True)
class PreparationOutcome:
    created_files: tuple[Path, ...]
    updated_files: tuple[Path, ...]
    skipped_files: tuple[Path, ...]
    metadata: dict[str, Any]


TextProvider = Iterable[str] | Callable[[Path], str]


SnapshotProvider = Callable[[Iterable[Path]], dict[str, FileState]]
SnapshotDiffer = Callable[
    [Iterable[Path], dict[str, FileState]],
    tuple[list[str], list[str], list[str]],
]


class _PreparationPipeline:
    def __init__(
        self,
        cfg: PreparerConfig,
        shared: MetadataConfig,
        *,
        text_provider: TextProvider | None = None,
        snapshot_provider: SnapshotProvider | None = None,
        snapshot_differ: SnapshotDiffer | None = None,
    ) -> None:
        self._cfg = cfg
        self._shared = shared
        self._logger = cfg.logger
        if text_provider:
            self._text_provider = text_provider
        else:

            def _default_text_provider(p: Path) -> str:
                return p.read_text(encoding="utf-8")

            self._text_provider = _default_text_provider
        self._snapshot_provider = snapshot_provider or snapshot_file_states
        self._snapshot_differ = snapshot_differ or diff_file_states

    @property
    def cfg(self) -> PreparerConfig:
        return self._cfg

    @property
    def shared(self) -> MetadataConfig:
        return self._shared

    def run(self) -> PreparationOutcome:
        tokenizer_kind: TokenizerKind = self._resolve_tokenizer_type()
        # Prefer DI factory if provided
        if self._cfg.tokenizer_factory is not None:
            tokenizer = self._cfg.tokenizer_factory(tokenizer_kind)  # type: ignore[assignment]
        else:
            from ml_playground.framework.core.tokenizer import create_tokenizer

            tokenizer = create_tokenizer(tokenizer_kind)
        raw_text = self._load_raw_text()
        return self.prepare_from_text(raw_text, tokenizer)

    def prepare_from_text(
        self,
        text: str,
        tokenizer: Tokenizer,
        *,
        split: float | None = None,
        meta_extras: dict[str, Any] | None = None,
    ) -> PreparationOutcome:
        data_cfg = self._resolve_data_config()
        outputs = self._output_paths(data_cfg)
        before = self._snapshot_provider(outputs)

        ratio = float(split) if split is not None else self._default_split()
        train_arr, val_arr, meta, tokenizer = prepare_with_tokenizer(
            text,
            tokenizer,
            split=ratio,
        )

        if meta_extras:
            meta.update(meta_extras)

        write_bin_and_meta(
            self._shared.dataset_dir,
            train_arr,
            val_arr,
            meta,
            logger=self._logger,
            data_cfg=data_cfg,
        )

        created, updated, skipped = self._snapshot_differ(outputs, before)
        return PreparationOutcome(
            created_files=tuple(Path(path) for path in created),
            updated_files=tuple(Path(path) for path in updated),
            skipped_files=tuple(Path(path) for path in skipped),
            metadata=meta,
        )

    def _resolve_tokenizer_type(self) -> TokenizerKind:
        return coerce_tokenizer_type(self._cfg.tokenizer_type)

    def _resolve_data_config(self) -> DataConfig | None:
        data_cfg_obj: object | None = self._cfg.extras.get("data_config")
        if data_cfg_obj is None:
            return None
        if isinstance(data_cfg_obj, DataConfig):
            return data_cfg_obj
        raise DataError(
            "prepare.extras.data_config must be a DataConfig instance when provided",
            reason=(
                f"Received extras['data_config'] of type {type(data_cfg_obj).__name__}"
            ),
            rationale="Preparation extras rely on DataConfig for deterministic file layout",
        )

    def _default_split(self) -> float:
        raw_value: object | None = self._cfg.extras.get("split")
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
                rationale="Training/validation split must be numeric to derive dataset boundaries",
            ) from exc
        if ratio < 0.0 or ratio > 1.0:
            raise DataError(
                f"split ratio must be within [0.0, 1.0]; received {ratio}",
                reason="Split ratio outside inclusive [0.0, 1.0] range",
                rationale="Dataset preparation assumes ratios describe a valid probability interval",
            )
        return ratio

    def _load_raw_text(self) -> str:
        raw_text_path = self._cfg.raw_text_path
        if raw_text_path is not None:
            path = Path(raw_text_path)
            if self._cfg.read_text_fn is not None:
                return self._cfg.read_text_fn(path)
            # Use injected dependency if read_text_fn is not configured
            # Distinct handling for Iterable vs Callable to satisfy strict type checkers
            if callable(self._text_provider):
                return self._text_provider(path)

            # If not callable, treat as Iterable[str]
            return "".join(self._text_provider)
        raise DataError(
            "No raw text path provided in preparer config",
            reason="Preparer configuration missing raw_text_path",
            rationale="Prepare pipeline requires a seed corpus path to produce binaries",
        )

    def _output_paths(self, data_cfg: DataConfig | None) -> list[Path]:
        if data_cfg is not None:
            return [
                data_cfg.train_path(self._shared.dataset_dir),
                data_cfg.val_path(self._shared.dataset_dir),
                data_cfg.meta_path(self._shared.dataset_dir),
            ]
        return [
            self._shared.dataset_dir / "train.bin",
            self._shared.dataset_dir / "val.bin",
            self._shared.dataset_dir / "meta.pkl",
        ]

    def output_snapshot(self, paths: Iterable[Path]) -> dict[str, FileState]:
        return snapshot_file_states(paths)


def create_pipeline(
    cfg: PreparerConfig,
    shared: MetadataConfig,
    *,
    text_provider: TextProvider | None = None,
    snapshot_provider: SnapshotProvider | None = None,
    snapshot_differ: SnapshotDiffer | None = None,
) -> _PreparationPipeline:
    return _PreparationPipeline(
        cfg,
        shared,
        text_provider=text_provider,
        snapshot_provider=snapshot_provider,
        snapshot_differ=snapshot_differ,
    )


__all__ = [
    "PreparationOutcome",
    "TextProvider",
    "SnapshotProvider",
    "SnapshotDiffer",
    "create_pipeline",
    "snapshot_file_states",
    "diff_file_states",
]
