from __future__ import annotations

from pathlib import Path
from typing import Callable, Optional, Any, Mapping, cast, Protocol
import numpy as np
import requests
import requests.exceptions
from ml_playground.framework.configuration.models import PreparerConfig
from ml_playground.framework.data_pipeline.transforms.tokenization import (
    create_standardized_metadata,
    split_train_val,
)
from ml_playground.framework.data_pipeline.transforms.io import (
    diff_file_states,
    snapshot_file_states,
    write_bin_and_meta,
)
from ml_playground.framework.core.tokenizer import create_tokenizer
from ml_playground.framework.core.tokenizer_protocol import Tokenizer
from ml_playground.framework.experiment_registry.protocol import (
    Preparer as _PreparerProto,
    PrepareReport,
)
from ml_playground.framework.core.error_handling import (
    DataError,
    validate_file_exists,
    ProgressReporter,
)
from ml_playground.framework.core.logging_protocol import LoggerLike

DATA_URL = "https://raw.githubusercontent.com/karpathy/char-rnn/master/data/tinyshakespeare/input.txt"


class _WriterFn(Protocol):
    def __call__(
        self,
        ds_dir: Path,
        train_ids: np.ndarray,
        val_ids: np.ndarray,
        meta: dict[str, Any],
        *,
        logger: LoggerLike,
    ) -> None: ...


class ShakespearePreparer(_PreparerProto):
    def prepare(self, cfg: PreparerConfig) -> PrepareReport:  # type: ignore[override]
        extras = cast(Mapping[str, object], getattr(cfg, "extras", {}) or {})
        base_dir = extras.get("base_dir")
        exp_dir = (
            Path(base_dir)
            if isinstance(base_dir, (str, Path))
            else Path(__file__).resolve().parent
        )
        ds_dir = exp_dir / "datasets"
        ds_dir.mkdir(parents=True, exist_ok=True)
        outputs = [ds_dir / "train.bin", ds_dir / "val.bin", ds_dir / "meta.pkl"]

        pre = snapshot_file_states(outputs)

        f_input = ds_dir / "input.txt"

        if not f_input.exists():
            # Allow injectable http_get for tests
            http_get: object | None = extras.get("http_get")
            try:
                _get = http_get if callable(http_get) else requests.get
                resp = _get(DATA_URL, timeout=30)
                # Support simple fake objects without raise_for_status
                rfs = getattr(resp, "raise_for_status", None)
                if callable(rfs):
                    rfs()
                text = getattr(resp, "text", None)
                if text is None:
                    raise DataError(
                        "http_get did not return an object with .text",
                        reason="Injected HTTP client returned response without 'text' attribute",
                        rationale="Dataset download expects a text payload to seed the corpus",
                    )
                text_str = cast(str, text)
                f_input.write_text(text_str, encoding="utf-8")
            except requests.exceptions.RequestException as e:
                raise DataError(
                    f"Failed to download Shakespeare dataset: {e}",
                    reason=f"HTTP request raised {e.__class__.__name__}",
                    rationale="Shakespeare preparer requires network access or cached input.txt",
                ) from e

        validate_file_exists(f_input, "Shakespeare input file")

        data = f_input.read_text(encoding="utf-8")
        train_text, val_text = split_train_val(data)

        logger = cfg.logger
        progress = ProgressReporter(logger, total_steps=4)

        progress.start("Starting Shakespeare dataset preparation")

        # Allow injectable tokenizer factory for tests
        tok_factory: Optional[Callable[[], Tokenizer]] = None
        tf = extras.get("tokenizer_factory")
        if callable(tf):
            tok_factory = cast(Callable[[], Tokenizer], tf)
        tokenizer: Tokenizer = (
            tok_factory()
            if tok_factory is not None
            else create_tokenizer("tiktoken", encoding_name="gpt2")
        )

        progress.update(1, "Creating tokenizer")

        progress.update(1, "Encoding training data")
        train_ids = np.array(tokenizer.encode(train_text), dtype=np.uint16)
        progress.update(1, "Encoding validation data")
        val_ids = tokenizer.encode(val_text)

        train_ids_arr: np.ndarray = np.array(train_ids, dtype=np.uint16)
        val_ids_arr: np.ndarray = np.array(val_ids, dtype=np.uint16)
        progress.update(1, "Creating metadata")
        meta = create_standardized_metadata(tokenizer, len(train_ids), len(val_ids))

        # Allow injectable writer function for tests
        writer_fn: object | None = extras.get("writer_fn")
        if callable(writer_fn):
            cast(_WriterFn, writer_fn)(
                ds_dir, train_ids_arr, val_ids_arr, meta, logger=cfg.logger
            )
        else:
            write_bin_and_meta(
                ds_dir, train_ids_arr, val_ids_arr, meta, logger=cfg.logger
            )

        progress.finish("Shakespeare dataset preparation completed")

        created, updated, skipped = diff_file_states(outputs, pre)
        created_paths = [Path(path) for path in created]
        updated_paths = [Path(path) for path in updated]
        skipped_paths = [Path(path) for path in skipped]

        msgs = (
            f"[shakespeare] prepared dataset at {ds_dir}",
            f"[shakespeare.outputs.created] {[str(p) for p in created_paths]}",
            f"[shakespeare.outputs.updated] {[str(p) for p in updated_paths]}",
            f"[shakespeare.outputs.skipped] {[str(p) for p in skipped_paths]}",
        )
        return PrepareReport(
            created_files=tuple(created_paths),
            updated_files=tuple(updated_paths),
            skipped_files=tuple(skipped_paths),
            messages=msgs,
        )
