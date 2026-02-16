from __future__ import annotations

import json
import importlib
from collections.abc import Mapping, Sequence
from datetime import datetime
from pathlib import Path
from typing import Any, Protocol, TypedDict, cast

from ml_playground.framework.configuration.models import RuntimeConfig, SamplerConfig
from ml_playground.framework.experiment_registry.protocol import (
    Sampler as _SamplerProto,
    SampleReport,
)
from ml_playground.framework.core.logging_protocol import LoggerLike


TokenBatch = Mapping[str, object | None]


class AnalysisResult(TypedDict):
    header: dict[str, str | None]
    lines: list[str]
    ngrams: dict[str, list[tuple[str, int]]]
    anomalies: list[str]


class _Tokenizer(Protocol):
    def __call__(self, text: str, *, return_tensors: str) -> TokenBatch: ...

    def decode(
        self,
        token_ids: object,
        *,
        skip_special_tokens: bool,
        clean_up_tokenization_spaces: bool,
    ) -> str: ...


class _Model(Protocol):
    def generate(
        self,
        *,
        input_ids: object,
        attention_mask: object | None = ...,
    ) -> Sequence[object]: ...


class _TokenizerFactory(Protocol):
    def __call__(self, model_path: Path, *, use_fast: bool) -> _Tokenizer: ...


class _BaseModelFactory(Protocol):
    def __call__(self, model_name: str) -> _Model: ...


class _PeftFactory(Protocol):
    def __call__(self, base_model: _Model, adapters_path: Path) -> _Model: ...


class _FallbackTokenizer:
    def __call__(self, text: str, *, return_tensors: str) -> TokenBatch:
        encoded = list(text.encode("utf-8"))
        return {
            "input_ids": encoded,
            "attention_mask": None,
            "return_tensors": return_tensors,
        }

    def decode(
        self,
        token_ids: object,
        *,
        skip_special_tokens: bool,
        clean_up_tokenization_spaces: bool,
    ) -> str:
        del skip_special_tokens, clean_up_tokenization_spaces
        if isinstance(token_ids, (bytes, bytearray)):
            return bytes(token_ids).decode("utf-8", errors="ignore")
        if isinstance(token_ids, Sequence):
            ints: list[int] = []
            for value in cast(Sequence[object], token_ids):
                if isinstance(value, int):
                    ints.append(int(value))
            return bytes(ints).decode("utf-8", errors="ignore")
        return str(token_ids)


class _FallbackModel:
    def generate(
        self,
        *,
        input_ids: object,
        attention_mask: object | None = None,
    ) -> Sequence[object]:
        del attention_mask
        return [input_ids]


class AutoTokenizer:
    @staticmethod
    def from_pretrained(*args: object, **kwargs: object) -> _Tokenizer:
        try:
            transformers_mod = importlib.import_module("transformers")
            tokenizer_cls = getattr(transformers_mod, "AutoTokenizer")
        except ImportError:
            return _FallbackTokenizer()
        tokenizer = tokenizer_cls.from_pretrained(*args, **kwargs)
        return cast(_Tokenizer, tokenizer)


class AutoModelForCausalLM:
    @staticmethod
    def from_pretrained(*args: object, **kwargs: object) -> _Model:
        try:
            transformers_mod = importlib.import_module("transformers")
            model_cls = getattr(transformers_mod, "AutoModelForCausalLM")
        except ImportError:
            return _FallbackModel()
        model = model_cls.from_pretrained(*args, **kwargs)
        return cast(_Model, model)


class PeftModel:
    @staticmethod
    def from_pretrained(base_model: _Model, adapters_path: Path) -> _Model:
        try:
            peft_mod = importlib.import_module("peft")
            peft_model_cls = getattr(peft_mod, "PeftModel")
        except ImportError:
            return base_model
        model = peft_model_cls.from_pretrained(base_model, adapters_path)
        return cast(_Model, model)


def _resolve_tokenizer_factory(
    factory: _TokenizerFactory | None,
) -> _TokenizerFactory:
    if factory is not None:
        return factory

    def _default(model_path: Path, *, use_fast: bool) -> _Tokenizer:
        tokenizer = AutoTokenizer.from_pretrained(
            str(model_path),
            use_fast=use_fast,  # type: ignore[arg-type]
        )
        return cast(_Tokenizer, tokenizer)

    return _default


def _resolve_base_model_factory(
    factory: _BaseModelFactory | None,
) -> _BaseModelFactory:
    if factory is not None:
        return factory

    def _default(model_name: str) -> _Model:
        model = AutoModelForCausalLM.from_pretrained(model_name)
        return cast(_Model, model)

    return _default


def _resolve_peft_factory(factory: _PeftFactory | None) -> _PeftFactory:
    if factory is not None:
        return factory

    def _default(base_model: _Model, adapters_path: Path) -> _Model:
        return PeftModel.from_pretrained(base_model, adapters_path)  # type: ignore[arg-type]

    return _default


def _config_path() -> Path:
    return Path(__file__).resolve().parent / "config.toml"


def config_path() -> Path:
    return _config_path()


def _load_best_stats(out_dir: Path) -> tuple[float | None, int | None]:
    try:
        import torch  # local import to avoid hard dep at import time
    except ImportError:
        return None, None

    best_path = out_dir / "state" / "best.pt"
    if not best_path.exists():
        return None, None

    obj: dict[str, Any] = torch.load(best_path, map_location="cpu")  # type: ignore[no-redef]
    raw_best = obj.get("best_val_loss", None)
    best_val: float | None
    if isinstance(raw_best, (int, float, str)):
        try:
            best_val = float(raw_best)
        except (TypeError, ValueError):
            best_val = None
    else:
        best_val = None
    raw_iter: object | None = obj.get("iter_num")
    try:
        iter_num: int = int(cast(Any, raw_iter))  # type: ignore[arg-type]
    except (TypeError, ValueError):
        iter_num = 0
    return best_val, iter_num


def analyze_text(text: str) -> AnalysisResult:
    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
    header: dict[str, str | None] = {"speaker": None, "topic": None, "year": None}
    # Simple header extraction
    for ln in lines[:5]:
        if ln.lower().startswith("sprecher:"):
            header["speaker"] = ln.split(":", 1)[1].strip() or None
        if ln.lower().startswith("thema:"):
            header["topic"] = ln.split(":", 1)[1].strip() or None
        if ln.lower().startswith("jahr:"):
            header["year"] = ln.split(":", 1)[1].strip() or None
    # Repetition analysis (1-grams)
    from collections import Counter

    cnt = Counter(lines)
    ngrams = {"1gram_top": cnt.most_common(5)}
    # Very lightweight anomalies
    anomalies: list[str] = []
    for ln, c in cnt.items():
        if c > 1 and ln not in anomalies:
            anomalies.append(f"repeated: {ln}")
        if ln.isdigit():
            anomalies.append(f"numeric_line: {ln}")
    return AnalysisResult(
        header=header,
        lines=lines,
        ngrams=ngrams,
        anomalies=anomalies,
    )


def _run_sampling(
    out_dir: Path,
    model_name: str,
    prompt: str,
    logger: LoggerLike,
    *,
    tokenizer_factory: _TokenizerFactory | None = None,
    base_model_factory: _BaseModelFactory | None = None,
    peft_model_factory: _PeftFactory | None = None,
) -> tuple[Path, Path]:
    samples_dir = out_dir / "samples"
    samples_dir.mkdir(parents=True, exist_ok=True)

    # Resolve factories with sensible defaults (heavy deps if available)
    _tok_factory = _resolve_tokenizer_factory(tokenizer_factory)
    _base_factory = _resolve_base_model_factory(base_model_factory)
    _peft_factory = _resolve_peft_factory(peft_model_factory)

    tokenizer_dir = out_dir / "tokenizer"
    tok: _Tokenizer = _tok_factory(tokenizer_dir, use_fast=True)
    base: _Model = _base_factory(model_name)
    model: _Model
    try:
        # build adapters path using Path joining, not bitwise and
        model = _peft_factory(base, out_dir / "adapters" / "best")
    except (FileNotFoundError, NotADirectoryError):
        model = base

    enc: TokenBatch = tok(prompt, return_tensors="pt")
    input_ids = enc.get("input_ids")
    attn = enc.get("attention_mask")
    if input_ids is None:
        raise ValueError("Tokenizer output missing input_ids")
    out: Sequence[object] = model.generate(input_ids=input_ids, attention_mask=attn)
    decoded_input = out[0]
    text = tok.decode(
        decoded_input,
        skip_special_tokens=True,
        clean_up_tokenization_spaces=False,
    )

    best_val_loss, _iter = _load_best_stats(out_dir)
    tag = (
        "best"
        if best_val_loss is not None
        else datetime.now().strftime("%Y%m%d_%H%M%S")
    )
    base_name = f"sample_{tag}"

    txt_path = samples_dir / f"{base_name}.txt"
    txt_path.write_text(text, encoding="utf-8")

    analysis = analyze_text(text)
    payload: dict[str, Any] = {
        "dataset": "speakger",
        "best_val_loss": best_val_loss,
        "iter_num": _iter,
        "analysis": analysis,
    }
    json_path = samples_dir / f"{base_name}.json"
    json_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8"
    )

    logger.info("[speakger] Sample analysis:")
    logger.info(f"== Lines == {len(analysis['lines'])}")
    return txt_path, json_path


class SpeakGerSampler(_SamplerProto):
    def sample(self, cfg: SamplerConfig) -> SampleReport:  # type: ignore[override]
        # Strict fail-fast: require concrete runtime injected by CLI (no runtime_ref resolution here)
        runtime = getattr(cfg, "runtime", None)
        if not isinstance(runtime, RuntimeConfig):
            raise ValueError(
                "SpeakGerSampler requires cfg.runtime to be provided (injected by CLI)"
            )
        out_dir = runtime.out_dir
        # Model name is expected to be provided via extras for this experiment
        extras = cast(Mapping[str, object], getattr(cfg, "extras", {}) or {})
        model_name = str(extras.get("hf_model_name", "dummy"))
        prompt = cfg.sample.start
        # Optional DI factories provided via cfg.extras for tests
        txt_path, json_path = _run_sampling(
            out_dir,
            model_name,
            prompt,
            cfg.logger,
            tokenizer_factory=cast(
                _TokenizerFactory | None, extras.get("tokenizer_factory")
            ),
            base_model_factory=cast(
                _BaseModelFactory | None, extras.get("base_model_factory")
            ),
            peft_model_factory=cast(
                _PeftFactory | None, extras.get("peft_model_factory")
            ),
        )
        return SampleReport(
            created_files=(txt_path, json_path),
            updated_files=(),
            skipped_files=(),
            messages=("[speakger] sample completed using injected SamplerConfig",),
        )
