from __future__ import annotations

from pathlib import Path
import pytest
import logging
from typing import Any, Sequence, Mapping
from ml_playground.experiments.speakger.sampler import SpeakGerSampler
from ml_playground.framework.configuration.models import (
    SamplerConfig,
    SampleConfig,
    RuntimeConfig,
)


# Mocking the interfaces defined in sampler.py for DI
class FakeTokenizer:
    def __call__(self, text: str, *, return_tensors: str) -> Mapping[str, Any]:
        return {
            "input_ids": list(text.encode("utf-8")),
            "attention_mask": [1] * len(text.encode("utf-8")),
        }

    def decode(
        self,
        token_ids: Any,
        *,
        skip_special_tokens: bool,
        clean_up_tokenization_spaces: bool,
    ) -> str:
        if isinstance(token_ids, (bytes, bytearray)):
            return bytes(token_ids).decode("utf-8", errors="ignore")
        if isinstance(token_ids, Sequence):
            return bytes([int(x) for x in token_ids if isinstance(x, int)]).decode(
                "utf-8", errors="ignore"
            )
        return str(token_ids)


class FakeModel:
    def generate(self, *, input_ids: Any, attention_mask: Any = None) -> Sequence[Any]:
        return [input_ids]


def test_fallback_tokenizer_and_model_basic(tmp_path: Path) -> None:
    out_dir = tmp_path / "out"
    out_dir.mkdir()
    (out_dir / "tokenizer").mkdir()

    logger = logging.getLogger("test_speakger")
    runtime = RuntimeConfig(
        out_dir=out_dir,
        max_iters=0,
        eval_interval=1,
        eval_iters=1,
        log_interval=1,
        eval_only=False,
        checkpointing=RuntimeConfig.Checkpointing(),
        seed=1,
        device="cpu",
        dtype="float32",
        compile=False,
    )
    sample_cfg = SampleConfig(
        start="Hello",
        num_samples=1,
        max_new_tokens=4,
        temperature=0.1,
        top_k=1,
    )

    # Use DI instead of monkeypatch
    config = SamplerConfig(
        runtime=runtime,
        sample=sample_cfg,
        logger=logger,
        extras={
            "hf_model_name": "dummy",
            "tokenizer_factory": lambda *_, **__: FakeTokenizer(),
            "base_model_factory": lambda *_, **__: FakeModel(),
            "peft_model_factory": lambda base, _, **__: base,
        },
    )

    sampler = SpeakGerSampler()
    report = sampler.sample(config)

    assert len(report.created_files) == 2
    txt_path = report.created_files[0]
    json_path = report.created_files[1]

    assert txt_path.exists()
    assert json_path.exists()

    text = txt_path.read_text(encoding="utf-8")
    assert text == "Hello"


def test_fake_tokenizer_decode_sequence() -> None:
    tokenizer = FakeTokenizer()

    # Test sequence of ints
    decoded = tokenizer.decode(
        [72, 101, 108, 108, 111],
        skip_special_tokens=True,
        clean_up_tokenization_spaces=False,
    )
    assert decoded == "Hello"

    # Test bytes
    decoded_bytes = tokenizer.decode(
        b"Hello", skip_special_tokens=True, clean_up_tokenization_spaces=False
    )
    assert decoded_bytes == "Hello"


def test_sampler_requires_runtime() -> None:
    sampler = SpeakGerSampler()
    config = SamplerConfig.model_construct(runtime=None)
    with pytest.raises(ValueError, match="SpeakGerSampler requires cfg.runtime"):
        sampler.sample(config)


def test_analyze_text_header_extraction() -> None:
    from ml_playground.experiments.speakger.sampler import analyze_text

    text = "Sprecher: Alice\nThema: AI\nJahr: 2024\nSome content."
    result = analyze_text(text)
    assert result["header"]["speaker"] == "Alice"
    assert result["header"]["topic"] == "AI"
    assert result["header"]["year"] == "2024"


def test_analyze_text_anomalies() -> None:
    from ml_playground.experiments.speakger.sampler import analyze_text

    text = "Repeated line\nRepeated line\n12345"
    result = analyze_text(text)
    assert any("repeated: Repeated line" in a for a in result["anomalies"])
    assert any("numeric_line: 12345" in a for a in result["anomalies"])
