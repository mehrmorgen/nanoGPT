from __future__ import annotations

from pathlib import Path
from types import ModuleType
from typing import Sequence, cast

import pytest

from ml_playground.experiments.speakger import sampler


def test_fallback_tokenizer_functionality() -> None:
    """Test that fallback tokenizer implementation works correctly."""

    # Test the fallback tokenizer directly by accessing the class when fallbacks are active
    # Since the import guards make the classes available at module level, we can test them
    tokenizer_class = getattr(sampler, "_FallbackTokenizer", None)
    if tokenizer_class is None:
        # If fallbacks aren't active, skip this test
        return

    tokenizer = tokenizer_class()

    # Test encoding
    result = tokenizer("hello", return_tensors="pt")
    assert "input_ids" in result
    assert result["input_ids"] == [104, 101, 108, 108, 111]  # "hello".encode("utf-8")
    assert result["attention_mask"] is None
    assert result["return_tensors"] == "pt"

    # Test decoding bytes
    decoded = tokenizer.decode(
        b"hello", skip_special_tokens=True, clean_up_tokenization_spaces=False
    )
    assert decoded == "hello"

    # Test decoding list of ints
    decoded = tokenizer.decode(
        [104, 101, 108, 108, 111],
        skip_special_tokens=True,
        clean_up_tokenization_spaces=False,
    )
    assert decoded == "hello"

    # Test decoding sequence
    decoded = tokenizer.decode(
        (104, 101, 108, 108, 111),
        skip_special_tokens=True,
        clean_up_tokenization_spaces=False,
    )
    assert decoded == "hello"


def test_fallback_model_functionality() -> None:
    """Test that fallback model implementation works correctly."""

    # Test the fallback model directly by accessing the class when fallbacks are active
    model_class = getattr(sampler, "_FallbackModel", None)
    if model_class is None:
        # If fallbacks aren't active, skip this test
        return

    # Test the fallback model directly
    model = model_class()

    # Test generation
    output = model.generate(input_ids=[1, 2, 3])
    assert output == [[1, 2, 3]]  # Should return input unchanged

    # Test generation with attention mask (should be ignored)
    output = model.generate(input_ids=[1, 2, 3], attention_mask=[1, 1, 1])
    assert output == [[1, 2, 3]]


def test_fallback_peft_functionality() -> None:
    """Test that fallback PeftModel works correctly."""

    import sys
    from typing import Any, cast

    # Temporarily mock PEFT as unavailable to force the fallback
    original_peft = sys.modules.get("peft")
    sys.modules["peft"] = cast(Any, None)

    # Force reimport of the sampler module to use fallbacks
    if "ml_playground.experiments.speakger.sampler" in sys.modules:
        del sys.modules["ml_playground.experiments.speakger.sampler"]

    try:
        # Reimport with fallbacks active
        import ml_playground.experiments.speakger.sampler as test_sampler

        # Create a mock model that conforms to the _Model protocol
        class MockModel:
            def generate(
                self,
                *,
                input_ids: object,
                attention_mask: object | None = None,
            ) -> Sequence[object]:
                return [input_ids]

        base_model = MockModel()
        adapters_path = Path("/dummy/path")

        # Test the fallback PeftModel directly
        result = test_sampler.PeftModel.from_pretrained(base_model, adapters_path)
        assert result is base_model  # Fallback just returns the base model

    finally:
        # Restore original PEFT module
        if original_peft is not None:
            sys.modules["peft"] = original_peft
        elif "peft" in sys.modules:
            del sys.modules["peft"]

        # Clean up the forced reimport
        if "ml_playground.experiments.speakger.sampler" in sys.modules:
            del sys.modules["ml_playground.experiments.speakger.sampler"]


def test_imports_work_when_available() -> None:
    """Test that module can be imported without errors."""

    # Just verify the module can be imported without errors
    import ml_playground.experiments.speakger.sampler as sampler_module

    # Verify the expected classes exist (either real or fallback)
    assert getattr(sampler_module, "AutoTokenizer", None) is not None
    assert getattr(sampler_module, "AutoModelForCausalLM", None) is not None
    assert getattr(sampler_module, "PeftModel", None) is not None


def test_default_factories_delegate() -> None:
    original_tok = sampler.AutoTokenizer.from_pretrained
    original_model = sampler.AutoModelForCausalLM.from_pretrained
    original_peft = sampler.PeftModel.from_pretrained
    sampler.AutoTokenizer.from_pretrained = staticmethod(  # type: ignore[assignment]
        lambda *args, **kwargs: ("tok", args, kwargs)
    )
    sampler.AutoModelForCausalLM.from_pretrained = staticmethod(  # type: ignore[assignment]
        lambda *args, **kwargs: ("model", args, kwargs)
    )
    sampler.PeftModel.from_pretrained = staticmethod(  # type: ignore[assignment]
        lambda *args, **kwargs: ("peft", args, kwargs)
    )

    try:
        tok_factory = sampler._resolve_tokenizer_factory(None)
        base_factory = sampler._resolve_base_model_factory(None)
        peft_factory = sampler._resolve_peft_factory(None)

        assert tok_factory(Path("/x"), use_fast=False) == (
            "tok",
            ("/x",),
            {"use_fast": False},
        )
        assert base_factory("hf/model") == ("model", ("hf/model",), {})

        class _DummyModel:
            def generate(
                self,
                *,
                input_ids: object,
                attention_mask: object | None = None,
            ) -> Sequence[object]:
                del attention_mask
                return [input_ids]

        base_model = cast(sampler._Model, _DummyModel())
        assert peft_factory(base_model, Path("/adapters")) == (
            "peft",
            (base_model, Path("/adapters")),
            {},
        )
    finally:
        sampler.AutoTokenizer.from_pretrained = original_tok  # type: ignore[assignment]
        sampler.AutoModelForCausalLM.from_pretrained = original_model  # type: ignore[assignment]
        sampler.PeftModel.from_pretrained = original_peft  # type: ignore[assignment]


def test_load_best_stats_handles_unexpected_payload(tmp_path: Path) -> None:
    best = tmp_path / "state" / "best.pt"
    best.parent.mkdir(parents=True)
    best.write_bytes(b"stub")

    fake_torch = ModuleType("torch")
    fake_torch.load = lambda *_args, **_kwargs: {  # type: ignore[attr-defined]
        "best_val_loss": object(),
        "iter_num": "bad",
    }
    import sys

    original_torch = sys.modules.get("torch")
    try:
        sys.modules["torch"] = cast(ModuleType, fake_torch)
        best_val, iter_num = sampler._load_best_stats(tmp_path)
    finally:
        if original_torch is None:
            sys.modules.pop("torch", None)
        else:
            sys.modules["torch"] = original_torch
    assert best_val is None
    assert iter_num == 0


def test_run_sampling_handles_missing_adapters_and_missing_input_ids(
    tmp_path: Path,
) -> None:
    out_dir = tmp_path / "run"
    out_dir.mkdir()

    class _Tok:
        def __call__(self, *_args: object, **_kwargs: object) -> dict[str, object]:
            return {"attention_mask": [1, 1]}

        def decode(
            self,
            token_ids: object,
            *,
            skip_special_tokens: bool,
            clean_up_tokenization_spaces: bool,
        ) -> str:
            del token_ids, skip_special_tokens, clean_up_tokenization_spaces
            return ""

    class _Model:
        def generate(
            self,
            *,
            input_ids: object,
            attention_mask: object | None = None,
        ) -> Sequence[object]:
            del input_ids, attention_mask
            return [""]

    class _Logger:
        def debug(self, *_args: object, **_kwargs: object) -> None:
            return

        def info(self, *_args: object, **_kwargs: object) -> None:
            return

        def warning(self, *_args: object, **_kwargs: object) -> None:
            return

        def error(self, *_args: object, **_kwargs: object) -> None:
            return

    def _tokenizer_factory(model_path: Path, *, use_fast: bool) -> sampler._Tokenizer:
        del model_path, use_fast
        return cast(sampler._Tokenizer, _Tok())

    def _base_model_factory(model_name: str) -> sampler._Model:
        del model_name
        return cast(sampler._Model, _Model())

    def _peft_factory(
        base_model: sampler._Model, adapters_path: Path
    ) -> sampler._Model:
        del base_model, adapters_path
        raise FileNotFoundError("no adapters")

    with pytest.raises(ValueError, match="missing input_ids"):
        sampler._run_sampling(
            out_dir,
            "hf/model",
            "prompt",
            cast(sampler.LoggerLike, _Logger()),
            tokenizer_factory=_tokenizer_factory,
            base_model_factory=_base_model_factory,
            peft_model_factory=_peft_factory,
        )
