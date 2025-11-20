from __future__ import annotations

from pathlib import Path
from typing import Sequence

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

    # Temporarily mock PEFT as unavailable to force the fallback
    original_peft = sys.modules.get("peft")
    sys.modules["peft"] = None

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
    assert hasattr(sampler_module, "AutoTokenizer")
    assert hasattr(sampler_module, "AutoModelForCausalLM")
    assert hasattr(sampler_module, "PeftModel")
