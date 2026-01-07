import pytest
from src.ml_playground.llm import GemmaLLM


@pytest.mark.integration
@pytest.mark.slow
def test_gemma_llm_generate():
    """Tests that the GemmaLLM can generate text."""
    llm = GemmaLLM()
    prompt = "Hello, my name is"
    response = llm.generate(prompt)
    assert isinstance(response, str)
    assert len(response) > 0
