from abc import ABC, abstractmethod
from transformers import pipeline
import torch


class LLM(ABC):
    """Abstract base class for a large language model."""

    @abstractmethod
    def generate(self, prompt: str) -> str:
        """Generates a response from the LLM."""
        pass


class GemmaLLM(LLM):
    """A wrapper for a Gemma model."""

    def __init__(self, model_name: str = "google/gemma-2b"):
        self.pipe = pipeline(
            "text-generation",
            model=model_name,
            torch_dtype=torch.bfloat16,
            device_map="auto",
        )

    def generate(self, prompt: str) -> str:
        """Generates a response from the LLM."""
        outputs = self.pipe(
            prompt,
            max_new_tokens=256,
            do_sample=True,
            temperature=0.7,
            top_k=50,
            top_p=0.95,
        )
        # The output includes the prompt, so we need to remove it.
        # The exact format of the output may vary depending on the model and library version.
        # This is a potential source of errors.
        if isinstance(outputs, list) and outputs and "generated_text" in outputs[0]:
            generated_text = outputs[0]["generated_text"]
            if isinstance(generated_text, str) and generated_text.startswith(prompt):
                return generated_text[len(prompt) :].strip()
            return generated_text.strip()
        return ""
