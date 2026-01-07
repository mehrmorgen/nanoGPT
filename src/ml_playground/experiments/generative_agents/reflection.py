from typing import List
from src.ml_playground.experiments.generative_agents.models import Memory
from src.ml_playground.llm import LLM


class Reflection:
    """Handles the reflection process for a generative agent."""

    def __init__(self, llm: LLM):
        self.llm = llm

    def generate_reflection_questions(
        self, _recent_memories: List[Memory]
    ) -> List[str]:
        """
        Generates questions for reflection based on recent memories.
        This is a simplified version that returns a fixed set of questions.
        """
        # In a real implementation, this would use an LLM to generate questions.
        # For now, we'll return a hardcoded list for testing purposes.
        return [
            "What is the most important topic I have been thinking about lately?",
            "What are my recent interactions with others about?",
        ]

    def generate_insights(self, question: str, relevant_memories: List[Memory]) -> str:
        """
        Generates insights from an LLM.
        """
        prompt = self._generate_insights_prompt(question, relevant_memories)
        return self.llm.generate(prompt)

    def _generate_insights_prompt(
        self, question: str, relevant_memories: List[Memory]
    ) -> str:
        """
        Generates a prompt to get insights from an LLM.
        """
        memories_str = "\n".join([f"- {m.description}" for m in relevant_memories])
        prompt = f"""
You are a generative agent. You are reflecting on your recent experiences.
Here is a question to reflect on: {question}
Here are some relevant memories:
{memories_str}

Based on these memories, what are your insights?
"""
        return prompt.strip()
