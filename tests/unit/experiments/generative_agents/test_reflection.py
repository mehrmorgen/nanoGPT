from src.ml_playground.experiments.generative_agents.reflection import Reflection
from src.ml_playground.experiments.generative_agents.models import Memory


def test_generate_reflection_questions():
    """Tests the generation of reflection questions."""
    reflection = Reflection()
    recent_memories = [
        Memory(description="I saw a cat.", importance=5),
        Memory(description="I talked to John Doe.", importance=8),
    ]
    questions = reflection.generate_reflection_questions(recent_memories)
    assert isinstance(questions, list)
    assert len(questions) > 0


def test_generate_insights_prompt():
    """Tests the generation of the insights prompt."""
    reflection = Reflection()
    question = "What is the most important topic I have been thinking about lately?"
    relevant_memories = [
        Memory(description="I read a book about space.", importance=7),
        Memory(description="I watched a documentary about Mars.", importance=8),
    ]
    prompt = reflection.generate_insights_prompt(question, relevant_memories)
    assert question in prompt
    for memory in relevant_memories:
        assert memory.description in prompt
