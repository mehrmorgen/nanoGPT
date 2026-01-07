from src.ml_playground.experiments.generative_agents.planning import Planning
from src.ml_playground.experiments.generative_agents.models import Agent, Memory


def test_generate_daily_plan_prompt():
    """Tests the generation of the daily plan prompt."""
    planning = Planning()
    agent = Agent(name="John Doe", description="A test agent.")
    recent_memories = [
        Memory(description="I need to buy groceries.", importance=8),
        Memory(description="I have a meeting at 2pm.", importance=9),
    ]
    prompt = planning.generate_daily_plan_prompt(agent, recent_memories)
    assert agent.name in prompt
    assert agent.description in prompt
    for memory in recent_memories:
        assert memory.description in prompt


def test_generate_hourly_plan_prompt():
    """Tests the generation of the hourly plan prompt."""
    planning = Planning()
    agent = Agent(name="John Doe", description="A test agent.")
    high_level_plan = "1. Go to work. 2. Have lunch. 3. Go home."
    prompt = planning.generate_hourly_plan_prompt(agent, high_level_plan)
    assert agent.name in prompt
    assert high_level_plan in prompt
