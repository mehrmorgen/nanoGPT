from typing import List
from src.ml_playground.experiments.generative_agents.models import (
    Agent,
    Memory,
    Location,
)
from src.ml_playground.llm import LLM


class Planning:
    """Handles the planning process for a generative agent."""

    def __init__(self, llm: LLM):
        self.llm = llm

    def generate_daily_plan(self, agent: Agent, recent_memories: List[Memory]) -> str:
        """
        Generates a daily plan for an agent.
        """
        prompt = self._generate_daily_plan_prompt(agent, recent_memories)
        return self.llm.generate(prompt)

    def generate_hourly_plan(
        self, agent: Agent, high_level_plan: str, location: "Location"
    ) -> str:
        """
        Generates an hourly plan for an agent.
        """
        prompt = self._generate_hourly_plan_prompt(agent, high_level_plan, location)
        return self.llm.generate(prompt)

    def _generate_daily_plan_prompt(
        self, agent: Agent, recent_memories: List[Memory]
    ) -> str:
        """
        Generates a prompt to create a daily plan for an agent.
        """
        memories_str = "\n".join([f"- {m.description}" for m in recent_memories])
        prompt = f"""
You are {agent.name}.
Your description is: {agent.description}
Here are your recent memories:
{memories_str}

Based on this, create a high-level plan for your day, with 5-8 items.
"""
        return prompt.strip()

    def _generate_hourly_plan_prompt(
        self, agent: Agent, high_level_plan: str, location: "Location"
    ) -> str:
        """
        Generates a prompt to break down a high-level plan into hourly plans.
        """
        actions_str = "\n".join(
            [f"- {a.name}: {a.description}" for a in location.available_actions]
        )
        prompt = f"""
You are {agent.name}.
Your high-level plan for the day is:
{high_level_plan}

Your current location is {location.name}.
Here are the available actions:
{actions_str}

Choose one action and create a plan for the next hour.
"""
        return prompt.strip()
