from typing import List
from src.ml_playground.experiments.generative_agents.models import World, Agent, Memory
from src.ml_playground.llm import GemmaLLM
from src.ml_playground.experiments.generative_agents.reflection import Reflection
from src.ml_playground.experiments.generative_agents.planning import Planning


class Simulation:
    """Manages the state of the world and the agents."""

    def __init__(self, world: World, agents: List[Agent]):
        self.world = world
        self.agents = agents
        self.llm = GemmaLLM()
        self.reflection = Reflection(self.llm)
        self.planning = Planning(self.llm)

    def step(self):
        """Advances the simulation by one time step."""
        print("Simulation step...")
        for agent in self.agents:
            print(f"Processing agent: {agent.name}")
            # 1. Perceive
            # For now, perception is not implemented.

            # 2. Retrieve
            recent_memories = agent.memory_stream.get_relevant_memories(
                ""
            )  # Empty query for now

            # 3. Reflect
            if len(recent_memories) > 0:
                # For simplicity, we reflect on every step for now.
                questions = self.reflection.generate_reflection_questions(
                    recent_memories
                )
                for question in questions:
                    insights = self.reflection.generate_insights(
                        question, recent_memories
                    )
                    print(f"  Reflection: {insights}")
                    # Add insights as new memories
                    agent.memory_stream.add_memory(
                        Memory(description=insights, importance=8)
                    )

            # 4. Plan
            # For simplicity, we plan on every step for now.
            daily_plan = self.planning.generate_daily_plan(agent, recent_memories)
            print(f"  Daily Plan: {daily_plan}")
            hourly_plan = self.planning.generate_hourly_plan(agent, daily_plan)
            print(f"  Hourly Plan: {hourly_plan}")

            # 5. Act
            # For now, the agent does not act on the plan.
