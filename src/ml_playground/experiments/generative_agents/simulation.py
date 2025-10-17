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

    def perceive(self, agent: Agent) -> List[str]:
        """Generates a list of perceptions for an agent."""
        perceptions = []
        for other_agent in self.world.agents:
            if (
                other_agent.name != agent.name
                and other_agent.location == agent.location
            ):
                perceptions.append(f"I saw {other_agent.name}.")
        return perceptions

    def act(self, agent: Agent, plan: str):
        """Executes an action for an agent based on a plan."""
        if "move" in plan.lower():
            # Simplified action: move to a random new location
            current_location = agent.location
            new_location = self.world.locations[
                (self.world.locations.index(current_location) + 1)
                % len(self.world.locations)
            ]
            current_location.agents.remove(agent)
            new_location.agents.append(agent)
            agent.location = new_location
            print(f"  Action: {agent.name} moved to {new_location.name}")

    def step(self):
        """Advances the simulation by one time step."""
        print("Simulation step...")
        for agent in self.agents:
            print(f"Processing agent: {agent.name}")
            # 1. Perceive
            perceptions = self.perceive(agent)
            for perception in perceptions:
                agent.memory_stream.add_memory(
                    Memory(description=perception, importance=5)
                )

            # 2. Retrieve
            query = " ".join(perceptions)
            recent_memories = agent.memory_stream.get_relevant_memories(query)

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
            self.act(agent, hourly_plan)
