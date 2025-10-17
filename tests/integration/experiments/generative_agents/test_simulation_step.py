import pytest
from src.ml_playground.experiments.generative_agents.simulation import Simulation
from src.ml_playground.experiments.generative_agents.models import (
    World,
    Agent,
    Location,
    Memory,
)


@pytest.mark.integration
@pytest.mark.slow
def test_simulation_step():
    """Tests a single step of the simulation."""
    # 1. Create the world
    locations = [
        Location(name="Town Square", description="A bustling town square."),
    ]
    world = World(locations=locations)

    # 2. Create agents
    agent = Agent(
        name="John Doe",
        description="A curious explorer.",
        location=locations[0],
    )
    locations[0].agents.append(agent)
    world.agents.append(agent)
    agent.memory_stream.add_memory(Memory(description="I saw a bird.", importance=5))
    agents = [agent]

    # 3. Initialize the simulation
    simulation = Simulation(world=world, agents=agents)

    # 4. Run one step
    simulation.step()

    # 5. Check if new memories were added
    assert len(agent.memory_stream.memories) > 1
