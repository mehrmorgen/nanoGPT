from src.ml_playground.experiments.generative_agents.simulation import Simulation
from src.ml_playground.experiments.generative_agents.models import (
    World,
    Agent,
    Location,
    Action,
)


def run_simulation(num_steps: int):
    """Initializes and runs the simulation."""
    # 1. Create the world
    move_action = Action(name="move", description="Move to a different location.")
    locations = [
        Location(
            name="Town Square",
            description="A bustling town square.",
            available_actions=[move_action],
        ),
        Location(
            name="Library",
            description="A quiet place to read.",
            available_actions=[move_action],
        ),
    ]
    world = World(locations=locations)

    # 2. Create agents
    agents = [
        Agent(
            name="John Doe",
            description="A curious explorer.",
            location=locations[0],
        ),
    ]
    locations[0].agents.append(agents[0])
    world.agents.extend(agents)

    # 3. Initialize the simulation
    simulation = Simulation(world=world, agents=agents)

    # 4. Run the simulation
    for i in range(num_steps):
        print(f"--- Step {i + 1} ---")
        simulation.step()


if __name__ == "__main__":
    run_simulation(num_steps=5)
