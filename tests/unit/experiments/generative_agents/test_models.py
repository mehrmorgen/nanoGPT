from src.ml_playground.experiments.generative_agents.models import (
    Agent,
    Memory,
    Location,
    World,
    MemoryStream,
)
from datetime import datetime, timedelta


def test_create_agent():
    """Tests the creation of an Agent."""
    agent = Agent(name="John Doe", description="A test agent.")
    assert agent.name == "John Doe"
    assert agent.description == "A test agent."
    assert agent.memory_stream.memories == []


def test_create_memory():
    """Tests the creation of a Memory."""
    memory = Memory(description="I saw a cat.", importance=5)
    assert memory.description == "I saw a cat."
    assert memory.importance == 5


def test_add_memory_to_stream():
    """Tests adding a memory to the memory stream."""
    memory_stream = MemoryStream()
    memory = Memory(description="I saw a dog.", importance=8)
    memory_stream.add_memory(memory)
    assert len(memory_stream.memories) == 1
    assert memory_stream.memories[0].description == "I saw a dog."


def test_get_relevant_memories():
    """Tests retrieving relevant memories."""
    memory_stream = MemoryStream()
    now = datetime.utcnow()
    memory1 = Memory(
        description="memory 1", importance=5, created_at=now - timedelta(minutes=10)
    )
    memory2 = Memory(
        description="memory 2", importance=8, created_at=now - timedelta(minutes=5)
    )
    memory3 = Memory(description="memory 3", importance=3, created_at=now)
    memory_stream.add_memory(memory1)
    memory_stream.add_memory(memory2)
    memory_stream.add_memory(memory3)

    relevant_memories = memory_stream.get_relevant_memories("any query")
    assert len(relevant_memories) == 3
    assert relevant_memories[0].description == "memory 3"
    assert relevant_memories[1].description == "memory 2"
    assert relevant_memories[2].description == "memory 1"


def test_create_location():
    """Tests the creation of a Location."""
    location = Location(name="Town Square", description="A bustling town square.")
    assert location.name == "Town Square"
    assert location.description == "A bustling town square."


def test_create_world():
    """Tests the creation of a World."""
    location = Location(name="Town Square", description="A bustling town square.")
    world = World(locations=[location])
    assert len(world.locations) == 1
    assert world.locations[0].name == "Town Square"
