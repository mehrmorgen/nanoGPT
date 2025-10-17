from pydantic import BaseModel, Field
from datetime import datetime, UTC
from typing import List


class Memory(BaseModel):
    """A memory of an agent."""

    description: str
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    importance: int = Field(gt=0, le=10)


class MemoryStream(BaseModel):
    """A memory stream for a generative agent."""

    memories: List[Memory] = []

    def add_memory(self, memory: Memory):
        """Adds a memory to the memory stream."""
        self.memories.append(memory)

    def get_relevant_memories(self, _query: str) -> List[Memory]:
        """
        Retrieves relevant memories from the memory stream.
        For now, this is a simplified implementation that returns the most recent memories.
        """
        # Sort memories by creation time in descending order (most recent first)
        sorted_memories = sorted(
            self.memories, key=lambda m: m.created_at, reverse=True
        )
        return sorted_memories[:10]  # Return the 10 most recent memories


class Agent(BaseModel):
    """A generative agent."""

    name: str
    description: str
    memory_stream: MemoryStream = Field(default_factory=MemoryStream)


class Location(BaseModel):
    """A location in the world."""

    name: str
    description: str


class World(BaseModel):
    """The text-based world."""

    locations: List[Location] = []
