from __future__ import annotations

from typing import (
    Callable,
    Generic,
    Iterable,
    Iterator,
    Mapping,
    MutableMapping,
    TypeVar,
)


__all__ = ["Registry", "RegistryItemFactory"]

T = TypeVar("T")
RegistryItemFactory = Callable[..., T]


class Registry(Generic[T]):
    """Simple name -> factory registry used for plugin-style resolution."""

    def __init__(self) -> None:
        self._factories: MutableMapping[str, RegistryItemFactory[T]] = {}

    def register(self, name: str, factory: RegistryItemFactory[T]) -> None:
        if not name:
            raise ValueError("Registry names must be non-empty")
        if name in self._factories:
            raise ValueError(f"Registry entry '{name}' is already registered")
        self._factories[name] = factory

    def get(self, name: str) -> RegistryItemFactory[T]:
        try:
            return self._factories[name]
        except KeyError as exc:
            raise KeyError(f"Registry entry not found: {name}") from exc

    def names(self) -> Iterable[str]:
        return self._factories.keys()

    def items(self) -> Mapping[str, RegistryItemFactory[T]]:
        return dict(self._factories)

    def __contains__(self, name: object) -> bool:
        return name in self._factories

    def __iter__(self) -> Iterator[str]:
        return iter(self._factories)
