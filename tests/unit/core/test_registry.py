from __future__ import annotations
import pytest
from ml_playground.core.registry import Registry


def test_registry_register_and_get():
    registry: Registry[int] = Registry()
    registry.register("one", lambda: 1)
    assert registry.get("one")() == 1


def test_registry_register_duplicate_raises():
    registry: Registry[int] = Registry()
    registry.register("one", lambda: 1)
    with pytest.raises(ValueError, match="already registered"):
        registry.register("one", lambda: 2)


def test_registry_get_missing_raises():
    registry: Registry[int] = Registry()
    with pytest.raises(KeyError, match="not found"):
        registry.get("missing")


def test_registry_names():
    registry: Registry[int] = Registry()
    registry.register("a", lambda: 1)
    registry.register("b", lambda: 2)
    assert set(registry.names()) == {"a", "b"}


def test_registry_metadata():
    registry: Registry[int] = Registry()
    registry.register("a", lambda: 1)
    assert "a" in registry
    assert "b" not in registry
    assert list(iter(registry)) == ["a"]
    assert registry.items() == {"a": registry.get("a")}


def test_registry_empty_name_raises():
    registry: Registry[int] = Registry()
    with pytest.raises(ValueError, match="non-empty"):
        registry.register("", lambda: 1)
