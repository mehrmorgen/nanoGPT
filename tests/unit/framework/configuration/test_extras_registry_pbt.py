from __future__ import annotations

import sys
from types import ModuleType

from hypothesis import given, settings, strategies as st
from pydantic import BaseModel

from ml_playground.framework.experiment_registry import extras_registry


class MockModel(BaseModel):
    pass


@given(
    experiment=st.text(min_size=1, max_size=24),
    section=st.text(min_size=1, max_size=24),
)
@settings(max_examples=20, deadline=None, derandomize=True)
def test_register_and_get_model(experiment: str, section: str) -> None:
    experiment = f"pbt_{experiment}"
    # Clear registry for this experiment to ensure clean state
    if experiment in extras_registry.EXTRAS_MODELS:
        del extras_registry.EXTRAS_MODELS[experiment]
    extras_registry.LOADED_EXPERIMENTS.discard(experiment)

    extras_registry.register_extras_model(experiment, section, MockModel)
    retrieved = extras_registry.get_extras_model(experiment, section)
    assert retrieved == MockModel


@given(
    experiment=st.text(min_size=1, max_size=24),
    section=st.text(min_size=1, max_size=24),
)
@settings(max_examples=20, deadline=None, derandomize=True)
def test_get_model_returns_none_for_missing(experiment: str, section: str) -> None:
    experiment = f"pbt_{experiment}"
    # Ensure it's not registered
    if experiment in extras_registry.EXTRAS_MODELS:
        del extras_registry.EXTRAS_MODELS[experiment]
    extras_registry.LOADED_EXPERIMENTS.discard(experiment)

    retrieved = extras_registry.get_extras_model(experiment, section)
    assert retrieved is None


def test_load_extras_models_skips_if_registered() -> None:
    extras_registry.EXTRAS_MODELS["existing"] = {"sec": MockModel}
    extras_registry.LOADED_EXPERIMENTS.add("existing")
    calls: list[str] = []

    def _import(module_path: str) -> None:
        calls.append(module_path)

    extras_registry.load_extras_models("existing", import_mod=_import)
    assert calls == []


def test_load_extras_models_uses_import_mod() -> None:
    if "new_exp" in extras_registry.EXTRAS_MODELS:
        del extras_registry.EXTRAS_MODELS["new_exp"]
    extras_registry.LOADED_EXPERIMENTS.discard("new_exp")

    calls: list[str] = []

    def _import(module_path: str) -> None:
        calls.append(module_path)

    extras_registry.load_extras_models("new_exp", import_mod=_import)
    assert calls == ["ml_playground.experiments.new_exp.extras"]


def test_fallback_logic_file_missing() -> None:
    if "fallback_exp" in extras_registry.EXTRAS_MODELS:
        del extras_registry.EXTRAS_MODELS["fallback_exp"]
    extras_registry.LOADED_EXPERIMENTS.discard("fallback_exp")

    def _import(_: str) -> None:
        raise ImportError

    extras_registry.load_extras_models("fallback_exp", import_mod=_import)


def test_singleton_initialization() -> None:
    # Remove the store module from sys.modules
    store_key = "ml_playground._extras_registry_store"
    if store_key in sys.modules:
        del sys.modules[store_key]

    # Reload to trigger initialization logic
    from importlib import reload

    reload(extras_registry)

    assert store_key in sys.modules
    models_value = getattr(sys.modules[store_key], "models", None)
    assert isinstance(models_value, dict)


def test_singleton_repair_corrupted_models() -> None:
    from importlib import reload

    store_key = "ml_playground._extras_registry_store"

    # Ensure module exists
    if store_key not in sys.modules:
        reload(extras_registry)

    # Corrupt the models attribute
    object.__setattr__(sys.modules[store_key], "models", "not a dict")

    # Reload should fix it
    reload(extras_registry)

    models_value = getattr(sys.modules[store_key], "models", None)
    assert isinstance(models_value, dict)
    assert models_value == {}
    loaded_value = getattr(sys.modules[store_key], "loaded", None)
    assert isinstance(loaded_value, set)
    assert loaded_value == set()


def test_singleton_repair_corrupted_store() -> None:
    from importlib import reload

    store_key = "ml_playground._extras_registry_store"

    # Corrupt the store itself
    sys.modules[store_key] = "not a module"  # type: ignore

    # Reload should fix it
    reload(extras_registry)

    assert isinstance(sys.modules[store_key], ModuleType)
    assert getattr(sys.modules[store_key], "models", None) is not None
    assert getattr(sys.modules[store_key], "loaded", None) is not None
