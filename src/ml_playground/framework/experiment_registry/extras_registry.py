from __future__ import annotations

from pathlib import Path
import sys
from types import ModuleType
from typing import Any, Callable, Dict, Optional, cast
from importlib import import_module, reload
from importlib.util import module_from_spec, spec_from_file_location

from pydantic import BaseModel


_STORE_KEY = "ml_playground._extras_registry_store"
store_mod = sys.modules.get(_STORE_KEY)
if not isinstance(store_mod, ModuleType):
    store_mod = ModuleType(_STORE_KEY)
    setattr(store_mod, "models", {})
    sys.modules[_STORE_KEY] = store_mod

models_obj: object = getattr(store_mod, "models", {})
if not isinstance(models_obj, dict):
    models_obj = {}
    setattr(store_mod, "models", models_obj)

EXTRAS_MODELS: Dict[str, Dict[str, type[BaseModel]]] = cast(
    Dict[str, Dict[str, type[BaseModel]]], models_obj
)

loaded_obj: object = getattr(store_mod, "loaded", None)
if not isinstance(loaded_obj, set):
    loaded_obj = set()
    setattr(store_mod, "loaded", loaded_obj)

LOADED_EXPERIMENTS: set[str] = cast(set[str], loaded_obj)


def register_extras_model(
    experiment: str,
    section: str,
    model: type[BaseModel],
) -> None:
    """Register a strict Pydantic model for experiment extras in a given section."""
    by_section = EXTRAS_MODELS.setdefault(experiment, {})
    by_section[section] = model


def get_extras_model(experiment: str, section: str) -> Optional[type[BaseModel]]:
    return EXTRAS_MODELS.get(experiment, {}).get(section)


def load_extras_models(
    experiment: str,
    *,
    import_mod: Callable[[str], Any] | None = None,
) -> None:
    """Import experiment extras modules so they can register their models."""
    if experiment in LOADED_EXPERIMENTS:
        return
    _import = import_mod or import_module
    module_path = f"ml_playground.experiments.{experiment}.extras"
    try:
        module = _import(module_path)
        if EXTRAS_MODELS.get(experiment):
            LOADED_EXPERIMENTS.add(experiment)
            return
        if isinstance(module, ModuleType):
            try:
                reload(module)
            except (ImportError, AttributeError, RuntimeError, OSError):
                module = None
        if EXTRAS_MODELS.get(experiment):
            LOADED_EXPERIMENTS.add(experiment)
            return
    except (ImportError, AttributeError, RuntimeError):
        # Fall back to a direct file-based import. This is more robust than relying on
        # namespace-package resolution during pytest collection and installed-package
        # scenarios.
        extras_root = Path(__file__).resolve().parents[2] / "experiments"
        extras_file = extras_root / experiment / "extras.py"
        if not extras_file.exists():
            return
        spec = spec_from_file_location(module_path, extras_file)
        if spec is None or spec.loader is None:
            return
        module = module_from_spec(spec)
        try:
            spec.loader.exec_module(module)
        except (ImportError, AttributeError, RuntimeError, OSError):
            return
        if EXTRAS_MODELS.get(experiment):
            LOADED_EXPERIMENTS.add(experiment)


__all__ = [
    "EXTRAS_MODELS",
    "LOADED_EXPERIMENTS",
    "register_extras_model",
    "get_extras_model",
    "load_extras_models",
]
