from __future__ import annotations

from typing import Any, Callable, Dict, Optional
from importlib import import_module

from pydantic import BaseModel


EXTRAS_MODELS: Dict[str, Dict[str, type[BaseModel]]] = {}


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
    if experiment in EXTRAS_MODELS:
        return
    _import = import_mod or import_module
    module_path = f"ml_playground.experiments.{experiment}.extras"
    try:
        _import(module_path)
    except (ImportError, AttributeError, RuntimeError):
        # Missing extras module means no explicit registration yet.
        return


__all__ = [
    "EXTRAS_MODELS",
    "register_extras_model",
    "get_extras_model",
    "load_extras_models",
]
