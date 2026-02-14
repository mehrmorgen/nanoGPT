from __future__ import annotations
from typing import Any, Callable, Dict, Type, cast
from importlib import import_module
from importlib import resources

from ml_playground.framework.experiment_registry.protocol import (
    Preparer as _PreparerProto,
)

# Registry for experiment-level dataset preparers (mirrors previous datasets.PREPARERS)
PREPARERS: Dict[str, Callable[[], None]] = {}


def load_preparers(
    *,
    resources_mod: Any | None = None,
    import_mod: Callable[[str], Any] | None = None,
) -> None:
    """Plugin loader: import experiment preparers to populate PREPARERS.

    Strict mode: only class-based API is supported. An experiment must expose
    a preparer.py with a class that has a .prepare method. A zero-arg callable
    is registered that instantiates the class and calls .prepare(PreparerConfig()).
    """
    if PREPARERS:
        # Already populated (or tests monkeypatched it)
        return

    pkg = "ml_playground.experiments"
    _resources = resources_mod if resources_mod is not None else resources
    _import = import_mod if import_mod is not None else import_module
    try:
        root = _resources.files(pkg)
    except (ImportError, FileNotFoundError, OSError, RuntimeError):
        return

    for entry in root.iterdir():
        try:
            if not entry.is_dir():
                continue
            exp_name = entry.name

            prep_file = entry / "preparer.py"
            if not prep_file.is_file():
                continue
            try:
                mod = _import(f"{pkg}.{exp_name}.preparer")
            except Exception as e:
                raise SystemExit(f"Failed to load experiment '{exp_name}': {e}") from e

            cls = None
            for attr_name in dir(mod):
                attr_candidate: object = cast(object, getattr(mod, attr_name))
                if isinstance(attr_candidate, type) and hasattr(
                    attr_candidate, "prepare"
                ):
                    attr_ty = cast(Type[_PreparerProto], attr_candidate)
                    cls = attr_ty
                    break
            if cls is None:
                continue

            from ml_playground.framework.configuration.models import (
                PreparerConfig,
            )  # local import

            def _make_fn(_cls: Type[_PreparerProto] | None = cls) -> None:  # type: ignore[no-redef]
                if _cls is None:
                    return
                inst = _cls()  # type: ignore[call-arg]
                try:
                    inst.prepare(PreparerConfig())  # type: ignore[attr-defined]
                except TypeError:
                    inst.prepare()  # type: ignore[call-arg, attr-defined]

            PREPARERS.setdefault(exp_name, _make_fn)
        except (OSError, RuntimeError):
            continue
